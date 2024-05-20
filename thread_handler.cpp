/*
Copyright (c) 2024, Hammurabi Mendes.
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:
    * Redistributions of source code must retain the above copyright
      notice, this list of conditions and the following disclaimer.
    * Redistributions in binary form must reproduce the above copyright
      notice, this list of conditions and the following disclaimer in the
      documentation and/or other materials provided with the distribution.
    * Neither the name of the copyright holder nor the names of its contributors
      may be used to endorse or promote products derived from this software without
      specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDERS BE LIABLE FOR ANY
DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

#include "thread_handler.h"

#include "ibutils.hpp"
#include "remote_calls.hpp"

#include "utils/Queues.hpp"

#ifdef USE_MPI
#include "MPI/MPIHelper.hpp"
#endif /* USE_MPI */

#include <mutex>
#include <atomic>
#include <thread>

#include <vector>
#include <array>

#include <iostream>
#include <stdlib.h>

using std::recursive_mutex;
using std::condition_variable;
using std::atomic;

using std::vector;
using std::array;

using std::cout;
using std::endl;
using std::cerr;

namespace seriema {

constexpr int COMPLETION_QUERY_BUFFER_SIZE = 65536;

/*
 * Rank-related globals
*/
int number_processes;
int process_rank;

int number_threads;
int number_threads_process;

int local_process_rank;
int local_number_processes;

MPI_Comm local_communicator;

thread_local int thread_rank = -1;
thread_local int thread_id = -1;

thread_local vector<int> local_thread_ids;

/*
* Operation-related globals
*/

vector<thread *> service_threads;
atomic<bool> stop_service_threads;

recursive_mutex print_mutex;

#ifdef USE_MPI
MPI_Request global_request;
#endif /* USE_MPI */

IBContext *context;

IBQueuePair **queue_pairs;
IBTransmitter<QP_GLOBAL_FLUSH_INTERVAL, QP_THREAD_FLUSH_INTERVAL> **transmitters;

int number_queue_pairs;

FastQueuePC<ReceivedMessageInformation> *incoming_message_queues;
FastQueuePC<RDMAMemory *> *incoming_memory_queues;

Configuration global_configuration;

RDMAAllocator<GLOBAL_ALLOCATOR_CHUNK_SIZE, GLOBAL_ALLOCATOR_SUPERCHUNK_SIZE> **global_allocators;

thread_local ThreadContext *thread_context;

AffinityHandler affinity_handler;

inline void flush_queue_pairs() {
    uint32_t call_immediate = FLAG_DUMMY;
    
    char dummy_buffer[128];
    RDMAMemory memory{&dummy_buffer, 128};

    Synchronizer flush_synchronizer{(uint64_t) number_queue_pairs};

    for(int i = 0; i < number_queue_pairs; i++) {
        transmitters[i]->send(&memory, 0, 0, IBV_SEND_SOLICITED, &call_immediate, &flush_synchronizer);
    }

    while(flush_synchronizer.get_number_operations_left() > 0) {
        seriema::flush_send_completion_queues();
    }
}

void flush_send_completion_queues() {
    static const bool use_multiple_completion_queues = global_configuration.multiple_completion_queues;

    if(use_multiple_completion_queues) {
        for(int i = 0; i < number_queue_pairs; i++) {
            queue_pairs[i]->completion_queues->flush_send_completion_queue();
        }
    }
    else {
        context->completion_queues->flush_send_completion_queue();
    }
}

void receiver(int rank) {
    init_thread(rank);

    for(int i = 0; i < THREAD_INCOMING_MEMORY_BUFFERS; i++) {
        context->post_receive(thread_context->incoming_allocator->allocate_incoming());
    }

    // Main loop

    const uint64_t number_service_threads = global_configuration.number_service_threads;
    const uint64_t multiplier_queue_pairs = global_configuration.multiplier_queue_pairs;

    const bool use_multiple_completion_queues = global_configuration.multiple_completion_queues;
    const bool use_lease_system = use_multiple_completion_queues && multiplier_queue_pairs != 1;
    const bool use_recv_event_system = global_configuration.create_completion_channel_shared;

    const int service_microsleep = global_configuration.service_microsleep;

    uint64_t queue_pair_index = rank;
    IBCompletionQueues *completion_queues = context->completion_queues;

    ReceivedMessageInformation *received_message_information;

    if(!use_lease_system) {
        received_message_information = new ReceivedMessageInformation[THREAD_INCOMING_MEMORY_BUFFERS];
    }

    uint64_t number_queried = 0;
    uint64_t number_received = 0;

    uint64_t iteration = 0;

    uint64_t outstanding_received = 0;

    if(use_recv_event_system) {
        if(use_multiple_completion_queues) {
            for(int i = 0; i < number_queue_pairs; i++) {
                queue_pairs[i]->completion_queues->set_recv_event_notification();
            }
        }
        else {
            completion_queues->set_recv_event_notification();
        }
    }

    while(true) {
        iteration++;

        if((iteration % 4096 == 0 || use_recv_event_system) && stop_service_threads.load(std::memory_order_relaxed)) {
            break;
        }

        if(use_multiple_completion_queues) {
            if(number_service_threads == 1) {
                queue_pair_index = (iteration % number_queue_pairs);
            }
            else if(number_service_threads == multiplier_queue_pairs) {
                queue_pair_index = ((iteration % number_processes) * multiplier_queue_pairs) + rank;
            }

            completion_queues = queue_pairs[queue_pair_index]->completion_queues;
        }

        completion_queues->flush_send_completion_queue();

        if(use_recv_event_system) {
            completion_queues = completion_queues->wait_recv_event();
        }

        if(!use_lease_system) {
            number_queried = std::max(1UL, THREAD_INCOMING_MEMORY_BUFFERS - outstanding_received);
        }
        else {
            received_message_information = incoming_message_queues[queue_pair_index % multiplier_queue_pairs].lease(number_queried, std::max(1UL, THREAD_INCOMING_MEMORY_BUFFERS - outstanding_received));
        }

        number_received = completion_queues->poll_recv_completion_queue(received_message_information, number_queried);

        outstanding_received += number_received;

        while(outstanding_received && thread_context->incoming_allocator->has_free_incoming()) {
            context->post_receive(thread_context->incoming_allocator->allocate_incoming());
            outstanding_received--;
        }

        if(number_received > 0 && outstanding_received == THREAD_INCOMING_MEMORY_BUFFERS) {
            context->post_receive(thread_context->incoming_allocator->allocate_incoming());
            outstanding_received--;
        }

        if(!use_lease_system) {
            for(uint64_t counter = 0; counter < number_received; counter++) {
                if(received_message_information[counter].has_immediate()) {
                    uint64_t immediate = received_message_information[counter].get_immediate();

                    if(immediate & FLAG_SERVICE) {
                        process_call(received_message_information[counter].get_buffer());

                        received_message_information[counter].get_memory()->ready = true;
                    }
                    else {
                        uint64_t destination_thread_id = immediate & 0xffffff;

                        incoming_message_queues[get_thread_rank(destination_thread_id)].push(received_message_information[counter]);
                    }
                }
#ifdef DEBUG
                else {
                    assert(false); // Helps finding missing messages on debugging
                }
#endif /* DEBUG */
            }
        }
        else {
            for(uint64_t counter = 0; counter < number_received; counter++) {
                if(received_message_information[counter].has_immediate()) {
                    uint64_t immediate = received_message_information[counter].get_immediate();

                    if(immediate & FLAG_SERVICE) {
                        process_call(received_message_information[counter].get_buffer());

                        received_message_information[counter].get_memory()->ready = true;
                    }
                }
#ifdef DEBUG
                else {
                    assert(false); // Helps finding missing messages on debugging
                }
#endif /* DEBUG */
            }

            if(number_received > 0) {
                incoming_message_queues[queue_pair_index % multiplier_queue_pairs].push_lease(number_received);
            }
        }

        if(service_microsleep > 0) {
            usleep(service_microsleep);
        }
    }

    if(!use_lease_system) {
        delete[] received_message_information;
    }

    finalize_thread(false);
}

void setup_rdma(int port) {
    context = new IBContext(0, port, global_configuration.multiple_completion_queues, global_configuration.create_completion_channel_shared);

    number_queue_pairs = global_configuration.multiplier_queue_pairs * number_processes;

    queue_pairs = new IBQueuePair *[number_queue_pairs];
    transmitters = new IBTransmitter<QP_GLOBAL_FLUSH_INTERVAL, QP_THREAD_FLUSH_INTERVAL> *[number_queue_pairs];

    for(uint64_t i = 0; i < number_queue_pairs; i++) {
        queue_pairs[i] = new IBQueuePair{context, global_configuration.multiple_completion_queues, global_configuration.create_completion_channel_private};
    }

    for(uint64_t i = 0; i < number_queue_pairs; i++) {
        transmitters[i] = new IBTransmitter<QP_GLOBAL_FLUSH_INTERVAL, QP_THREAD_FLUSH_INTERVAL>{context, queue_pairs[i], i};
    }

    struct QPInfo {
        uint32_t number;
        uint16_t lid;
    };

    QPInfo qpinfo_outgoing[number_queue_pairs];
    QPInfo qpinfo_incoming[number_queue_pairs];

    for(int i = 0; i < number_queue_pairs; i++) {
        qpinfo_outgoing[i].number = queue_pairs[i]->queue_pair->qp_num;
        qpinfo_outgoing[i].lid = context->port_information.lid;
    }

#ifdef USE_MPI
    MPIHelper::allToAll(qpinfo_outgoing, qpinfo_incoming, MPI_BYTE, global_configuration.multiplier_queue_pairs * sizeof(QPInfo));
#else
    memcpy(qpinfo_incoming, qpinfo_outgoing, global_configuration.multiplier_queue_pairs * sizeof(QPInfo));
#endif /* USE_MPI */

#ifdef DEBUG
    for(int process = 0; process < number_processes; process++) {
        if(process_rank == process) {
            cout << "# threads per process: " << number_threads_process << endl;
            cout << "# threads: " << number_threads << endl;
            cout << "# processes: " << number_processes << endl;

            for(int i = 0; i < number_queue_pairs; i++) {
                cout << "Connecting (" << queue_pairs[i]->queue_pair->qp_num << "," << context->port_information.lid << ") <---> (" << qpinfo_incoming[i].number << "," << qpinfo_incoming[i].lid << ")" << endl;
            }

            fflush(stdout);
        }

#ifdef USE_MPI
        MPIHelper::barrier();
#endif /* USE_MPI */
    }
#endif /* DEBUG */

    for(int i = 0; i < number_queue_pairs; i++) {
        queue_pairs[i]->setup(context, qpinfo_incoming[i].number, qpinfo_incoming[i].lid);
    }

#ifdef USE_MPI
    MPIHelper::barrier();
#endif /* USE_MPI */
}

void init_thread(int offset, bool create_allocators) {
    thread_rank = offset;
    thread_id = thread_rank + (process_rank * number_threads_process);

    RDMAAllocator<GLOBAL_ALLOCATOR_CHUNK_SIZE, GLOBAL_ALLOCATOR_SUPERCHUNK_SIZE> *global_allocator = global_allocators[affinity_handler.get_numa_zone(thread_rank)];

    thread_context = new ThreadContext(create_allocators, global_allocator);

#if defined(LIBNUMA) && defined(__linux__)
    affinity_handler.set_numa_location(thread_rank);
#endif /* LIBNUMA and __linux__ */
}

int init_thread_handler(int &argc, char **&argv, Configuration &configuration) {
    global_configuration = configuration;

    int thread_support_provided = -1;
    int status;

#ifdef USE_MPI
    status = MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &thread_support_provided);

    if(status != MPI_SUCCESS) {
        fprintf(stderr, "Fatal: MPI implementation does not support multiple threads\n");
        MPI_Abort(MPI_COMM_WORLD, 1);

        exit(EXIT_FAILURE);
    }

    if(thread_support_provided != MPI_THREAD_MULTIPLE) {
        fprintf(stderr, "Fatal: MPI implementation does not support multiple threads\n");
        MPI_Abort(MPI_COMM_WORLD, 1);

        exit(EXIT_FAILURE);
    }

    status = MPI_Comm_size(MPI_COMM_WORLD, &number_processes);

    if(status != MPI_SUCCESS) {
        fprintf(stderr, "Fatal: Unable to obtain global communicator size\n");
        MPI_Abort(MPI_COMM_WORLD, 1);

        exit(EXIT_FAILURE);
    }

    status = MPI_Comm_rank(MPI_COMM_WORLD, &process_rank);

    if(status != MPI_SUCCESS) {
        fprintf(stderr, "Fatal: Unable to obtain global communicator rank\n");
        MPI_Abort(MPI_COMM_WORLD, 1);

        exit(EXIT_FAILURE);
    }

    status = MPI_Comm_split_type(MPI_COMM_WORLD, MPI_COMM_TYPE_SHARED, 0, MPI_INFO_NULL, &local_communicator);

    if(status != MPI_SUCCESS) {
        fprintf(stderr, "Fatal: Unable to split global communicator into local communicator\n");
        MPI_Abort(MPI_COMM_WORLD, 1);

        exit(EXIT_FAILURE);
    }

    status = MPI_Comm_rank(local_communicator, &local_process_rank);

    if(status != MPI_SUCCESS) {
        fprintf(stderr, "Fatal: Unable to obtain global communicator rank\n");
        MPI_Abort(MPI_COMM_WORLD, 1);

        exit(EXIT_FAILURE);
    }

    status = MPI_Comm_size(local_communicator, &local_number_processes);

    if(status != MPI_SUCCESS) {
        fprintf(stderr, "Fatal: Unable to obtain local communicator size\n");
        MPI_Abort(MPI_COMM_WORLD, 1);

        exit(EXIT_FAILURE);
    }
#else
    process_rank = 0;
    number_processes = 1;
#endif /* USE_MPI */

    number_threads_process = global_configuration.number_threads_process;
    number_threads = number_threads_process * number_processes;

    if(global_configuration.device == -1) {
        global_configuration.device = (local_process_rank % IBContext::get_device_quantity());
    }

    if(global_configuration.device_port == -1) {
        global_configuration.device_port = (local_process_rank % IBContext::get_port_quantity(global_configuration.device)) + 1;
    }

    cout << "Local rank " << local_process_rank << " running on device/port " << global_configuration.device << "/" << global_configuration.device_port << endl;

    global_configuration.check_configuration();
    setup_rdma(global_configuration.device_port);

    global_allocators = new RDMAAllocator<GLOBAL_ALLOCATOR_CHUNK_SIZE, GLOBAL_ALLOCATOR_SUPERCHUNK_SIZE>*[affinity_handler.get_number_zones()];

    for(int i = 0; i < affinity_handler.get_number_zones(); i++) {
        global_allocators[i] = new RDMAAllocator<GLOBAL_ALLOCATOR_CHUNK_SIZE, GLOBAL_ALLOCATOR_SUPERCHUNK_SIZE>(context);
    }

    incoming_message_queues = new FastQueuePC<ReceivedMessageInformation>[number_threads_process];
    incoming_memory_queues = new FastQueuePC<RDMAMemory *>[number_threads_process];

    const unsigned int share_hardware_concurrency = std::thread::hardware_concurrency() / local_number_processes;

    stop_service_threads = false;

    for(uint64_t i = 0; i < global_configuration.number_service_threads; i++) {
        service_threads.push_back(new thread(receiver, i));
    }

#ifdef USE_MPI
    MPIHelper::barrier();
#endif /* USE_MPI */

    return 0;
}

void finalize_thread(bool perform_flush_queue_pairs) {
    if(perform_flush_queue_pairs) {
        flush_queue_pairs();
    }

    delete thread_context;
}

int finalize_thread_handler() {
#ifdef USE_MPI
    MPIHelper::barrier();
#endif /* USE_MPI */

    stop_service_threads = true;
    flush_queue_pairs();

    for(int i = 0; i < global_configuration.number_service_threads; i++) {
        service_threads[i]->join();
    }

    for(int i = 0; i < global_configuration.number_service_threads; i++) {
        delete service_threads[i];
    }

    delete[] incoming_message_queues;

    while(!incoming_memory_queues->empty()) {
        incoming_memory_queues->pop_remove();
    }

    delete[] incoming_memory_queues;

    for(int i = 0; i < number_queue_pairs; i++) {
        delete transmitters[i];
    }
    delete[] transmitters;

    for(int i = 0; i < number_queue_pairs; i++) {
        delete queue_pairs[i];
    }
    delete[] queue_pairs;

    for(int i = 0; i < affinity_handler.get_number_zones(); i++) {
        delete global_allocators[i];
    }

    delete[] global_allocators;

    delete context;

#ifdef USE_MPI
    MPI_Finalize();
#endif /* USE_MPI */

    return 0;
}

}; // namespace seriema