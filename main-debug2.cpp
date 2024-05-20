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

#include <iostream>
#include <unistd.h>

#include <vector>
#include <array>

#include <thread>
#include <mutex>

#include "seriema.h"

using std::vector;
using std::array;

using std::thread;
using std::recursive_mutex;

using std::cout;
using std::cerr;
using std::endl;

using seriema::GlobalAddress;

using seriema::number_processes;
using seriema::process_rank;
using seriema::number_threads;
using seriema::thread_rank;
using seriema::number_threads_process;
using seriema::thread_id;

using seriema::thread_context;

using seriema::context;
using seriema::queue_pairs;

using seriema::incoming_message_queues;

using seriema::RDMAMessengerGlobal;
using seriema::RDMAAggregatorGlobal;

using seriema::Configuration;

constexpr uint64_t number_operations = 65536*16*100;

atomic<uint64_t> aggregate_nanosecond_difference[128];
thread_local uint64_t counter[128];

recursive_mutex a;

void tester_thread(int offset) {
    seriema::init_thread(offset);

    RDMAMemory *source = new RDMAMemory(context, 4096);

    RDMAMessengerGlobal messenger;

    ChunkMemoryAllocator chunk_allocator{thread_context->global_allocator};

    // RDMAAggregatorGlobal aggregator{&chunk_allocator, &messenger};

    TimeHolder timer;

    uint64_t received = 0;

    for(int i = 0 ; i < 128; i++) counter[i] = 0;
    for(uint64_t iteration = 0; iteration < number_operations / number_threads; iteration++) {
        // if(iteration % 1000 == 0) {
        //     seriema::print_mutex.lock();
        //     cout << "ID = " << thread_id << " iteration = " << iteration << " received = " << received << endl;
        //     seriema::print_mutex.unlock();
        // }

        int destination_thread_id = iteration % number_threads;
        
        bool result = messenger.call_buffer(destination_thread_id, [iteration, s = thread_id](void *buffer, uint64_t size) {
            assert(counter[s] == iteration / number_threads);
            counter[s]++;
                if(iteration % 10000 == 0) {
                    seriema::print_mutex.lock();
                    cout << "receiver working on iteration " << iteration << "(buffer = " << buffer << ", size = " << size << ")" << endl;
                    seriema::print_mutex.unlock();
                }
            }, source, 0, 4096);
        
        if(!result) {
            iteration--;
            messenger.process_calls_all();
        }
        else if(iteration % 1 == 0) {
            messenger.process_calls_all();
            // aggregator.flush_all();
        }
    }

    Synchronizer flush_synchronizer{(uint64_t) number_threads};

    messenger.shutdown_all(&flush_synchronizer);

    while(flush_synchronizer.get_number_operations_left() > 0) {
        messenger.process_calls_all();
        // aggregator.flush_all();
        cout << "waiting for incoming shutdown (1)" << endl;
    }

    while(!messenger.get_incoming_shutdown_all()) {
        messenger.process_calls_all();
        cout << "waiting for incoming shutdown (2)" << endl;
    }

    long nanosecond_difference = timer.tick();

    double message_rate = ((double) (number_operations * 1000000000ULL)) / nanosecond_difference;
    double bandwidth = ((double) (number_operations * 4096)) / (1024 * 1024) / (((double) nanosecond_difference) / 1000000000ULL);

    seriema::print_mutex.lock();
    printf("Rate: %.2f messages/s\nBandwidth: %.2f MB/s\n", message_rate, bandwidth);
    seriema::print_mutex.unlock();

    aggregate_nanosecond_difference[thread_rank] = nanosecond_difference;

    seriema::print_mutex.lock();
    cout << "done" << endl;
    seriema::print_mutex.unlock();

    seriema::finalize_thread();
}

int main(int argc, char **argv) {
    vector<thread> thread_list;

    if(argc != 2) {
        cerr << "Run with format " << argv[0] << " <number_threads_process>" << endl;
        exit(EXIT_FAILURE);
    }

    number_threads_process = atoi(argv[1]);

    Configuration configuration{number_threads_process, true, false};
    configuration.number_service_threads = 1;

    seriema::init_thread_handler(argc, argv, configuration);

    for(int i = 0; i < number_threads_process; i++) {
        thread_list.push_back(thread(tester_thread, i));
    }

    for(int i = 0; i < number_threads_process; i++) {
        thread_list[i].join();
    }

    long nanosecond_difference = 0;

    for(auto i = 0; i < number_threads_process; i++) {
        nanosecond_difference += aggregate_nanosecond_difference[i];
    }

    nanosecond_difference /= number_threads_process;

    double message_rate = ((double) (number_operations * 1000000000ULL)) / nanosecond_difference;
    double bandwidth = ((double) (number_operations * 4096)) / (1024 * 1024) / (((double) nanosecond_difference) / 1000000000ULL);

    printf("AVG Rate: %.2f messages/s\nAVG Bandwidth: %.2f MB/s\n", message_rate, bandwidth);

    seriema::finalize_thread_handler();

    return 0;
}
