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

#ifndef THREAD_HANDLER_H
#define THREAD_HANDLER_H

#ifdef USE_MPI
#include <mpi.h>
#endif /* USE_MPI */

#include <thread>
#include <mutex>
#include <condition_variable>
#include <atomic>

#include <vector>
#include <array>
#include <queue>
#include <unordered_map>
#include <iostream>

#include <unistd.h>
#include <limits>

#ifdef DEBUG
#include <cassert>
#endif /* DEBUG */

#include "ibutils.hpp"

#include "utils/Queues.hpp"
#include "utils/Barrier.hpp"

#include "utils/TimeHolder.hpp"
#include "utils/Random.hpp"

#include "utils/AffinityHandler.hpp"

using std::thread;
using std::recursive_mutex;
using std::condition_variable;
using std::atomic;

using std::vector;
using std::array;
using std::queue;
using std::unordered_map;

using std::cout;
using std::cerr;
using std::endl;
using std::ios;

namespace dsys {

/*
 * Definitions
 */
constexpr uint64_t QP_GLOBAL_FLUSH_INTERVAL = 15360;
constexpr uint64_t QP_THREAD_FLUSH_INTERVAL = 1024;

constexpr uint64_t THREAD_INCOMING_MEMORY_BUFFERS = 128;
constexpr uint64_t THREAD_OUTGOING_MEMORY_BUFFERS = 128;

// Best size for rate: 512B
// Best size for bandwidth: 16K
constexpr uint64_t RDMA_MEMORY_SIZE = 16384;

constexpr uint64_t GLOBAL_ALLOCATOR_CHUNK_SIZE = 4096 * 512;
constexpr uint64_t GLOBAL_ALLOCATOR_SUPERCHUNK_SIZE = 4096 * 512 * 512;

#define FLAG_DUMMY (1 << 24)
#define FLAG_SERVICE (1 << 25)
#define FLAG_SINGLE (1 << 26)
#define FLAG_MULTIPLE (1 << 27)

/*
 * Rank-related globals
 */
extern int number_processes;
extern int process_rank;

extern int number_threads;
extern int number_threads_process;

extern int local_process_rank;
extern int local_number_processes;

extern MPI_Comm local_communicator;

extern thread_local int thread_rank;
extern thread_local int thread_id;

extern thread_local vector<int> local_thread_ids;

/*
 * Operation-related globals
 */

extern vector<thread *> service_threads;

extern recursive_mutex print_mutex;

#ifdef USE_MPI
extern MPI_Request global_request;
#endif /* USE_MPI */

/*
 * Configuration-related data
 */

// Best with number_service_threads == multiplier_queue_pairs, multiple_completion_queues = false
struct Configuration {
    int number_threads_process;

    bool single_thread_queue_pairs;
    int multiplier_queue_pairs = 1;

    bool multiple_completion_queues;
    int number_service_threads = 1;

    int device = -1;
    int device_port = -1;

    int service_microsleep = 0;

    bool create_completion_channel_shared = false;
    bool create_completion_channel_private = false;

    Configuration(int number_threads_process = 1, bool single_thread_queue_pairs = false, bool multiple_completion_queues = false): number_threads_process{number_threads_process}, single_thread_queue_pairs{single_thread_queue_pairs}, multiple_completion_queues{multiple_completion_queues} {
        if(single_thread_queue_pairs) {
            multiplier_queue_pairs = number_threads_process;
        }
    }

    void check_configuration() {
        uint64_t number_queue_pairs = multiplier_queue_pairs * number_processes;

        if(number_service_threads != 1 && number_service_threads != multiplier_queue_pairs) {
            cerr << "Invalid configuration: number_service_threads different than 1 and different than multiplier_queue_pairs" << endl;
            exit(EXIT_FAILURE);
        }

        if(number_service_threads != 1 && !multiple_completion_queues) {
            cerr << "Invalid configuration: number_service_threads is not 1 and multiple_completion_queues is False" << endl;
            exit(EXIT_FAILURE);
        } 
    }
};

extern Configuration global_configuration;

extern IBContext *context;

extern IBQueuePair **queue_pairs;
extern IBTransmitter<QP_GLOBAL_FLUSH_INTERVAL, QP_THREAD_FLUSH_INTERVAL> **transmitters;
extern int number_queue_pairs;

/*
 * Process-mapping and transmitter-mapping functions
 */

inline uint64_t get_transmitter_index(int destination_thread_id) noexcept {
#ifdef DEBUG
    assert(destination_thread_id < dsys::number_threads);
#endif /* DEBUG */
    uint64_t destination_process_rank = destination_thread_id / number_threads_process;
    uint64_t destination_thread_rank = destination_thread_id % number_threads_process;

    return (destination_process_rank * global_configuration.multiplier_queue_pairs) + (destination_thread_rank % global_configuration.multiplier_queue_pairs);
}

inline IBTransmitter<QP_GLOBAL_FLUSH_INTERVAL, QP_THREAD_FLUSH_INTERVAL> *get_transmitter(int destination_thread_id) noexcept {
#ifdef DEBUG
    assert(destination_thread_id < dsys::number_threads);
#endif /* DEBUG */
    return transmitters[get_transmitter_index(destination_thread_id)];
}

inline int get_process_rank(int thread_id) noexcept { 
    return thread_id / number_threads_process;
}

inline int get_thread_rank(int thread_id) noexcept { 
    return thread_id % number_threads_process;
}

inline int get_random_thread_id(int process_rank) noexcept { 
    return (process_rank * number_threads_process) + Random<int>::getRandom(0, number_threads_process - 1);
}
}; // namespace dsys

#include "memory_allocation.hpp"

namespace dsys {
extern FastQueuePC<ReceivedMessageInformation> *incoming_message_queues;
extern FastQueuePC<RDMAMemory *> *incoming_memory_queues;

extern RDMAAllocator<GLOBAL_ALLOCATOR_CHUNK_SIZE, GLOBAL_ALLOCATOR_SUPERCHUNK_SIZE> **global_allocators;

struct ThreadContext {
    MemoryAllocator *incoming_allocator = nullptr;
    MemoryAllocator *outgoing_allocator = nullptr;

    LinearMemoryAllocator *linear_allocator = nullptr;
    LinearAllocator<MemoryAllocator> *fine_outgoing_allocator = nullptr;

    // Allocated and deleted outside this class
    RDMAAllocator<> *global_allocator;

    unordered_map<int, queue<RDMAMemory *>> incoming_memory_map;

#ifndef WITHOUT_INLINE_CALLS
    char inline_buffer[MAX_INLINE_DATA];
    RDMAMemory inline_memory{inline_buffer, MAX_INLINE_DATA};
#endif /* WITHOUT_INLINE_CALLS */

    ThreadContext(bool create_allocators, RDMAAllocator<> *global_allocator): incoming_allocator{incoming_allocator}, outgoing_allocator{outgoing_allocator}, linear_allocator{linear_allocator}, global_allocator{global_allocator} {
        if(create_allocators) {
            incoming_allocator = new MemoryAllocator(global_allocator, THREAD_INCOMING_MEMORY_BUFFERS, RDMA_MEMORY_SIZE);
            outgoing_allocator = new MemoryAllocator(global_allocator, THREAD_OUTGOING_MEMORY_BUFFERS, RDMA_MEMORY_SIZE);

            linear_allocator = new LinearMemoryAllocator(global_allocator);
            fine_outgoing_allocator = new LinearAllocator(new MemoryAllocator(global_allocator, THREAD_OUTGOING_MEMORY_BUFFERS, RDMA_MEMORY_SIZE));
        }
    }

    virtual ~ThreadContext() {
        if(fine_outgoing_allocator) {
            delete fine_outgoing_allocator;
        }

        if(linear_allocator) {
            delete linear_allocator;
        }

        if(incoming_allocator) {
            delete incoming_allocator;
        }

        if(outgoing_allocator) {
            delete outgoing_allocator;
        }
    }
};

extern thread_local ThreadContext *thread_context;

extern AffinityHandler affinity_handler;

/*
 * Functions
 */

void init_thread(int offset, bool create_allocators = true);
void finalize_thread(bool perform_flush_queue_pairs = true);

void flush_send_completion_queues();

int init_thread_handler(int &argc, char **&argv, Configuration &configuration);
int finalize_thread_handler();

// TODO: Ugh
inline vector<int> &get_local_thread_ids() {
    if(local_thread_ids.empty()) {
        for(int i = 0; i < number_threads_process; i++) {
            local_thread_ids.push_back((process_rank * number_threads_process) + i);
        }
    }

    return local_thread_ids;
}

}; // namespace dsys

#endif /* THREAD_HANDLER_H */