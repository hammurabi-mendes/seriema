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
#include "utils/TimeHolder.hpp"

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
using seriema::number_threads_process;
using seriema::local_number_processes;
using seriema::local_process_rank;
using seriema::thread_rank;
using seriema::thread_id;

using seriema::context;
using seriema::queue_pairs;

using seriema::incoming_message_queues;

using seriema::RDMAMessengerGlobal;
using seriema::RDMAAggregatorGlobal;

using seriema::Configuration;

constexpr uint64_t number_operations = 65536000;

static TimeHolder timer{CLOCK_REALTIME};
static atomic<bool> finished = false;

void worker_thread(int offset) {
    seriema::init_thread(offset);

    FastQueuePC<ReceivedMessageInformation> &my_incoming_message_queue = incoming_message_queues[thread_rank];

    struct Payload {
        char data[128];
    } payload;

    RDMAMessengerGlobal messenger;
    RDMAMessenger<> *peer = messenger.get_messenger(thread_id == 0 ? 1 : 0);

    RDMAAggregatorGlobal aggregator{&messenger};

    if(thread_id == 0) {
        bool result;

        do {
            result = aggregator.call(1, [iteration = UINT64_MAX] {
                timer.start();
            });
        } while(!result);

        for(uint64_t iteration = 0; iteration < number_operations - 1; iteration++) {
            result = aggregator.call(1, [iteration]() {
            });
            
            if(!result) {
                iteration--;
            }
        }

        do {
            result = aggregator.call(1, [iteration = number_operations - 1] {
                finished = true;
            });
        } while(!result);
    }
    else {
        uint64_t data;
        auto function = [data]() {
        };

        uint64_t function_size = sizeof(function);

        cout << "Function size: " << function_size << endl;

        while(!finished) {
            peer->process_calls();
        }

        long nanosecond_difference = timer.tick();

        double message_rate = ((double) (number_operations * 1000000000ULL)) / nanosecond_difference;
        double bandwidth = ((double) (number_operations * function_size)) / (1024 * 1024) / (((double) nanosecond_difference) / 1000000000ULL);

        seriema::print_mutex.lock();
        printf("Rate: %.2f messages/s\nBandwidth: %.2f MB/s\n", message_rate, bandwidth);
        seriema::print_mutex.unlock();
    }

    Synchronizer flush_synchronizer{(uint64_t) number_threads};

    aggregator.shutdown_all(&flush_synchronizer);

    while(flush_synchronizer.get_number_operations_left() > 0) {
        messenger.process_calls_all();
        aggregator.flush_all();
        cout << "waiting for incoming shutdown (1)" << endl;
    }

    while(!messenger.get_incoming_shutdown_all()) {
        messenger.process_calls_all();
        cout << "waiting for incoming shutdown (2)" << endl;
    }

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

    const unsigned int share_hardware_concurrency = std::thread::hardware_concurrency() / local_number_processes;

    TimeHolder timer_real{CLOCK_REALTIME};

    for(int i = 0; i < number_threads_process; i++) {
        thread_list.emplace_back(thread(worker_thread, i));

        if(configuration.number_service_threads == 1) {
            seriema::affinity_handler.set_thread_affinity(&thread_list[i], (local_process_rank * share_hardware_concurrency) + i);
        }
        else if(configuration.number_service_threads > 1 && configuration.number_threads_process <= std::thread::hardware_concurrency() / 2) {
            seriema::affinity_handler.set_thread_affinity(&thread_list[i], (local_process_rank * share_hardware_concurrency) + (2 * i));
        }
    }

    for(int i = 0; i < number_threads_process; i++) {
        thread_list[i].join();
    }

    seriema::finalize_thread_handler();

    return 0;
}
