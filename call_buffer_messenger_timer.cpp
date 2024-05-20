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

using seriema::Configuration;

constexpr uint64_t number_operations = 6553600;

atomic<uint64_t> aggregate_nanosecond_difference[128];

void tester_thread(int offset) {
    init_thread(offset);

    RDMAMessengerGlobal messenger;
    RDMAMemory *registered_data = new RDMAMemory(seriema::thread_information->context, 4096);

    // Fill registered_data
    int filled_size = user_produce(registered_data->get_buffer());

    for(uint64_t i = 0; i < number_operations; i++) {
        int destination_thread = i % seriema::number_threads;

        bool result = messenger.call_buffer<RetryAsync>(destination_thread,
            [source_thread = i](void *buffer, uint64_t size) {
                // do something with "source_thread", "buffer", "size"
                // "buffer" has the "size" bytes filled in the source thread
            },
            source, 0, filled_size, synchronizer);

        if(i % 128 == 0) { messenger.process_calls_all(); }
    }

    RDMASynchronizer remote_synchronizer;

    seriema::call<RemoteAsync>(0, [source_thread = i] {
        // "source_thread" notified it is done
        number_clients_done--;
    }, remote_synchronizer);

    remote_synchronizer.spin_nonzero_operations_left();

    messenger.shutdown_all();

    seriema::finalize_thread();
}

int main(int argc, char **argv) {
    vector<thread> thread_list;

    if(argc != 2) {
        cerr << "Run with format " << argv[0] << " <number_threads_process>" << endl;
        exit(EXIT_FAILURE);
    }

    number_threads_process = atoi(argv[1]);

    Configuration configuration{number_threads_process};

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
