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
using seriema::Synchronizer;

using seriema::number_processes;
using seriema::process_rank;
using seriema::number_threads;
using seriema::thread_rank;
using seriema::number_threads_process;
using seriema::thread_id;

using seriema::context;
using seriema::queue_pairs;

using seriema::Configuration;

constexpr uint64_t number_operations = 65536;

void tester_thread(int offset) {
    seriema::init_thread(offset);

    uint64_t received = 0;

    for(uint64_t iteration = 0; iteration < number_operations; iteration++) {
        // if(iteration % 1000 == 0) {
        //     seriema::print_mutex.lock();
        //     cout << "ID = " << thread_id << " iteration = " << iteration << " received = " << received << endl;
        //     seriema::print_mutex.unlock();
        // }

        int destination_thread_id = iteration % number_threads;
        RDMAMemory *outgoing = seriema::outgoing_allocator->allocate();

        //sprintf((char *) outgoing->get_buffer(), "Hello world %d!\n", iteration);

        get_transmitter(destination_thread_id)->send(outgoing, 0, 3000);
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

    Configuration configuration{number_threads_process};
    seriema::init_thread_handler(argc, argv, configuration);

    for(int i = 0; i < number_threads_process; i++) {
        thread_list.push_back(thread(tester_thread, i));
    }

    for(int i = 0; i < number_threads_process; i++) {
        thread_list[i].join();
    }

    seriema::finalize_thread_handler();

    return 0;
}