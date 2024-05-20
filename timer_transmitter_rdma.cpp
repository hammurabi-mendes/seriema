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

#include <thread>
#include <mutex>

#include <array>

#include "networking.h"
#include "ibutils.hpp"

#include "utils/TimeHolder.hpp"

using std::thread;
using std::recursive_mutex;

using std::array;

using std::cin;
using std::cout;
using std::cerr;
using std::endl;

constexpr uint64_t number_operations = 6553600;

IBContext context(0, 2);

bool server = false;

#define SYNC_HOST "0.0.0.0"
#define SYNC_PORT 50135

void tester_thread() {
    array<RDMAMemory *, 128> incoming_memory;
    array<RDMAMemory *, 128> outgoing_memory;

    for(int i = 0; i < incoming_memory.size(); i++) {
        incoming_memory[i] = new RDMAMemory(&context, 4096);
        outgoing_memory[i] = new RDMAMemory(&context, 4096);
    }

    IBQueuePair queue_pair(&context);
    IBTransmitter transmitter(&context, &queue_pair, 0);

    // Synchronize
    struct QPInfo {
        uint32_t number;
        uint16_t lid;
        RDMAMemoryLocator locator;
    };

    QPInfo qpinfo_outgoing;
    QPInfo qpinfo_incoming;

    qpinfo_outgoing.number = queue_pair.queue_pair->qp_num;
    qpinfo_outgoing.lid = context.port_information.lid;
    qpinfo_outgoing.locator = RDMAMemoryLocator(incoming_memory[0]);

    cout << "(Me) QP number = " << qpinfo_outgoing.number << " lid = " << qpinfo_outgoing.lid << endl;

    uint64_t *incoming_data = reinterpret_cast<uint64_t *>(incoming_memory[0]->get_buffer());
    uint64_t *outgoing_data = reinterpret_cast<uint64_t *>(outgoing_memory[0]->get_buffer());
    RDMAMemoryLocator *remote_memory = &qpinfo_incoming.locator;

    *incoming_data = 0;

    if(server) {
        int server_socket = create_server(SYNC_PORT);

        int client_socket = accept_client(server_socket);

        send(client_socket, &qpinfo_outgoing, sizeof(struct QPInfo), 0);
        recv(client_socket, &qpinfo_incoming, sizeof(struct QPInfo), 0);
    }
    else {
        int server_socket = connect_server(SYNC_HOST, SYNC_PORT);

        recv(server_socket, &qpinfo_incoming, sizeof(struct QPInfo), 0);
        send(server_socket, &qpinfo_outgoing, sizeof(struct QPInfo), 0);
    }

    cout << "(Other) QP number = " << qpinfo_incoming.number << " lid = " << qpinfo_incoming.lid << endl;

    queue_pair.setup(&context, qpinfo_incoming.number, qpinfo_incoming.lid);

    Synchronizer synchronizer{1};
    sprintf((char *) outgoing_memory[0]->get_buffer(), "Hello %s!\n", server ? "client" : "server");

    transmitter.rdma_write(outgoing_memory[0], 0, 4096, remote_memory, IBV_SEND_SIGNALED, nullptr, &synchronizer);

    while(synchronizer.get_number_operations_left() > 0) {
        context.completion_queues->flush_send_completion_queue();
    }

    while(*incoming_data == '\0') {
    }

    cout << (char *) incoming_data << endl;

    *incoming_data = 0;

    if(server) {
        while(*incoming_data != 0xbeef) {
        }

        // Last RDMA has flag
        outgoing_data = reinterpret_cast<uint64_t *>(outgoing_memory[0]->get_buffer());
        *outgoing_data = 0xbeef;

        synchronizer.reset(1);

        transmitter.rdma_write(outgoing_memory[0], 0, 4096, remote_memory, IBV_SEND_SIGNALED, nullptr, &synchronizer);

        while(synchronizer.get_number_operations_left() > 0) {
            context.completion_queues->flush_send_completion_queue();
        }

        sleep(3);
    }
    else {
        TimeHolder timer;

        for(uint64_t iteration = 0; iteration < number_operations - 1; iteration++) {
            outgoing_data = reinterpret_cast<uint64_t *>(outgoing_memory[iteration % outgoing_memory.size()]->get_buffer());
            *outgoing_data = 0;

            transmitter.rdma_write(outgoing_memory[iteration % outgoing_memory.size()], 0, 4096, remote_memory);
        }

        // Last RDMA has flag
        outgoing_data = reinterpret_cast<uint64_t *>(outgoing_memory[(number_operations - 1) % outgoing_memory.size()]->get_buffer());
        *outgoing_data = 0xbeef;

        synchronizer.reset(1);

        transmitter.rdma_write(outgoing_memory[(number_operations - 1) % outgoing_memory.size()], 0, 4096, remote_memory, IBV_SEND_SIGNALED, nullptr, &synchronizer);

        while(synchronizer.get_number_operations_left() > 0) {
            context.completion_queues->flush_send_completion_queue();
        }

        while(*incoming_data != 0xbeef) {
        }

        long nanosecond_difference = timer.tick();

        double message_rate = ((double) (number_operations * 1000000000ULL)) / nanosecond_difference;
        double bandwidth = ((double) (number_operations * 4096)) / (1024 * 1024) / (((double) nanosecond_difference) / 1000000000ULL);

        printf("Rate: %.2f messages/s\nBandwidth: %.2f MB/s\n", message_rate, bandwidth);
    }

    cout << "done" << endl;
}

int main(int argc, char **argv) {
    if(argc > 1) {
        cout << "Starting as server" << endl;

        server = true;
    }

    vector<thread> thread_list;

    int number_threads_process = 1;

    for(int i = 0; i < number_threads_process; i++) {
        thread_list.push_back(thread(tester_thread));
    }

    for(int i = 0; i < number_threads_process; i++) {
        thread_list[i].join();
    }

    return 0;
}