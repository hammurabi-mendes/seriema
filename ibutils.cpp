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

#include <cstdlib>
#include <iostream>

#include <arpa/inet.h>

#include "ibutils.hpp"

using std::cout;
using std::cerr;
using std::endl;

using std::string;


// On the Infinity tests, they use the maximum inline cap as 0,
// and the queue sizes as 16351. Change these if you want to compare apples to apples.

#define MAX_SEND_QUEUE_SIZE context->device_information.max_qp_wr
#define MAX_SEND_QUEUE_SGE 1
#define MAX_RECV_QUEUE_SIZE context->device_information.max_qp_wr
#define MAX_RECV_QUEUE_SGE 1

#define MAX_COMPLETION_QUEUE_SIZE_SHARED device_information.max_cqe
#define MAX_COMPLETION_QUEUE_SIZE_PRIVATE context->device_information.max_cqe

#define MAX_SRQ_SIZE device_information.max_srq_wr
#define MAX_SRQ_SGE 1

IBCompletionQueues::IBCompletionQueues(ibv_context *context, int size, struct ibv_comp_channel *recv_event_channel, bool recv_event_only_solicited): initialized{false}, recv_event_channel{recv_event_channel}, recv_event_only_solicited{recv_event_only_solicited} {
    send_completion_queue = ibv_create_cq(context, size, NULL, NULL, 0);

    if(send_completion_queue == NULL) {
        cerr << "Error creating send completion queue" << endl;

        exit(EXIT_FAILURE);
    }

    recv_completion_queue = ibv_create_cq(context, size, recv_event_channel ? this : NULL, recv_event_channel ? recv_event_channel : NULL, 0);

    if(recv_completion_queue == NULL) {
        cerr << "Error creating recv completion queue" << endl;

        exit(EXIT_FAILURE);
    }

    initialized = true;
}

IBCompletionQueues::~IBCompletionQueues() {
    if(send_completion_queue != NULL) {
        ibv_destroy_cq(send_completion_queue);
    }

    if(recv_completion_queue != NULL) {
        ibv_destroy_cq(recv_completion_queue);
    }
}

int IBContext::get_device_quantity() {
    struct ibv_device **device_list = NULL;
    int device_quantity;

    // Get device list

    device_list = ibv_get_device_list(&device_quantity);

    if(device_list == NULL) {
        cerr << "Error obtaining device list" << endl;

        return -1;
    }

    ibv_free_device_list(device_list);
    return device_quantity;
}

int IBContext::get_port_quantity(int device_number) {
    struct ibv_device **device_list = NULL;
    int device_quantity;

    // Get device list

    device_list = ibv_get_device_list(&device_quantity);

    if(device_list == NULL) {
        cerr << "Error obtaining device list" << endl;

        return -1;
    }

    ibv_context *context = ibv_open_device(device_list[device_number]);

    if(context == NULL) {
        cerr << "Error opening device #" << device_number << endl;

        ibv_free_device_list(device_list);

        return -1;
    }

    int return_status;
    ibv_device_attr device_information;

    // Query device and port, store into context

    return_status = ibv_query_device(context, &device_information);

    if(return_status != 0) {
        cerr << "Error obtaining device information" << endl;

        ibv_free_device_list(device_list);
        ibv_close_device(context);

        return -1;
    }

    int device_ports = device_information.phys_port_cnt;

    ibv_free_device_list(device_list);
    ibv_close_device(context);

    return device_ports;
}

IBContext::IBContext(int device_number, int port_number, bool private_completion_queues, bool create_completion_channel): initialized{false}, device_number{device_number}, port_number{port_number}, private_completion_queues{private_completion_queues}, create_completion_channel{create_completion_channel} {
    struct ibv_device **device_list = NULL;
    int device_quantity;

    // Get device list

    device_list = ibv_get_device_list(&device_quantity);

    if(device_list == NULL) {
        cerr << "Error obtaining device list" << endl;

        exit(EXIT_FAILURE);
    }

    // Open device

    if(device_number >= device_quantity) {
        cerr << "Requested device does not exist" << endl;

        ibv_free_device_list(device_list);

        exit(EXIT_FAILURE);
    }

    context = ibv_open_device(device_list[device_number]);

    if(context == NULL) {
        cerr << "Error opening device #" << device_number << endl;

        ibv_free_device_list(device_list);

        exit(EXIT_FAILURE);
    }

    // Allocate protection domain

    protection_domain = ibv_alloc_pd(context);

    if(protection_domain == NULL) {
        cerr << "Error allocating protection domain" << endl;

        ibv_free_device_list(device_list);

        exit(EXIT_FAILURE);
    }

    int return_status;

    // Query device and port, store into context

    return_status = ibv_query_device(context, &device_information);

    if(return_status != 0) {
        cerr << "Error obtaining device information" << endl;

        ibv_free_device_list(device_list);

        exit(EXIT_FAILURE);
    }

    return_status = ibv_query_port(context, port_number, &port_information);

    if(return_status != 0) {
        cerr << "Error obtaining port information from device" << endl;

        ibv_free_device_list(device_list);

        exit(EXIT_FAILURE);
    }

    // Create completion queues

    recv_event_channel = nullptr;

    if(!private_completion_queues) {
        if(create_completion_channel) {
            recv_event_channel = ibv_create_comp_channel(context);

            if(!recv_event_channel) {
                cerr << "Error creating completion event channel" << endl;

                exit(EXIT_FAILURE);
            }
        }

        completion_queues = new IBCompletionQueues(context, MAX_COMPLETION_QUEUE_SIZE_SHARED, recv_event_channel);
    }
    else {
        completion_queues = nullptr;
    }

    struct ibv_srq_init_attr srq_attributes;
    memset(&srq_attributes, 0, sizeof(struct ibv_srq_init_attr));

    srq_attributes.srq_context = context;
    srq_attributes.attr.max_wr = MAX_SRQ_SIZE;
    srq_attributes.attr.max_sge = MAX_SRQ_SGE;

    shared_receive_queue = ibv_create_srq(protection_domain, &srq_attributes);

    if(shared_receive_queue == NULL) {
        cerr << "Error creating shared completion queue" << endl;

        ibv_free_device_list(device_list);

        exit(EXIT_FAILURE);
    }

    ibv_free_device_list(device_list);
    initialized = true;
}

IBContext::~IBContext() {
    if(!private_completion_queues && completion_queues != nullptr) {
        delete completion_queues;

        if(create_completion_channel && recv_event_channel != NULL) {
            if(ibv_destroy_comp_channel(recv_event_channel) != 0) {
                cerr << "Error destroying completion event channel" << endl;
            }
        }
    }

    if(shared_receive_queue != NULL) {
        ibv_destroy_srq(shared_receive_queue);
    }

    if(protection_domain != NULL) {
        ibv_dealloc_pd(protection_domain);
    }

    if(context != NULL) {
        ibv_close_device(context);
    }
}

IBQueuePair::IBQueuePair(IBContext *context, bool private_completion_queues, bool create_completion_channel): initialized{false}, completion_queues{nullptr}, private_completion_queues{private_completion_queues}, create_completion_channel{create_completion_channel} {
    struct ibv_qp_init_attr create_qp_attributes;
    memset(&create_qp_attributes, 0, sizeof(struct ibv_qp_init_attr));

    recv_event_channel = nullptr;

    if(private_completion_queues) {
        // Create completion channel and queues

        if(create_completion_channel) {
            recv_event_channel = ibv_create_comp_channel(context->context);

            if(!recv_event_channel) {
                cerr << "Error creating completion event channel" << endl;

                exit(EXIT_FAILURE);
            }
        }
        else {
            recv_event_channel = context->recv_event_channel;
        }

        completion_queues = new IBCompletionQueues(context->context, MAX_COMPLETION_QUEUE_SIZE_PRIVATE, recv_event_channel);

        create_qp_attributes.send_cq = completion_queues->send_completion_queue;
        create_qp_attributes.recv_cq = completion_queues->recv_completion_queue;
    }
    else {
        completion_queues = nullptr;

        create_qp_attributes.send_cq = context->completion_queues->send_completion_queue;
        create_qp_attributes.recv_cq = context->completion_queues->recv_completion_queue;
    }
    create_qp_attributes.srq = context->shared_receive_queue;
    create_qp_attributes.cap.max_send_wr = MAX_SEND_QUEUE_SIZE;
    create_qp_attributes.cap.max_send_sge = MAX_SEND_QUEUE_SGE;
    create_qp_attributes.cap.max_recv_wr = MAX_RECV_QUEUE_SIZE;
    create_qp_attributes.cap.max_recv_sge = MAX_RECV_QUEUE_SGE;
    create_qp_attributes.cap.max_inline_data = MAX_INLINE_DATA;
    create_qp_attributes.qp_type = IBV_QPT_RC;

    queue_pair = ibv_create_qp(context->protection_domain, &create_qp_attributes);

    if(queue_pair == NULL) {
        cout << "Error creating queue pair" << endl;

        return;
    }

    struct ibv_qp_attr modify_qp_attributes;
    memset(&modify_qp_attributes, 0, sizeof(struct ibv_qp_attr));

    modify_qp_attributes.qp_state = IBV_QPS_INIT;
    modify_qp_attributes.pkey_index = 0;
    modify_qp_attributes.port_num = context->port_number;
    modify_qp_attributes.qp_access_flags = IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_ATOMIC | IBV_ACCESS_LOCAL_WRITE;

    int return_value = ibv_modify_qp(queue_pair, &modify_qp_attributes, IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS);

    if(return_value != 0) {
        cout << "Error initializing queue pair" << endl;

        return;
    }

    initialized = true;
}

bool IBQueuePair::reset() {
    struct ibv_qp_attr modify_qp_attributes;
    memset(&modify_qp_attributes, 0, sizeof(struct ibv_qp_attr));

    modify_qp_attributes.qp_state = IBV_QPS_RESET;

    int return_value = ibv_modify_qp(queue_pair, &modify_qp_attributes, IBV_QP_STATE);

    if(return_value != 0) {
        cout << "Error resetting queue pair" << endl;

        return false;
    }

    return true;
}

IBQueuePair::~IBQueuePair() {
    if(queue_pair != NULL) {
        if(ibv_destroy_qp(queue_pair) != 0) {
            perror("Error destroying queue pair");
        }
    }

    if(private_completion_queues && completion_queues != nullptr) {
        delete completion_queues;

        if(create_completion_channel && recv_event_channel != nullptr) {
            if(ibv_destroy_comp_channel(recv_event_channel) != 0) {
                cerr << "Error destroying completion event channel" << endl;
            }
        }
    }
}

bool IBQueuePair::setup(IBContext *context, uint32_t peer_queue_pair, uint16_t peer_lid) {
    struct ibv_qp_attr queue_pair_attributes;
    memset(&(queue_pair_attributes), 0, sizeof(queue_pair_attributes));

    queue_pair_attributes.qp_state = IBV_QPS_RTR;
    queue_pair_attributes.rq_psn = 0;
    queue_pair_attributes.path_mtu = IBV_MTU_4096;
    queue_pair_attributes.dest_qp_num = peer_queue_pair;
    queue_pair_attributes.min_rnr_timer = 12;
    queue_pair_attributes.max_dest_rd_atomic = 1;
    queue_pair_attributes.ah_attr.dlid = peer_lid;
    queue_pair_attributes.ah_attr.port_num = context->port_number;

    int return_value = ibv_modify_qp(queue_pair, &queue_pair_attributes, IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU | IBV_QP_DEST_QPN | IBV_QP_RQ_PSN | IBV_QP_MIN_RNR_TIMER | IBV_QP_MAX_DEST_RD_ATOMIC);

    if(return_value != 0) {
        cerr << "Error moving queue pair to RTR state" << endl;

        return false;
    }

    queue_pair_attributes.qp_state = IBV_QPS_RTS;
    queue_pair_attributes.sq_psn = 0;
    queue_pair_attributes.timeout = 14;
    queue_pair_attributes.retry_cnt = 7;
    queue_pair_attributes.rnr_retry = 7;
    queue_pair_attributes.max_rd_atomic = 1;

    return_value = ibv_modify_qp(queue_pair, &queue_pair_attributes, IBV_QP_STATE | IBV_QP_SQ_PSN | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT | IBV_QP_RNR_RETRY | IBV_QP_MAX_QP_RD_ATOMIC);

    if(return_value != 0) {
        cerr << "Error moving queue pair to RTS state" << endl;

        return false;
    }

    return true;
}

RDMAMemory::RDMAMemory(IBContext *context, uint64_t size, uint64_t mode): buffer{nullptr}, size{size}, internal_memory{true}, internal_register{true}, operation_timestamp{0}, operation_tag{0}, sticky{false} {
    buffer = aligned_alloc(4096, size);

    if(buffer == nullptr) {
        cerr << "Error creating memory region: " << endl;

        exit(EXIT_FAILURE);
    }

    region = ibv_reg_mr(context->protection_domain, buffer, size, mode);

    if(region == NULL) {
        cerr << "Error registering memory region: " << endl;

        exit(EXIT_FAILURE);
    }

    local_key = region->lkey;
    remote_key = region->lkey;
}

RDMAMemory::RDMAMemory(IBContext *context, void *buffer, uint64_t size, uint64_t mode): buffer{buffer}, size{size}, internal_memory{false}, internal_register{true}, operation_timestamp{0}, operation_tag{0}, sticky{false} {
    region = ibv_reg_mr(context->protection_domain, buffer, size, mode);

    if(region == NULL) {
        cerr << "Error registering memory region: " << endl;

        exit(EXIT_FAILURE);
    }

    local_key = region->lkey;
    remote_key = region->lkey;
}