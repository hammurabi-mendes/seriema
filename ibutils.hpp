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

#ifndef IB_UTILS
#define IB_UTILS

#include <cstdint>
#include <atomic>

#include <iostream>
#include <vector>

#include <arpa/inet.h>
#include <infiniband/verbs.h>

#include "utils/TimeHolder.hpp"
#include "utils/Synchronizer.hpp"

// Maximum for my device is 928
#define MAX_INLINE_DATA 64

using std::atomic;

using std::cout;
using std::cerr;
using std::endl;

using std::vector;

class IBContext;
class IBQueuePair;
class RDMAMemory;
class RDMAMemoryLocator;
class SentMessageInformation;
class ReceivedMessageInformation;

class alignas(64) RDMAMemory {
private:
    void *buffer;
    uint64_t size;

    uint64_t offset = 0;

    struct ibv_mr *region;

    uint32_t local_key;
    uint32_t remote_key;

    bool internal_memory;
    bool internal_register;

public:
    uint64_t operation_timestamp;
    uint64_t operation_tag: 63;
    uint64_t sticky: 1;

    atomic<bool> ready{true};

    // Not inline, they perform memory registration and use the context class

    RDMAMemory(IBContext *context, uint64_t size = 4096, uint64_t mode = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE);
    RDMAMemory(IBContext *context, void *buffer, uint64_t size, uint64_t mode = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE);

    // Inline, they are simply "views" of larger buffers or other RDMAMemory objects

    inline RDMAMemory(void *buffer, uint64_t size): buffer{buffer}, size{size}, internal_memory{false}, internal_register{false}, operation_timestamp{0}, operation_tag{0}, sticky{false} {
        region = NULL;

        local_key = 0;
        remote_key = 0;
    }

    inline RDMAMemory(RDMAMemory *other, uint64_t size, uint64_t offset): size{size}, region{other->region}, internal_memory{false}, internal_register{false}, operation_timestamp{0}, operation_tag{0}, sticky{false} {
        buffer = other->get_buffer(offset);

        local_key = region->lkey;
        remote_key = region->lkey;
    }

    inline ~RDMAMemory() {
        if(internal_register && region != NULL) {
            ibv_dereg_mr(region);
        }

        if(internal_memory && buffer != NULL) {
            std::free(buffer);
        }
    }

    inline void *get_buffer() const noexcept {
        char *base = reinterpret_cast<char *>(buffer);

        return reinterpret_cast<void *>(base + offset);
    }

    inline void *get_buffer(uint64_t additional_offset) const noexcept {
        char *base = reinterpret_cast<char *>(buffer);

        return reinterpret_cast<void *>(base + offset + additional_offset);
    }

    inline uint64_t get_size() const noexcept {
        return size - offset;
    }

    inline uint32_t get_local_key() const noexcept {
        return local_key;
    }

    inline uint32_t get_remote_key() const noexcept {
        return remote_key;
    }

    inline void update_offset(uint64_t new_offset) noexcept {
        offset = new_offset;
    }
};

class RDMAMemoryLocator {
public:
    void *buffer;
    uint32_t remote_key;

    RDMAMemoryLocator() = default;
    RDMAMemoryLocator(const RDMAMemoryLocator &other) = default;

    RDMAMemoryLocator(RDMAMemory *memory) {
        buffer = memory->get_buffer();
        remote_key = memory->get_remote_key();
    }

    RDMAMemoryLocator(RDMAMemory *memory, uint64_t offset) {
        buffer = memory->get_buffer(offset);
        remote_key = memory->get_remote_key();
    }

    inline void *get_buffer() const noexcept {
        return buffer;
    }

    inline void *get_buffer(uint64_t offset) const noexcept {
        char *base = reinterpret_cast<char *>(buffer);

        return reinterpret_cast<void *>(base + offset);
    }

    inline uint32_t get_remote_key() const noexcept {
        return remote_key;
    }

    inline RDMAMemoryLocator &operator+=(int bytes) noexcept {
        buffer = (reinterpret_cast<char *>(buffer) + bytes);

        return *this;
    }

    inline RDMAMemoryLocator &operator-=(int bytes) noexcept {
        buffer = (reinterpret_cast<char *>(buffer) - bytes);

        return *this;
    }
};

class SentMessageInformation: public ibv_wc {
    inline RDMAMemory *get_memory() {
        return reinterpret_cast<RDMAMemory *>(wr_id);
    }

    inline void *get_buffer() {
        return get_memory()->get_buffer();
    }

    inline uint32_t get_size() {
        return byte_len;
    }

    inline bool has_immediate() {
        return (wc_flags & IBV_WC_WITH_IMM);
    }

    inline uint32_t get_immediate() {
        return ntohl(imm_data);
    }

    inline uint32_t get_queue_pair_number() {
        return this->qp_num;
    }
};

class ReceivedMessageInformation: public ibv_wc {
public:
    inline RDMAMemory *get_memory() {
        return reinterpret_cast<RDMAMemory *>(wr_id);
    }

    inline void *get_buffer() {
        return get_memory()->get_buffer();
    }

    inline uint32_t get_size() {
        return byte_len;
    }

    inline bool has_immediate() {
        return (wc_flags & IBV_WC_WITH_IMM);
    }

    inline uint32_t get_immediate() {
        return ntohl(imm_data);
    }

    inline uint32_t get_queue_pair_number() {
        return this->qp_num;
    }
};

class IBCompletionQueues {
public:
    struct ibv_cq *send_completion_queue;
    struct ibv_cq *recv_completion_queue;

    struct ibv_comp_channel *recv_event_channel;
    bool recv_event_only_solicited;

    bool initialized;

    IBCompletionQueues(ibv_context *context, int size, struct ibv_comp_channel *recv_event_channel = nullptr, bool recv_event_only_solicited = false);
    ~IBCompletionQueues();

    inline bool set_recv_event_notification() {
        if(recv_event_channel == nullptr) {
            return false;
        }

        bool result = (ibv_req_notify_cq(recv_completion_queue, (recv_event_only_solicited ? 1 : 0)) == 0);

        return result;
    }

    inline IBCompletionQueues *wait_recv_event() {
        struct ibv_cq *recv_completion_queue_detected;
        void *recv_completion_context_detected;

        if(ibv_get_cq_event(recv_event_channel, &recv_completion_queue_detected, &recv_completion_context_detected) != 0) {
            cerr << "Error waiting for receive event" << endl;

            return nullptr;
        }

        ibv_ack_cq_events(recv_completion_queue_detected, 1);
 
        if(!set_recv_event_notification()) {
            return nullptr;
        }

        return reinterpret_cast<IBCompletionQueues *>(recv_completion_context_detected);
    }

    inline int poll_send_completion_queue(SentMessageInformation *completed_work_request, int quantity = 1) noexcept {
        int number_completed = ibv_poll_cq(send_completion_queue, quantity, reinterpret_cast<ibv_wc *>(completed_work_request));

        for(int i = 0; i < number_completed; i++) {
            if(completed_work_request[i].status != IBV_WC_SUCCESS) {
                cerr << "Error sending data with ID " << completed_work_request[i].wr_id << ": " << ibv_wc_status_str(completed_work_request[i].status) << endl;

                continue;
            }

            uint64_t id = completed_work_request[i].wr_id;

            if(id) {
                Synchronizer *synchronizer = reinterpret_cast<Synchronizer *>(id);

                synchronizer->decrease();
            }
        }

        return number_completed;
    }

    inline int poll_recv_completion_queue(ReceivedMessageInformation *completed_work_request, int quantity = 1) noexcept {
        int number_completed = ibv_poll_cq(recv_completion_queue, quantity, reinterpret_cast<ibv_wc *>(completed_work_request));

        for(int i = 0; i < number_completed; i++) {
            if(completed_work_request[i].status != IBV_WC_SUCCESS) {
                cerr << "Error receiving data with ID " << completed_work_request[i].wr_id << ": " << ibv_wc_status_str(completed_work_request[i].status) << endl;

                continue;
            }
        }

        return number_completed;
    }

    inline int flush_send_completion_queue() noexcept {
        SentMessageInformation sent_message_information[128];

        int total_number_completed = 0;
        int number_completed = 0;

        while((number_completed = poll_send_completion_queue(sent_message_information, 128)) > 0) {
            total_number_completed += number_completed;
        }

        return total_number_completed;
    }
};

class IBContext {
public:
    int device_number;
    int port_number;

    struct ibv_context *context;
    struct ibv_pd *protection_domain;
    struct ibv_port_attr port_information;
    struct ibv_device_attr device_information;

    struct ibv_comp_channel *recv_event_channel;
    bool create_completion_channel;

    IBCompletionQueues *completion_queues;
    bool private_completion_queues;

    struct ibv_srq *shared_receive_queue;

    bool initialized;

    static int get_device_quantity();
    static int get_port_quantity(int device_number);

    IBContext(int device_number = 0, int port_number = 1, bool private_completion_queues = false, bool create_completion_channel = false);
    virtual ~IBContext();

    inline bool post_receive(RDMAMemory *memory) const noexcept {
        return post_receive(memory->get_buffer(), memory->get_size(), memory->get_local_key(), reinterpret_cast<uint64_t>(memory));
    }

    inline bool post_receive(RDMAMemory &memory) const noexcept {
        return post_receive(memory.get_buffer(), memory.get_size(), memory.get_local_key(), reinterpret_cast<uint64_t>(&memory));
    }

    inline bool post_receive(void *buffer, uint32_t buffer_size, uint32_t buffer_key) const noexcept {
        return post_receive(buffer, buffer_size, buffer_key, reinterpret_cast<uint64_t>(buffer));
    }

    inline bool post_receive(void *buffer, uint32_t buffer_size, uint32_t buffer_key, uint64_t request_id) const noexcept {
        struct ibv_sge sge_entry;
        memset(&sge_entry, 0, sizeof(struct ibv_sge));

        sge_entry.addr = reinterpret_cast<uint64_t>(buffer);
        sge_entry.length = buffer_size;
        sge_entry.lkey = buffer_key;

        struct ibv_recv_wr work_request;
        memset(&work_request, 0, sizeof(struct ibv_recv_wr));

        work_request.wr_id = request_id;
        work_request.sg_list = &sge_entry;
        work_request.num_sge = 1;

        struct ibv_recv_wr *bad_work_request;

        int return_value = ibv_post_srq_recv(shared_receive_queue, &work_request, &bad_work_request);

        if(return_value != 0) {
            cerr << "Error posting recv " << return_value << endl;

            return false;
        }

        return true;
    }
};

class IBQueuePair {
public:
    struct ibv_qp *queue_pair;

    IBCompletionQueues *completion_queues;
    bool private_completion_queues;

    struct ibv_comp_channel *recv_event_channel;
    bool create_completion_channel;

    bool initialized;

    IBQueuePair(IBContext *context, bool private_completion_queues = false, bool create_completion_channel = false);
    virtual ~IBQueuePair();

    bool setup(IBContext *context, uint32_t peer_queue_pair, uint16_t peer_lid);
    bool reset();

    inline bool post_send(RDMAMemory *memory, uint32_t offset, uint32_t size, uint64_t flags = 0, uint32_t *immediate_value = 0, Synchronizer *synchronizer = nullptr) const noexcept {
        struct ibv_sge sge_entry;
        memset(&sge_entry, 0, sizeof(struct ibv_sge));

        sge_entry.addr = reinterpret_cast<uint64_t>(memory->get_buffer(offset));
        sge_entry.length = size;
        sge_entry.lkey = memory->get_local_key();

        struct ibv_send_wr work_request;
        memset(&work_request, 0, sizeof(struct ibv_send_wr));

        work_request.wr_id = reinterpret_cast<uint64_t>(synchronizer);
        work_request.sg_list = &sge_entry;
        work_request.num_sge = 1;

        work_request.opcode = IBV_WR_SEND;

        if(immediate_value != nullptr) {
            work_request.opcode = IBV_WR_SEND_WITH_IMM;
            work_request.imm_data = htonl(*immediate_value);
        }

        if(synchronizer != nullptr) {
            flags |= IBV_SEND_SIGNALED;
        }

        work_request.send_flags = flags;

        struct ibv_send_wr *bad_work_request;

        int return_value = ibv_post_send(queue_pair, &work_request, &bad_work_request);

        if(return_value != 0) {
            cerr << "Error posting send " << return_value << endl;

            return false;
        }

        return true;
    }

    inline bool post_send_multiple(RDMAMemory **memories, uint32_t *offsets, uint32_t *sizes, uint32_t amount_buffers, uint64_t flags = 0, uint32_t *immediate_value = 0, Synchronizer *synchronizer = nullptr) const noexcept {
        struct ibv_sge sge_entries[amount_buffers];
        memset(&sge_entries, 0, amount_buffers * sizeof(struct ibv_sge));

        for(int i = 0; i < amount_buffers; i++) {
            sge_entries[i].addr = reinterpret_cast<uint64_t>(memories[i]->get_buffer(offsets[i]));
            sge_entries[i].length = sizes[i];
            sge_entries[i].lkey = memories[i]->get_local_key();
        }

        struct ibv_send_wr work_request;
        memset(&work_request, 0, sizeof(struct ibv_send_wr));

        work_request.wr_id = reinterpret_cast<uint64_t>(synchronizer);
        work_request.sg_list = sge_entries;
        work_request.num_sge = amount_buffers;

        work_request.opcode = IBV_WR_SEND;

        if(immediate_value != nullptr) {
            work_request.opcode = IBV_WR_SEND_WITH_IMM;
            work_request.imm_data = htonl(*immediate_value);
        }

        if(synchronizer != nullptr) {
            flags |= IBV_SEND_SIGNALED;
        }

        work_request.send_flags = flags;

        struct ibv_send_wr *bad_work_request;

        int return_value = ibv_post_send(queue_pair, &work_request, &bad_work_request);

        if(return_value != 0) {
            cerr << "Error posting send " << return_value << endl;

            return false;
        }

        return true;
    }

    inline bool post_rdma_read(RDMAMemory *memory, uint32_t offset, uint32_t size, const RDMAMemoryLocator *remote_memory_locator, uint64_t flags = 0, Synchronizer *synchronizer = nullptr) const noexcept {
        struct ibv_sge sge_entry;
        memset(&sge_entry, 0, sizeof(struct ibv_sge));

        sge_entry.addr = reinterpret_cast<uint64_t>(memory->get_buffer(offset));
        sge_entry.length = size;
        sge_entry.lkey = memory->get_local_key();

        struct ibv_send_wr work_request;
        memset(&work_request, 0, sizeof(struct ibv_send_wr));

        work_request.wr_id = reinterpret_cast<uint64_t>(synchronizer);
        work_request.sg_list = &sge_entry;
        work_request.num_sge = 1;

        work_request.opcode = IBV_WR_RDMA_READ;

        if(synchronizer != nullptr) {
            flags |= IBV_SEND_SIGNALED;
        }

        work_request.send_flags = flags;

        work_request.wr.rdma.remote_addr = reinterpret_cast<uint64_t>(remote_memory_locator->get_buffer());
        work_request.wr.rdma.rkey = remote_memory_locator->get_remote_key();

        struct ibv_send_wr *bad_work_request;

        int return_value = ibv_post_send(queue_pair, &work_request, &bad_work_request);

        if(return_value != 0) {
            cerr << "Error posting send " << return_value << endl;

            return false;
        }

        return true;
    }

    inline bool post_rdma_read_multiple(RDMAMemory **memories, uint64_t *offsets, uint32_t *sizes, uint32_t amount_buffers, const RDMAMemoryLocator *remote_memory_locator, uint64_t flags = 0, Synchronizer *synchronizer = nullptr) const noexcept {
        struct ibv_sge sge_entries[amount_buffers];
        memset(&sge_entries, 0, amount_buffers * sizeof(struct ibv_sge));

        for(int i = 0; i < amount_buffers; i++) {
            sge_entries[i].addr = reinterpret_cast<uint64_t>(memories[i]->get_buffer(offsets[i]));
            sge_entries[i].length = sizes[i];
            sge_entries[i].lkey = memories[i]->get_local_key();
        }

        struct ibv_send_wr work_request;
        memset(&work_request, 0, sizeof(struct ibv_send_wr));

        work_request.wr_id = reinterpret_cast<uint64_t>(synchronizer);
        work_request.sg_list = sge_entries;
        work_request.num_sge = amount_buffers;

        work_request.opcode = IBV_WR_RDMA_READ;

        if(synchronizer != nullptr) {
            flags |= IBV_SEND_SIGNALED;
        }

        work_request.send_flags = flags;

        work_request.wr.rdma.remote_addr = reinterpret_cast<uint64_t>(remote_memory_locator->get_buffer());
        work_request.wr.rdma.rkey = remote_memory_locator->get_remote_key();

        struct ibv_send_wr *bad_work_request;

        int return_value = ibv_post_send(queue_pair, &work_request, &bad_work_request);

        if(return_value != 0) {
            cerr << "Error posting send " << return_value << endl;

            return false;
        }

        return true;
    }

    inline bool post_rdma_write(RDMAMemory *memory, uint32_t offset, uint32_t size, const RDMAMemoryLocator *remote_memory_locator, uint64_t flags = 0, uint32_t *immediate_value = 0, Synchronizer *synchronizer = nullptr) const noexcept {
        struct ibv_sge sge_entry;
        memset(&sge_entry, 0, sizeof(struct ibv_sge));

        sge_entry.addr = reinterpret_cast<uint64_t>(memory->get_buffer(offset));
        sge_entry.length = size;
        sge_entry.lkey = memory->get_local_key();

        struct ibv_send_wr work_request;
        memset(&work_request, 0, sizeof(struct ibv_send_wr));

        work_request.wr_id = reinterpret_cast<uint64_t>(synchronizer);
        work_request.sg_list = &sge_entry;
        work_request.num_sge = 1;

        work_request.opcode = IBV_WR_RDMA_WRITE;

        if(immediate_value != nullptr) {
            work_request.opcode = IBV_WR_RDMA_WRITE_WITH_IMM;
            work_request.imm_data = htonl(*immediate_value);
        }

        if(synchronizer != nullptr) {
            flags |= IBV_SEND_SIGNALED;
        }

        work_request.send_flags = flags;

        work_request.wr.rdma.remote_addr = reinterpret_cast<uint64_t>(remote_memory_locator->get_buffer());
        work_request.wr.rdma.rkey = remote_memory_locator->get_remote_key();

        struct ibv_send_wr *bad_work_request;

        int return_value = ibv_post_send(queue_pair, &work_request, &bad_work_request);

        if(return_value != 0) {
            cerr << "Error posting send " << return_value << endl;

            return false;
        }

        return true;
    }

    inline bool post_rdma_write_multiple(RDMAMemory **memories, uint64_t *offsets, uint32_t *sizes, uint32_t amount_buffers, const RDMAMemoryLocator *remote_memory_locator, uint64_t flags = 0, uint32_t *immediate_value = 0, Synchronizer *synchronizer = nullptr) const noexcept {
        struct ibv_sge sge_entries[amount_buffers];
        memset(&sge_entries, 0, amount_buffers * sizeof(struct ibv_sge));

        for(int i = 0; i < amount_buffers; i++) {
            sge_entries[i].addr = reinterpret_cast<uint64_t>(memories[i]->get_buffer(offsets[i]));
            sge_entries[i].length = sizes[i];
            sge_entries[i].lkey = memories[i]->get_local_key();
        }

        struct ibv_send_wr work_request;
        memset(&work_request, 0, sizeof(struct ibv_send_wr));

        work_request.wr_id = reinterpret_cast<uint64_t>(synchronizer);
        work_request.sg_list = sge_entries;
        work_request.num_sge = amount_buffers;

        work_request.opcode = IBV_WR_RDMA_WRITE;

        if(immediate_value != nullptr) {
            work_request.opcode = IBV_WR_RDMA_WRITE_WITH_IMM;
            work_request.imm_data = htonl(*immediate_value);
        }

        if(synchronizer != nullptr) {
            flags |= IBV_SEND_SIGNALED;
        }

        work_request.send_flags = flags;

        work_request.wr.rdma.remote_addr = reinterpret_cast<uint64_t>(remote_memory_locator->get_buffer());
        work_request.wr.rdma.rkey = remote_memory_locator->get_remote_key();

        struct ibv_send_wr *bad_work_request;

        int return_value = ibv_post_send(queue_pair, &work_request, &bad_work_request);

        if(return_value != 0) {
            cerr << "Error posting send " << return_value << endl;

            return false;
        }

        return true;
    }

    inline bool post_cas(RDMAMemory *memory, uint32_t offset, uint32_t size, const RDMAMemoryLocator *remote_memory_locator, uint64_t expected, uint64_t changed, uint64_t flags = 0, Synchronizer *synchronizer = nullptr) const noexcept {
        struct ibv_sge sge_entry;
        memset(&sge_entry, 0, sizeof(struct ibv_sge));

        sge_entry.addr = reinterpret_cast<uint64_t>(memory->get_buffer(offset));
        sge_entry.length = size;
        sge_entry.lkey = memory->get_local_key();

        struct ibv_send_wr work_request;
        memset(&work_request, 0, sizeof(struct ibv_send_wr));

        work_request.wr_id = reinterpret_cast<uint64_t>(synchronizer);
        work_request.sg_list = &sge_entry;
        work_request.num_sge = 1;

        work_request.opcode = IBV_WR_ATOMIC_CMP_AND_SWP;

        if(synchronizer != nullptr) {
            flags |= IBV_SEND_SIGNALED;
        }

        work_request.send_flags = flags;

        work_request.wr.atomic.remote_addr = reinterpret_cast<uint64_t>(remote_memory_locator->get_buffer());
        work_request.wr.atomic.rkey = remote_memory_locator->get_remote_key();
        work_request.wr.atomic.compare_add = expected;
        work_request.wr.atomic.swap = changed;

        struct ibv_send_wr *bad_work_request;

        int return_value = ibv_post_send(queue_pair, &work_request, &bad_work_request);

        if(return_value != 0) {
            cerr << "Error posting send " << return_value << endl;

            return false;
        }

        return true;
    }

    inline bool post_fetch_increment(RDMAMemory *memory, uint32_t offset, uint32_t size, const RDMAMemoryLocator *remote_memory_locator, uint64_t increment, uint64_t flags = 0, Synchronizer *synchronizer = nullptr) const noexcept {
        struct ibv_sge sge_entry;
        memset(&sge_entry, 0, sizeof(struct ibv_sge));

        sge_entry.addr = reinterpret_cast<uint64_t>(memory->get_buffer(offset));
        sge_entry.length = size;
        sge_entry.lkey = memory->get_local_key();

        struct ibv_send_wr work_request;
        memset(&work_request, 0, sizeof(struct ibv_send_wr));

        work_request.wr_id = reinterpret_cast<uint64_t>(synchronizer);
        work_request.sg_list = &sge_entry;
        work_request.num_sge = 1;

        work_request.opcode = IBV_WR_ATOMIC_FETCH_AND_ADD;

        if(synchronizer != nullptr) {
            flags |= IBV_SEND_SIGNALED;
        }

        work_request.send_flags = flags;

        work_request.wr.atomic.remote_addr = reinterpret_cast<uint64_t>(remote_memory_locator->get_buffer());
        work_request.wr.atomic.rkey = remote_memory_locator->get_remote_key();
        work_request.wr.atomic.compare_add = increment;

        struct ibv_send_wr *bad_work_request;

        int return_value = ibv_post_send(queue_pair, &work_request, &bad_work_request);

        if(return_value != 0) {
            cerr << "Error posting send " << return_value << endl;

            return false;
        }

        return true;
    }

    inline bool post_receive(RDMAMemory *memory) const noexcept {
        return post_receive(memory->get_buffer(), memory->get_size(), memory->get_local_key(), reinterpret_cast<uint64_t>(memory));
    }

    inline bool post_receive(RDMAMemory &memory) const noexcept {
        return post_receive(memory.get_buffer(), memory.get_size(), memory.get_local_key(), reinterpret_cast<uint64_t>(&memory));
    }

    inline bool post_receive(void *buffer, uint32_t buffer_size, uint32_t buffer_key) const noexcept {
        return post_receive(buffer, buffer_size, buffer_key, reinterpret_cast<uint64_t>(buffer));
    }

    inline bool post_receive(void *buffer, uint32_t buffer_size, uint32_t buffer_key, uint64_t request_id) const noexcept {
        struct ibv_sge sge_entry;
        memset(&sge_entry, 0, sizeof(struct ibv_sge));

        sge_entry.addr = reinterpret_cast<uint64_t>(buffer);
        sge_entry.length = buffer_size;
        sge_entry.lkey = buffer_key;

        struct ibv_recv_wr work_request;
        memset(&work_request, 0, sizeof(struct ibv_recv_wr));

        work_request.wr_id = request_id;
        work_request.sg_list = &sge_entry;
        work_request.num_sge = 1;

        struct ibv_recv_wr *bad_work_request;

        int return_value = ibv_post_recv(queue_pair, &work_request, &bad_work_request);

        if(return_value != 0) {
            cerr << "Error posting recv " << return_value << endl;

            return false;
        }

        return true;
    }
};

template<uint64_t FLUSH_INTERVAL = 15360, uint64_t THREAD_FLUSH_INTERVAL = 128>
struct alignas(64) IBTransmitter {
    atomic<uint64_t> outgoing_batches;
    atomic<uint64_t> completed_flushes;
    atomic<uint64_t> completed_timestamp;

    IBContext *context;
    IBQueuePair *queue_pair;

    IBCompletionQueues *completion_queues;

    uint64_t operation_tag;

    IBTransmitter(IBContext *context, IBQueuePair *queue_pair, uint64_t operation_tag): context{context}, queue_pair{queue_pair}, operation_tag{operation_tag}, outgoing_batches{FLUSH_INTERVAL}, completed_flushes{1}, completed_timestamp{1} {
        if(context->completion_queues) {
            completion_queues = context->completion_queues;
        }
        else {
            completion_queues = queue_pair->completion_queues;
        }
    }

    inline uint64_t update_timestamp(uint64_t new_completed_timestamp) {
        uint64_t observed = completed_timestamp;
        uint64_t proposed = new_completed_timestamp;

        while(observed < proposed) {
            if(completed_timestamp.compare_exchange_strong(observed, proposed)) {
                return proposed;
            }
        }

        return observed;
    }

    inline uint64_t update_flush_counters(uint64_t flush_number, uint64_t new_completed_timestamp) {
        uint64_t observed = completed_flushes;
        uint64_t proposed = flush_number;

        if(observed + 1 == proposed) {
            completed_flushes.compare_exchange_strong(observed, proposed);
        }

        return update_timestamp(new_completed_timestamp);
    }

    inline bool send_batches(RDMAMemory *memory, uint64_t offset, uint64_t size, uint64_t flags, uint32_t *immediate_value, Synchronizer *synchronizer) noexcept {
        thread_local uint64_t thread_outgoing = 0;

        uint64_t number_batches = size / INT32_MAX;

        if(size % INT32_MAX != 0) {
            number_batches++;
        }

        uint32_t batch_size = INT32_MAX;

        for(uint64_t batch = 0; batch < number_batches; batch++) {
            if(batch == number_batches - 1) {
                batch_size = size % INT32_MAX;
            }

            uint64_t snapshot_outgoing_batches = outgoing_batches.fetch_add(1, std::memory_order::memory_order_relaxed) + 1;
            uint64_t flush_number = (snapshot_outgoing_batches / FLUSH_INTERVAL);

            if((thread_outgoing++ % THREAD_FLUSH_INTERVAL) == 0 || completed_flushes < flush_number) {
                Synchronizer flush_synchronizer{1};

                queue_pair->post_send(memory, offset, batch_size, flags, immediate_value, &flush_synchronizer);

                while(flush_synchronizer.get_number_operations_left() > 0) {
                    completion_queues->flush_send_completion_queue();
                }

                if(batch == number_batches - 1 && synchronizer) {
                    synchronizer->decrease();
                }

                update_flush_counters(flush_number, snapshot_outgoing_batches);
            }
            else {
                queue_pair->post_send(memory, offset, batch_size, flags, immediate_value, synchronizer);
            }

            offset += batch_size;
        }

        memory->operation_tag = operation_tag;
        memory->operation_timestamp = outgoing_batches.load(std::memory_order::memory_order_relaxed);

        return true;
    }

    inline bool rdma_write_batches(RDMAMemory *memory, uint64_t offset, uint64_t size, const RDMAMemoryLocator *remote_memory_locator, uint64_t flags, uint32_t *immediate_value, Synchronizer *synchronizer) noexcept {
        thread_local uint64_t thread_outgoing = 0;

        uint64_t number_batches = size / INT32_MAX;

        if(size % INT32_MAX != 0) {
            number_batches++;
        }

        RDMAMemoryLocator adjusted_memory_locator(*remote_memory_locator);

        uint32_t batch_size = INT32_MAX;

        for(uint64_t batch = 0; batch < number_batches; batch++) {
            if(batch == number_batches - 1) {
                batch_size = size % INT32_MAX;
            }

            uint64_t snapshot_outgoing_batches = outgoing_batches.fetch_add(1, std::memory_order::memory_order_relaxed) + 1;
            uint64_t flush_number = (snapshot_outgoing_batches / FLUSH_INTERVAL);

            if((thread_outgoing++ % THREAD_FLUSH_INTERVAL) == 0 || completed_flushes < flush_number) {
                Synchronizer flush_synchronizer{1};

                queue_pair->post_rdma_write(memory, offset, batch_size, &adjusted_memory_locator, flags, immediate_value, &flush_synchronizer);

                while(flush_synchronizer.get_number_operations_left() > 0) {
                    completion_queues->flush_send_completion_queue();
                }

                if(batch == number_batches - 1 && synchronizer) {
                    synchronizer->decrease();
                }

                update_flush_counters(flush_number, snapshot_outgoing_batches);
            }
            else {
                queue_pair->post_rdma_write(memory, offset, batch_size, &adjusted_memory_locator, flags, immediate_value, synchronizer);
            }

            offset += batch_size;
            adjusted_memory_locator += batch_size;
        }

        memory->operation_tag = operation_tag;
        memory->operation_timestamp = outgoing_batches.load(std::memory_order::memory_order_relaxed);

        return true;
    }

    inline bool rdma_read_batches(RDMAMemory *memory, uint64_t offset, uint64_t size, const RDMAMemoryLocator *remote_memory_locator, uint64_t flags, Synchronizer *synchronizer) noexcept {
        thread_local uint64_t thread_outgoing = 0;

        uint64_t number_batches = size / INT32_MAX;

        if(size % INT32_MAX != 0) {
            number_batches++;
        }

        RDMAMemoryLocator adjusted_memory_locator(*remote_memory_locator);

        uint32_t batch_size = INT32_MAX;

        for(uint64_t batch = 0; batch < number_batches; batch++) {
            if(batch == number_batches - 1) {
                batch_size = size % INT32_MAX;
            }

            uint64_t snapshot_outgoing_batches = outgoing_batches.fetch_add(1, std::memory_order::memory_order_relaxed) + 1;
            uint64_t flush_number = (snapshot_outgoing_batches / FLUSH_INTERVAL);

            if((thread_outgoing++ % THREAD_FLUSH_INTERVAL) == 0 || completed_flushes < flush_number) {
                Synchronizer flush_synchronizer{1};

                queue_pair->post_rdma_read(memory, offset, batch_size, &adjusted_memory_locator, flags, &flush_synchronizer);

                while(flush_synchronizer.get_number_operations_left() > 0) {
                    completion_queues->flush_send_completion_queue();
                }

                if(batch == number_batches - 1 && synchronizer) {
                    synchronizer->decrease();
                }

                update_flush_counters(flush_number, snapshot_outgoing_batches);
            }
            else {
                queue_pair->post_rdma_read(memory, offset, batch_size, &adjusted_memory_locator, flags, synchronizer);
            }

            offset += batch_size;
            adjusted_memory_locator += batch_size;
        }

        memory->operation_tag = operation_tag;
        memory->operation_timestamp = outgoing_batches.load(std::memory_order::memory_order_relaxed);

        return true;
    }

    inline bool send(RDMAMemory *memory, uint64_t offset, int32_t size, uint64_t flags = 0, uint32_t *immediate_value = 0, Synchronizer *synchronizer = nullptr) noexcept {
        thread_local uint64_t thread_outgoing = 0;

        uint64_t snapshot_outgoing_batches = outgoing_batches.fetch_add(1, std::memory_order::memory_order_relaxed) + 1;
        uint64_t flush_number = (snapshot_outgoing_batches / FLUSH_INTERVAL);

        if((thread_outgoing++ % THREAD_FLUSH_INTERVAL) == 0 || completed_flushes < flush_number) {
            Synchronizer flush_synchronizer{1};

            queue_pair->post_send(memory, offset, size, flags, immediate_value, &flush_synchronizer);

            while(flush_synchronizer.get_number_operations_left() > 0) {
                completion_queues->flush_send_completion_queue();
            }

            if(synchronizer) {
                synchronizer->decrease();
            }

            update_flush_counters(flush_number, snapshot_outgoing_batches);
        }
        else {
            queue_pair->post_send(memory, offset, size, flags, immediate_value, synchronizer);
        }

        memory->operation_tag = operation_tag;
        memory->operation_timestamp = outgoing_batches.load(std::memory_order::memory_order_relaxed);

        return true;
    }

    inline bool rdma_write(RDMAMemory *memory, uint64_t offset, int32_t size, const RDMAMemoryLocator *remote_memory_locator, uint64_t flags = 0, uint32_t *immediate_value = 0, Synchronizer *synchronizer = nullptr) noexcept {
        thread_local uint64_t thread_outgoing = 0;

        uint64_t snapshot_outgoing_batches = outgoing_batches.fetch_add(1, std::memory_order::memory_order_relaxed) + 1;
        uint64_t flush_number = (snapshot_outgoing_batches / FLUSH_INTERVAL);

        if((thread_outgoing++ % THREAD_FLUSH_INTERVAL) == 0 || completed_flushes < flush_number) {
            Synchronizer flush_synchronizer{1};

            queue_pair->post_rdma_write(memory, offset, size, remote_memory_locator, flags, immediate_value, &flush_synchronizer);

            while(flush_synchronizer.get_number_operations_left() > 0) {
                completion_queues->flush_send_completion_queue();
            }

            if(synchronizer) {
                synchronizer->decrease();
            }

            update_flush_counters(flush_number, snapshot_outgoing_batches);
        }
        else {
            queue_pair->post_rdma_write(memory, offset, size, remote_memory_locator, flags, immediate_value, synchronizer);
        }

        memory->operation_tag = operation_tag;
        memory->operation_timestamp = outgoing_batches.load(std::memory_order::memory_order_relaxed);

        return true;
    }

    inline bool rdma_read(RDMAMemory *memory, uint64_t offset, int32_t size, const RDMAMemoryLocator *remote_memory_locator, uint64_t flags = 0, Synchronizer *synchronizer = nullptr) noexcept {
        thread_local uint64_t thread_outgoing = 0;

        uint64_t snapshot_outgoing_batches = outgoing_batches.fetch_add(1, std::memory_order::memory_order_relaxed) + 1;
        uint64_t flush_number = (snapshot_outgoing_batches / FLUSH_INTERVAL);

        if((thread_outgoing++ % THREAD_FLUSH_INTERVAL) == 0 || completed_flushes < flush_number) {
            Synchronizer flush_synchronizer{1};

            queue_pair->post_rdma_read(memory, offset, size, remote_memory_locator, flags, &flush_synchronizer);

            while(flush_synchronizer.get_number_operations_left() > 0) {
                completion_queues->flush_send_completion_queue();
            }

            if(synchronizer) {
                synchronizer->decrease();
            }

            update_flush_counters(flush_number, snapshot_outgoing_batches);
        }
        else {
            queue_pair->post_rdma_read(memory, offset, size, remote_memory_locator, flags, synchronizer);
        }

        memory->operation_tag = operation_tag;
        memory->operation_timestamp = outgoing_batches.load(std::memory_order::memory_order_relaxed);

        return true;
    }

    inline bool rdma_write_multiple(RDMAMemory **memories, uint64_t *offsets, uint32_t *sizes, uint32_t amount_buffers, const RDMAMemoryLocator *remote_memory_locator, uint64_t flags = 0, uint32_t *immediate_value = 0, Synchronizer *synchronizer = nullptr) noexcept {
        thread_local uint64_t thread_outgoing = 0;

        uint64_t snapshot_outgoing_batches = outgoing_batches.fetch_add(1, std::memory_order::memory_order_relaxed) + 1;
        uint64_t flush_number = (snapshot_outgoing_batches / FLUSH_INTERVAL);

        if((thread_outgoing++ % THREAD_FLUSH_INTERVAL) == 0 || completed_flushes < flush_number) {
            Synchronizer flush_synchronizer{1};

            queue_pair->post_rdma_write_multiple(memories, offsets, sizes, amount_buffers, remote_memory_locator, flags, immediate_value, &flush_synchronizer);

            while(flush_synchronizer.get_number_operations_left() > 0) {
                completion_queues->flush_send_completion_queue();
            }

            if(synchronizer) {
                synchronizer->decrease();
            }

            update_flush_counters(flush_number, snapshot_outgoing_batches);
        }
        else {
            queue_pair->post_rdma_write_multiple(memories, offsets, sizes, amount_buffers, remote_memory_locator, flags, immediate_value, synchronizer);
        }

        for(uint32_t i = 0; i < amount_buffers; i++) {
            memories[i]->operation_tag = operation_tag;
            memories[i]->operation_timestamp = outgoing_batches.load(std::memory_order::memory_order_relaxed);
        }

        return true;
    }

    inline bool rdma_read_multiple(RDMAMemory **memories, uint64_t *offsets, uint32_t *sizes, uint32_t amount_buffers, const RDMAMemoryLocator *remote_memory_locator, uint64_t flags = 0, Synchronizer *synchronizer = nullptr) noexcept {
        thread_local uint64_t thread_outgoing = 0;

        uint64_t snapshot_outgoing_batches = outgoing_batches.fetch_add(1, std::memory_order::memory_order_relaxed) + 1;
        uint64_t flush_number = (snapshot_outgoing_batches / FLUSH_INTERVAL);

        if((thread_outgoing++ % THREAD_FLUSH_INTERVAL) == 0 || completed_flushes < flush_number) {
            Synchronizer flush_synchronizer{1};

            queue_pair->post_rdma_read_multiple(memories, offsets, sizes, amount_buffers, remote_memory_locator, flags, &flush_synchronizer);

            while(flush_synchronizer.get_number_operations_left() > 0) {
                completion_queues->flush_send_completion_queue();
            }

            if(synchronizer) {
                synchronizer->decrease();
            }

            update_flush_counters(flush_number, snapshot_outgoing_batches);
        }
        else {
            queue_pair->post_rdma_read_multiple(memories, offsets, sizes, amount_buffers, remote_memory_locator, flags, synchronizer);
        }

        for(uint32_t i = 0; i < amount_buffers; i++) {
            memories[i]->operation_tag = operation_tag;
            memories[i]->operation_timestamp = outgoing_batches.load(std::memory_order::memory_order_relaxed);
        }

        return true;
    }
};

#endif /* IB_UTILS */