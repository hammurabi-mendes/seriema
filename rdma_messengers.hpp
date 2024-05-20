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

#ifndef RDMA_MESSENGER_H
#define RDMA_MESSENGER_H

#include <deque>
#include <cstring>

#include "remote_calls.hpp"
#include "remote_objects.hpp"

using std::deque;

namespace dsys {

extern Mapper<uint64_t> messenger_map;

// Main definitions

struct Header {
    struct Setup {
        uint64_t producer_id;
        RDMAMemoryLocator producer_locator;
    } setup;

    struct Producer {
        uint64_t offset: 61;

        bool fetch: 1;
        bool rotate: 1;
        bool shutdown: 1;

        Producer(): offset{0}, rotate{false}, fetch{false}, shutdown{false} {}
    } producer;

    struct Consumer {
        uint64_t offset;

        Consumer(): offset{0} {}
    } consumer;
};

template<uint64_t NUMBER_CHUNKS_MAXIMUM = 2>
class RDMAMessenger {
public:
    static constexpr uint64_t NUMBER_CHUNKS_ALLOCATION = 2;

protected:
    const int destination_thread_id;

    /////////////////////
    // Incoming Memory //
    /////////////////////

    deque<RDMAMemory *> incoming_chunks;
    RDMAMemory *incoming_chunks_front = nullptr;

    uint64_t incoming_position = 0;
    uint64_t incoming_position_total = 0;

    bool incoming_shutdown = false;
    bool used_incoming = false;

    /////////////////////
    // Outgoing Memory //
    /////////////////////

    deque<RDMAMemoryLocator> outgoing_chunks;

    uint64_t outgoing_position = 0;
    uint64_t outgoing_position_total = 0;

    bool outgoing_shutdown = false;
    bool used_outgoing = false;

    /////////////////////////////
    // Outgoing Offset Control //
    /////////////////////////////

    RDMAMemoryLocator original_remote_memory_locator;
    RDMAMemoryLocator adjusted_remote_memory_locator;

    ///////////////////////////////////
    // Flush and Consumption Control //
    ///////////////////////////////////

    uint64_t last_flushed_offset = 0;
    unordered_map<void *, uint64_t> last_flushed_offsets;

    uint64_t last_consumed_offset = 0;

    ///////////////////////////////////////////
    // Memory Allocation & Rotation Protocol //
    ///////////////////////////////////////////

    struct RetrievalHeader {
        RDMAMemoryLocator memory_locators[NUMBER_CHUNKS_ALLOCATION];
        bool done;
    };

    RDMAMemory *request_memory = nullptr;

    RDMAMemory *self_memory = nullptr;

    RDMAMemoryLocator peer_memory_locator;
    RDMAMemoryLocator peer_last_consumed_offset_locator;

    Synchronizer inter_flush_synchronizer{0};

    bool processing_calls = false;

public:
    RDMAMessenger(int destination_thread_id): destination_thread_id{destination_thread_id} {
        original_remote_memory_locator.buffer = nullptr;
        adjusted_remote_memory_locator.buffer = nullptr;

        peer_memory_locator.buffer = nullptr;
    }

    virtual ~RDMAMessenger() {
        for(RDMAMemory *memory: incoming_chunks) {
            delete memory;
        }

        if(self_memory != nullptr) {
            delete self_memory;
        }
    }

    ////////////////////////
    // Accessor functions //
    ////////////////////////

    inline bool get_incoming_shutdown() const noexcept {
        return !used_incoming || incoming_shutdown;
    }

    inline bool get_outgoing_shutdown() const noexcept {
        return !used_outgoing || outgoing_shutdown;
    }

    ///////////////////////////////
    // Flush and reset functions //
    ///////////////////////////////

    inline bool is_fully_consumed(RDMAMemoryLocator &remote_memory_locator) noexcept {
        if(get_incoming_shutdown()) {
            Synchronizer synchronizer{1};

            RDMAMemory *memory = thread_context->incoming_allocator->allocate_incoming();
            Header *memory_header = reinterpret_cast<Header *>(memory->get_buffer());

            get_transmitter(destination_thread_id)->rdma_read(memory, 0, sizeof(Header), &remote_memory_locator, 0, &synchronizer);

            synchronizer.spin_nonzero_operations_left();

            bool result = (memory_header->consumer.offset == memory_header->producer.offset) && !memory_header->producer.rotate && !memory_header->producer.fetch && !memory_header->producer.shutdown;

            // Make incoming memory reusable
            memory->ready = true;

            return result;
        }

        uint64_t target_flushed_offset = last_flushed_offsets[remote_memory_locator.buffer];

        if(last_consumed_offset < target_flushed_offset) {
            return false;
        }

        return true;
    }

    inline void update_consumed_offset(uint64_t consumed_offset) noexcept {
        RDMAMemory *memory = thread_context->fine_outgoing_allocator->allocate_outgoing(sizeof(uint64_t));

        uint64_t *memory_header = reinterpret_cast<uint64_t *>(memory->get_buffer());

        *memory_header = consumed_offset;

        get_transmitter(destination_thread_id)->rdma_write(memory, 0, sizeof(uint64_t), &peer_last_consumed_offset_locator, 0, nullptr);
    }

    inline bool is_fully_flushed() noexcept {
        uint64_t this_flush_offset = outgoing_position_total + outgoing_position;

        return (adjusted_remote_memory_locator.buffer == nullptr) || (last_flushed_offset == this_flush_offset);
    }

    inline void flush_internal(bool rotate = false, bool fetch = false, bool shutdown = false, Synchronizer *synchronizer = nullptr) noexcept {
        if(request_memory != nullptr) {
            // Special case for when the shutdown happens between memory rotations
            // "rotate" and "flush" are irrelevant after shutdown

#ifdef DEBUG
            if(!shutdown && (rotate || fetch)) {
                cerr << "Rotate/fetch requested during remote memory allocation!" << endl;
                exit(EXIT_FAILURE);
            }
#endif /* DEBUG */

            while(!obtain_memory()) {
                cout << "Shutdown requested during remote memory allocation - waiting" << endl;
            }
        }

        RDMAMemoryLocator producer_locator = original_remote_memory_locator;
        producer_locator += offsetof(Header, producer);

        // Wait for the previous operation to complete
        inter_flush_synchronizer.spin_nonzero_operations_left();

        RDMAMemory *memory = thread_context->fine_outgoing_allocator->allocate_outgoing(sizeof(Header::Producer));
        Header::Producer *memory_header_producer = reinterpret_cast<Header::Producer *>(memory->get_buffer());

        uint64_t current_flush_offset = outgoing_position_total + outgoing_position;

        memory_header_producer->rotate = rotate;
        memory_header_producer->fetch = fetch;
        memory_header_producer->shutdown = shutdown;
        memory_header_producer->offset = current_flush_offset;

        if(!synchronizer && !shutdown) {
            synchronizer = &inter_flush_synchronizer;

            inter_flush_synchronizer.reset(1);
        }

        get_transmitter(destination_thread_id)->rdma_write(memory, 0, sizeof(Header::Producer), &producer_locator, 0, nullptr, synchronizer);

        // Update flush and shutdown markers

        if(shutdown) {
            outgoing_shutdown = true;
        }

        last_flushed_offset = current_flush_offset;
        last_flushed_offsets[original_remote_memory_locator.buffer] = current_flush_offset;
    }

    inline void enqueue_incoming_memory() noexcept {
        FastQueuePC<RDMAMemory *> &incoming_memory_queue = incoming_memory_queues[get_thread_rank(thread_id)];

        while(incoming_memory_queue.size() > 0) {
            RDMAMemory *incoming_memory = incoming_memory_queue.pop_remove();

            Header *memory_header = reinterpret_cast<Header *>(incoming_memory->get_buffer());

            thread_context->incoming_memory_map[memory_header->setup.producer_id].push(incoming_memory);
        }
    }

    inline void retrieve_incoming_memory() noexcept {
        enqueue_incoming_memory();

        if(thread_context->incoming_memory_map[destination_thread_id].size() < NUMBER_CHUNKS_ALLOCATION) {
            return;
        }

        used_incoming = true;

        if(peer_memory_locator.buffer == nullptr) {
            RDMAMemory *memory = thread_context->incoming_memory_map[destination_thread_id].front();
            Header *memory_header = reinterpret_cast<Header *>(memory->get_buffer());

            peer_memory_locator = memory_header->setup.producer_locator;

            peer_last_consumed_offset_locator = peer_memory_locator;
            peer_last_consumed_offset_locator += (((uint64_t) &last_consumed_offset) - ((uint64_t) this));
        }

        for(int i = 0; i < NUMBER_CHUNKS_ALLOCATION; i++) {
            incoming_chunks.emplace_front(thread_context->incoming_memory_map[destination_thread_id].front());

            thread_context->incoming_memory_map[destination_thread_id].pop();
        }
    }

    inline bool retrieve_outgoing_memory() noexcept {
        used_outgoing = true;

        if(!request_memory) {
            request_memory = thread_context->incoming_allocator->allocate_incoming();

            RetrievalHeader *request_header = reinterpret_cast<RetrievalHeader *>(request_memory->get_buffer());
            request_header->done = false;

            Synchronizer synchronizer{1};

            if(self_memory == nullptr) {
                self_memory = new RDMAMemory(context, this, sizeof(RDMAMessenger<NUMBER_CHUNKS_MAXIMUM>));
            } 

            dsys::call_service(destination_thread_id, [source_thread_id = thread_id, destination_thread_id = this->destination_thread_id, request_header_locator = RDMAMemoryLocator{request_memory}, source_memory_locator = RDMAMemoryLocator{self_memory}] {
                RDMAMemory *response = thread_context->fine_outgoing_allocator->allocate_outgoing(sizeof(RetrievalHeader));

                RetrievalHeader *response_object = reinterpret_cast<RetrievalHeader *>(response->get_buffer());

                for(int i = 0; i < NUMBER_CHUNKS_ALLOCATION; i++) {
                    RDMAMemory *memory = global_allocators[affinity_handler.get_numa_zone(get_thread_rank(destination_thread_id))]->allocate_chunk();
                    std::memset(memory->get_buffer(), 0, memory->get_size());

                    Header *memory_header = reinterpret_cast<Header *>(memory->get_buffer());

                    new(memory_header) Header();
                    memory_header->setup.producer_id = source_thread_id;
                    memory_header->setup.producer_locator = source_memory_locator;

                    incoming_memory_queues[get_thread_rank(destination_thread_id)].push(memory);

                    response_object->memory_locators[i] = RDMAMemoryLocator(memory);
                }

                response_object->done = true;

                get_transmitter(source_thread_id)->rdma_write(response, 0, sizeof(RetrievalHeader), &request_header_locator);
            }, &synchronizer);

            synchronizer.spin_nonzero_operations_left();

            return false;
        }

        RetrievalHeader *request_header = reinterpret_cast<RetrievalHeader *>(request_memory->get_buffer());

        if(!request_header->done) {
#ifdef DEBUG
            dsys::print_mutex.lock();
            cout << "Waiting for external memory" << endl;
            dsys::print_mutex.unlock();
#endif /* DEBUG */
            return false;
        }

        for(int i = 0; i < NUMBER_CHUNKS_ALLOCATION; i++) {
            outgoing_chunks.emplace_front(request_header->memory_locators[i]);

            last_flushed_offsets[request_header->memory_locators[i].buffer] = 0;
        }

        // Make incoming memory reusable
        request_memory->ready = true;

        // Reset memory request
        request_memory = nullptr;

        return true;
    }

    inline void setup_chunk() noexcept {
        original_remote_memory_locator = adjusted_remote_memory_locator = outgoing_chunks.front();

        adjusted_remote_memory_locator += sizeof(Header);
    }

    inline void rotate_chunk() noexcept {
        outgoing_position_total += outgoing_position;
        outgoing_position = 0;

        // Rotate on the sender
        outgoing_chunks.emplace_back(outgoing_chunks.front());
        outgoing_chunks.pop_front();

        adjusted_remote_memory_locator.buffer = nullptr;
    }

    inline bool obtain_memory() noexcept {
        if(adjusted_remote_memory_locator.buffer == nullptr) {
            if(!retrieve_outgoing_memory()) {
                return false;
            }

            setup_chunk();
            return true;
        }

        uint64_t outgoing_chunks_size = outgoing_chunks.size();

        if(!is_fully_consumed(outgoing_chunks[std::min(1UL, outgoing_chunks_size - 1)])) {
            if(outgoing_chunks_size < NUMBER_CHUNKS_MAXIMUM) {
                // Starts memory retrieval process
                retrieve_outgoing_memory();

                // More memory allocation: rotate, fetch, do not shutdown
                flush_internal(true, true, false);

                rotate_chunk();
            }

            return false;
        }
        else {
            // No memory allocation: rotate, do not fetch, do not shutdown
            flush_internal(true, false, false);

            rotate_chunk();
            setup_chunk();

            return true;
        }
    }

    ///////////////////////
    // Closing functions //
    ///////////////////////

    inline void shutdown(Synchronizer *synchronizer = nullptr) noexcept {
        if(get_outgoing_shutdown()) {
            if(synchronizer) {
                synchronizer->decrease();
            }

            return;
        }

        flush_internal(false, false, true, synchronizer);
    }

    /////////////////////
    // Call processing //
    /////////////////////

    inline uint64_t process_calls() noexcept {
        if(processing_calls) {
            return 0;
        }

        if(!used_incoming) {
            retrieve_incoming_memory();

            if(used_incoming) {
                incoming_chunks_front = incoming_chunks.front();
            }
        }

        if(get_incoming_shutdown()) {
            return 0;
        }

        processing_calls = true;

        uint64_t total = 0;

        while(true) {
            RDMAMemory *chunk = incoming_chunks_front;
            Header *chunk_header = reinterpret_cast<Header *>(chunk->get_buffer());

            // We have to snapshot the flags/producer because two flushes
            // can happen very close to each other
            Header::Producer producer_information = chunk_header->producer;

            uint64_t consumed_offset = 0;

            total += process_multiple_calls_flagged(chunk->get_buffer(sizeof(Header) + incoming_position), consumed_offset);

            incoming_position += consumed_offset;

            chunk_header->consumer.offset = incoming_position_total + incoming_position;

// #ifdef DEBUG
            // assert(producer_information.offset <= chunk_header->consumer.offset);
// #endif /* DEBUG */

            if(producer_information.shutdown && producer_information.offset == chunk_header->consumer.offset) {
                // Shutdown message came before all data have been processed
                if(chunk_header->consumer.offset != producer_information.offset) {
                    break;
                }

                chunk_header->producer.shutdown = false;
                incoming_shutdown = true;

                for(RDMAMemory *memory: incoming_chunks) {
                    delete memory;
                }

                incoming_chunks.clear();

                if(!get_outgoing_shutdown()) {
                    update_consumed_offset(chunk_header->consumer.offset);
                }
                
                break;
            }

            if(producer_information.rotate && producer_information.offset == chunk_header->consumer.offset) {
                // Rotate message came before all data have been processed
                if(chunk_header->consumer.offset != producer_information.offset) {
                    break;
                }

                if(producer_information.fetch) {
                    enqueue_incoming_memory();

                    if(thread_context->incoming_memory_map[destination_thread_id].size() < NUMBER_CHUNKS_ALLOCATION) {
    #ifdef DEBUG
                        cerr << "Rotation request received before service thread allocated memory" << endl;
    #endif /* DEBUG */
                        break;
                    }
                }

                std::memset(chunk->get_buffer(sizeof(Header)), 0, chunk->get_size() - sizeof(Header));
                chunk_header->producer.rotate = false;

                incoming_position_total += incoming_position;
                incoming_position = 0;

                // Rotate the incoming queue
                incoming_chunks.emplace_back(incoming_chunks.front());
                incoming_chunks.pop_front();

                if(producer_information.fetch) {
                    chunk_header->producer.fetch = false;

                    retrieve_incoming_memory();
                }

                incoming_chunks_front = incoming_chunks.front();

                if(!get_outgoing_shutdown()) {
                    update_consumed_offset(chunk_header->consumer.offset);
                }
            }
            else {
                break;
            }
        }

        processing_calls = false;
        return total;
    }

    ///////////////////////////
    // Transparent functions //
    ///////////////////////////

    template<typename F>
    inline bool call(int destination_thread_id, F &&function, Synchronizer *synchronizer = nullptr) noexcept {
#ifdef DIRECT_LOCAL_CALLS
        int destination_process_rank = (destination_thread_id / dsys::number_threads_process);

        if(destination_process_rank == process_rank) {
            function();

            if(synchronizer) {
                synchronizer->decrease();
            }

            return true;
        }
#endif /* DIRECT_LOCAL_CALLS */

        using G = typename std::remove_reference<F>::type;

        if(adjusted_remote_memory_locator.buffer == nullptr) {
            if(!obtain_memory()) {
                return false;
            }
        }

        if(sizeof(FlaggedFunctionWrapper<G>) + outgoing_position > GLOBAL_ALLOCATOR_CHUNK_SIZE - sizeof(Header)) {
            if(sizeof(FlaggedFunctionWrapper<G>) > GLOBAL_ALLOCATOR_CHUNK_SIZE - sizeof(Header)) {
                cerr << "Error: call does not fit RDMAMessenger's buffer size" << endl;
                exit(EXIT_FAILURE);
            }

            if(!obtain_memory()) {
                return false;
            }
        }

        uint64_t flags = 0;
        RDMAMemory *memory = get_flagged_function_wrapper(function, flags);

        get_transmitter(destination_thread_id)->rdma_write(memory, 0, sizeof(FlaggedFunctionWrapper<G>), &adjusted_remote_memory_locator, flags, nullptr, synchronizer);
        
        outgoing_position += sizeof(FlaggedFunctionWrapper<G>);
        adjusted_remote_memory_locator += sizeof(FlaggedFunctionWrapper<G>);

        return true;
    }

    template<typename F>
    inline bool call_buffer_unchecked(int destination_thread_id, F &&function, RDMAMemory *local_memory, uint64_t offset, uint64_t size, Synchronizer *synchronizer = nullptr) noexcept {
        using G = typename std::remove_reference<F>::type;

        if(sizeof(FlaggedFunctionWrapperBuffer<G>) + size > INT32_MAX) {
            uint64_t flags = 0;
            RDMAMemory *memory = get_flagged_function_wrapper_buffer(function, size, flags);

            FlaggedFunctionWrapperBuffer<G> *function_wrapper = reinterpret_cast<FlaggedFunctionWrapperBuffer<G> *>(memory->get_buffer());

            uint8_t *last_byte_pointer = reinterpret_cast<uint8_t *>(local_memory->get_buffer(offset + size - 1));
            uint64_t last_byte_offset = size - 1;

            while(last_byte_offset > 0 && *last_byte_pointer == 0) {
                last_byte_pointer--;
                last_byte_offset--;
            }

            if(*last_byte_pointer != 0) {
                function_wrapper->buffer_checkmark = (((uint64_t) (*last_byte_pointer)) << 56) | (last_byte_offset & 0xffffffffffffff);
            }
            else {
                function_wrapper->buffer_checkmark = UINT64_MAX;
            }

            get_transmitter(destination_thread_id)->rdma_write(memory, 0, sizeof(FlaggedFunctionWrapperBuffer<G>), &adjusted_remote_memory_locator, flags, nullptr, nullptr);

            outgoing_position += sizeof(FlaggedFunctionWrapperBuffer<G>);
            adjusted_remote_memory_locator += sizeof(FlaggedFunctionWrapperBuffer<G>);

            if(*last_byte_pointer != 0) {
                get_transmitter(destination_thread_id)->rdma_write_batches(local_memory, offset, last_byte_offset + 1, &adjusted_remote_memory_locator, 0, nullptr, synchronizer);
            }

            outgoing_position += size;
            adjusted_remote_memory_locator += size;

            return true;
        }

        // We have to allocate directly because we are sending a scattered request
        RDMAMemory *memory = thread_context->fine_outgoing_allocator->allocate_outgoing(sizeof(FlaggedFunctionWrapperBuffer<G>));

        FlaggedFunctionWrapperBuffer<G> *function_wrapper = new(memory->get_buffer()) FlaggedFunctionWrapperBuffer<G>(function, size);

        RDMAMemory* memories[2];
        uint64_t offsets[2];
        uint32_t sizes[2];

        memories[0] = memory;
        memories[1] = local_memory;

        offsets[0] = 0;
        offsets[1] = offset;

        uint8_t *last_byte_pointer = reinterpret_cast<uint8_t *>(local_memory->get_buffer(offset + size - 1));
        uint64_t last_byte_offset = size - 1;

        while(last_byte_offset > 0 && *last_byte_pointer == 0) {
            last_byte_pointer--;
            last_byte_offset--;
        }

        if(*last_byte_pointer != 0) {
            function_wrapper->buffer_checkmark = (((uint64_t) (*last_byte_pointer)) << 56) | (last_byte_offset & 0xffffffffffffff);
        }
        else {
            function_wrapper->buffer_checkmark = UINT64_MAX;
        }

        sizes[0] = sizeof(FlaggedFunctionWrapperBuffer<G>);
        sizes[1] = (uint32_t) last_byte_offset + 1;

        get_transmitter(destination_thread_id)->rdma_write_multiple(memories, offsets, sizes, 2, &adjusted_remote_memory_locator, 0, nullptr, synchronizer);

        outgoing_position += (sizeof(FlaggedFunctionWrapperBuffer<G>) + size);
        adjusted_remote_memory_locator += (sizeof(FlaggedFunctionWrapperBuffer<G>) + size);

        return true;
    }

    template<typename F>
    inline bool call_buffer(int destination_thread_id, F &&function, RDMAMemory *local_memory, uint64_t offset, uint64_t size, Synchronizer *synchronizer = nullptr) noexcept {
#ifdef DIRECT_LOCAL_CALLS
        int destination_process_rank = (destination_thread_id / dsys::number_threads_process);

        if(destination_process_rank == process_rank) {
            function(buffer, size);

            if(synchronizer) {
                synchronizer->decrease();
            }

            return true;
        }
#endif /* DIRECT_LOCAL_CALLS */

        using G = typename std::remove_reference<F>::type;

        if(adjusted_remote_memory_locator.buffer == nullptr) {
            if(!obtain_memory()) {
                return false;
            }
        }

        if(sizeof(FlaggedFunctionWrapperBuffer<G>) + size + outgoing_position > GLOBAL_ALLOCATOR_CHUNK_SIZE - sizeof(Header)) {
            if(sizeof(FlaggedFunctionWrapperBuffer<G>) + size > GLOBAL_ALLOCATOR_CHUNK_SIZE - sizeof(Header)) {
                cerr << "Error: call does not fit RDMAMessenger's buffer size" << endl;
                exit(EXIT_FAILURE);
            }

            if(!obtain_memory()) {
                return false;
            }
        }

        return call_buffer_unchecked(destination_thread_id, std::forward<F>(function), local_memory, offset, size, synchronizer);
    }

    template<typename F>
    inline bool call_buffer(int destination_thread_id, F &&function, void *data, uint64_t size, Synchronizer *synchronizer = nullptr) noexcept {
#ifdef DIRECT_LOCAL_CALLS
        int destination_process_rank = (destination_thread_id / dsys::number_threads_process);

        if(destination_process_rank == process_rank) {
            function(buffer, size);

            if(synchronizer) {
                synchronizer->decrease();
            }

            return true;
        }
#endif /* DIRECT_LOCAL_CALLS */

        using G = typename std::remove_reference<F>::type;

        if(adjusted_remote_memory_locator.buffer == nullptr) {
            if(!obtain_memory()) {
                return false;
            }
        }

        if(sizeof(FlaggedFunctionWrapperBuffer<G>) + size + outgoing_position > GLOBAL_ALLOCATOR_CHUNK_SIZE - sizeof(Header)) {
            if(sizeof(FlaggedFunctionWrapperBuffer<G>) + size > GLOBAL_ALLOCATOR_CHUNK_SIZE - sizeof(Header)) {
                cerr << "Error: call does not fit RDMAMessenger's buffer size" << endl;
                exit(EXIT_FAILURE);
            }

            if(!obtain_memory()) {
                return false;
            }
        }

        if(sizeof(FlaggedFunctionWrapperBuffer<G>) + size + outgoing_position <= GLOBAL_ALLOCATOR_CHUNK_SIZE - sizeof(Header)) {
            RDMAMemory *memory = thread_context->fine_outgoing_allocator->allocate_outgoing(size);
            memcpy(memory->get_buffer(), data, size);
            
            return call_buffer_unchecked(destination_thread_id, std::forward<F>(function), memory, 0, size, synchronizer);
        }

        cerr << "Error: call does not fit RDMAMessenger's buffer size" << endl;
        exit(EXIT_FAILURE);
    }
};

template<typename ChildMessenger = RDMAMessenger<>>
class RDMAMessengerGlobal {
public:
    using ChildMessengerType = ChildMessenger;

protected:
    vector<ChildMessenger *> messengers;

public:
    template<typename... Args>
    RDMAMessengerGlobal(Args &&... args) {
        messengers.resize(dsys::number_threads);

        for(int i = 0; i < dsys::number_threads; i++) {
            messengers[i] = new ChildMessenger(i, std::forward<Args>(args)...);
        }
    }

    virtual ~RDMAMessengerGlobal() {
        for(int i = 0; i < dsys::number_threads; i++) {
            delete messengers[i];
        }
    }

    ////////////////////////
    // Accessor functions //
    ////////////////////////

    inline ChildMessenger *get_messenger(int destination_thread_id) const noexcept {
        return messengers[destination_thread_id];
    }

    inline void set_messenger(int destination_thread_id, ChildMessenger *new_messenger) noexcept {
        messengers[destination_thread_id] = new_messenger;
    }

    //////////////////////////////////
    // Flush and shutdown functions //
    //////////////////////////////////

    inline void shutdown(int destination_thread_id, Synchronizer *synchronizer = nullptr) noexcept {
        messengers[destination_thread_id]->shutdown(synchronizer);
    }

    inline void shutdown_all(Synchronizer *synchronizer = nullptr) noexcept {
        for(int destination_thread_id = 0; destination_thread_id < dsys::number_threads; destination_thread_id++) {
            shutdown(destination_thread_id, synchronizer);
        }
    }

    inline bool get_incoming_shutdown(int destination_thread_id) noexcept {
        return messengers[destination_thread_id]->get_incoming_shutdown();
    }

    inline bool get_incoming_shutdown_all() noexcept {
        for(int destination_thread_id = 0; destination_thread_id < dsys::number_threads; destination_thread_id++) {
            if(!messengers[destination_thread_id]->get_incoming_shutdown()) {
                return false;
            }
        }

        return true;
    }

    ///////////////////////////
    // Transparent functions //
    ///////////////////////////

    template<typename F>
    inline bool call(int destination_thread_id, F &&function, Synchronizer *synchronizer = nullptr) noexcept {
        return messengers[destination_thread_id]->call(destination_thread_id, function, synchronizer);
    }

    template<typename F>
    inline bool call_buffer(int destination_thread_id, F &&function, RDMAMemory *local_memory, uint64_t offset, uint64_t size, Synchronizer *synchronizer = nullptr) noexcept {
        return messengers[destination_thread_id]->call_buffer(destination_thread_id, function, local_memory, offset, size, synchronizer);
    }

    template<typename F>
    inline bool call_buffer(int destination_thread_id, F &&function, void *data, uint64_t size, Synchronizer *synchronizer = nullptr) noexcept {
        return messengers[destination_thread_id]->call_buffer(destination_thread_id, function, data, size, synchronizer);
    }

    //////////////////////////
    // Processing functions //
    //////////////////////////

    inline uint64_t process_calls(int destination_thread_id) noexcept {
        return messengers[destination_thread_id]->process_calls();
    }

    inline uint64_t process_calls_all() noexcept {
        uint64_t total = 0;

        for(int destination_thread_id = 0; destination_thread_id < dsys::number_threads; destination_thread_id++) {
            total += messengers[destination_thread_id]->process_calls();
        }

        return total;
    }
};

} // namespace dsys

#endif /* RDMA_MESSENGER_H */
