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

#ifndef MEMORY_ALLOCATION_HPP
#define MEMORY_ALLOCATION_HPP

#include "ibutils.hpp"

#include <stdlib.h>
#include <cstdint>

#include <vector>
#include <mutex>

#ifdef THREAD_HANDLER_H
using seriema::get_transmitter;
#else
extern IBTransmitter<QP_GLOBAL_FLUSH_INTERVAL, QP_THREAD_FLUSH_INTERVAL> *get_transmitter(int destination_thread_id) noexcept;
#endif /* THREAD_HANDLER_H */

using std::vector;

using std::mutex;
using std::lock_guard;

#if defined(LIBNUMA) && defined(__linux__)
#include <numa.h>
#endif /* LIBNUMA and __linux__ */

template<uint64_t CHUNK_SIZE = 4096 * 512, uint64_t SUPERCHUNK_SIZE = 4096 * 512 * 512>
class RDMAAllocator {
    constexpr static int MAX_BUFFERS = 65536;

private:
    IBContext *context;

    RDMAMemory *buffers[MAX_BUFFERS];

    atomic<uint64_t> current_buffer_watermark;

    mutex lock;

public:
    RDMAAllocator(IBContext *context): context{context}, current_buffer_watermark{0} {
        for(int i = 0; i < MAX_BUFFERS; i++) {
            buffers[i] = nullptr;
        }

        // Eager allocation
        add_buffer(0);
    }

    ~RDMAAllocator() {
        cleanup();
    }

    inline void cleanup() noexcept {
        uint64_t buffer_number = current_buffer_watermark / SUPERCHUNK_SIZE;

        for(int i = 0; i <= buffer_number; i++) {
            deallocate_superchunk(buffers[i]);
        }
    }

    inline RDMAMemory *allocate_chunk() noexcept {
        // This read synchronizes with the write of buffers[] in add_buffer()
        uint64_t snapshot = current_buffer_watermark.fetch_add(CHUNK_SIZE);

        uint64_t buffer_number = snapshot / SUPERCHUNK_SIZE;
        uint64_t buffer_offset = snapshot % SUPERCHUNK_SIZE;

        if(buffers[buffer_number] == nullptr) {
            add_buffer(buffer_number);
        }

        RDMAMemory *memory = new RDMAMemory(buffers[buffer_number], CHUNK_SIZE, buffer_offset);

        memory->operation_timestamp = UINT64_MAX;
        return memory;
    }

    inline RDMAMemory *allocate_chunk(uint64_t number_chunks) noexcept {
        while(true) {
            // This read synchronizes with the write of buffers[] in add_buffer()
            uint64_t snapshot = current_buffer_watermark.fetch_add(number_chunks * CHUNK_SIZE);

            uint64_t buffer_number = snapshot / SUPERCHUNK_SIZE;
            uint64_t buffer_offset = snapshot % SUPERCHUNK_SIZE;

            if(buffers[buffer_number] == nullptr) {
                add_buffer(buffer_number);
            }

            // Linear allocation has failed to be contiguous; try again
            if((snapshot + (number_chunks * CHUNK_SIZE)) % SUPERCHUNK_SIZE < (number_chunks * CHUNK_SIZE)) {
                continue;
            }

            RDMAMemory *memory = new RDMAMemory(buffers[buffer_number], number_chunks * CHUNK_SIZE, buffer_offset);

            memory->operation_timestamp = UINT64_MAX;
            return memory;
        }
    }

    inline RDMAMemory *allocate_arbitrary(uint64_t size) noexcept {
#if defined(LIBNUMA) && defined(__linux__)
        void *buffer = numa_alloc_local(size);

        RDMAMemory *memory = new RDMAMemory(context, buffer, size);
#else
        RDMAMemory *memory = new RDMAMemory(context, size);
#endif /* LIBNUMA and __linux__ */

        memory->operation_timestamp = UINT64_MAX;
        return memory;
    }

    inline void deallocate_arbitrary(RDMAMemory *memory) noexcept {
#if defined(LIBNUMA) && defined(__linux__)
        numa_free(memory->get_buffer(), memory->get_size());
#endif /* LIBNUMA and __linux__ */
        delete memory;
    }

    inline RDMAMemory *allocate_superchunk() noexcept {
#if defined(LIBNUMA) && defined(__linux__)
        void *buffer = numa_alloc_local(SUPERCHUNK_SIZE);

        RDMAMemory *memory = new RDMAMemory(context, buffer, SUPERCHUNK_SIZE);
#else
        RDMAMemory *memory = new RDMAMemory(context, SUPERCHUNK_SIZE);
#endif /* LIBNUMA and __linux__ */

        memory->operation_timestamp = UINT64_MAX;
        return memory;
    }

    inline void deallocate_superchunk(RDMAMemory *memory) noexcept {
#if defined(LIBNUMA) && defined(__linux__)
        numa_free(memory->get_buffer(), SUPERCHUNK_SIZE);
#endif /* LIBNUMA and __linux__ */
        delete memory;
    }

    constexpr uint64_t get_chunk_size() const noexcept {
        return CHUNK_SIZE;
    }

    constexpr uint64_t get_superchunk_size() const noexcept {
        return SUPERCHUNK_SIZE;
    }

private:
    inline void add_buffer(uint64_t buffer_number) noexcept {
        if(buffers[buffer_number] != nullptr) {
            return;
        }

        lock_guard holder(lock);

        if(buffers[buffer_number] != nullptr) {
            return;
        }

        RDMAMemory *memory = allocate_superchunk();

#ifdef DEBUG
        cout << "RDMAAllocator is allocating " << SUPERCHUNK_SIZE << " bytes" << endl;
#endif /* DEBUG */
        buffers[buffer_number] = memory;
        current_buffer_watermark += 0; // To force memory ordering
    }
};

struct BasicMemoryAllocator {
    RDMAAllocator<> *parent_allocator;

    uint64_t watermark = 0;

    vector<RDMAMemory *> prepared_memory;

    BasicMemoryAllocator(RDMAAllocator<> *parent_allocator): parent_allocator{parent_allocator} {
    }

    virtual void add_buffer() noexcept  = 0;

    inline RDMAMemory *allocate_outgoing() noexcept {
        if(prepared_memory.size() == 0) {
            add_buffer();
        }

        uint64_t slot = watermark++ % prepared_memory.size();

        uint64_t timestamp_memory_slot = prepared_memory[slot]->operation_timestamp;

        if(timestamp_memory_slot == UINT64_MAX || prepared_memory[slot]->sticky == true || timestamp_memory_slot >= get_transmitter(prepared_memory[slot]->operation_tag)->completed_timestamp.load(std::memory_order::memory_order_relaxed)) {
            slot = prepared_memory.size();
            watermark = slot + 1;

            add_buffer();
        }

        prepared_memory[slot]->operation_timestamp = UINT64_MAX;
        return prepared_memory[slot];
    }

    inline RDMAMemory *allocate_incoming() noexcept {
        if(prepared_memory.size() == 0) {
            add_buffer();
        }

        uint64_t slot = watermark++ % prepared_memory.size();

        if(prepared_memory[slot]->ready == false || prepared_memory[slot]->sticky == true) {
            slot = prepared_memory.size();
            watermark = slot + 1;

            add_buffer();
        }

        prepared_memory[slot]->operation_timestamp = UINT64_MAX;
        prepared_memory[slot]->ready = false;
        return prepared_memory[slot];
    }

    inline RDMAMemory *allocate_managed() noexcept {
        if(prepared_memory.size() == 0) {
            add_buffer();
        }

        RDMAMemory *result = prepared_memory.back();

        prepared_memory.pop_back();

        result->operation_timestamp = UINT64_MAX;
        return result;
    }

    inline void deallocate_managed(RDMAMemory *memory) noexcept {
        prepared_memory.push_back(memory);
    }

    inline bool has_free_incoming() const noexcept {
        uint64_t slot = watermark % prepared_memory.size();

        if(prepared_memory[slot]->ready == false || prepared_memory[slot]->sticky == true) {
            return false;
        }

        return true;
    }

    inline bool has_free_outgoing() const noexcept {
        uint64_t slot = watermark % prepared_memory.size();

        uint64_t timestamp_memory_slot = prepared_memory[slot]->operation_timestamp;

        if(timestamp_memory_slot == UINT64_MAX || prepared_memory[slot]->sticky == true || timestamp_memory_slot >= get_transmitter(prepared_memory[slot]->operation_tag)->completed_timestamp.load(std::memory_order::memory_order_relaxed)) {
            return false;
        }

        return true;
    }

    inline bool has_free_managed() const noexcept {
        return (prepared_memory.size() > 0);
    }
};

struct ChunkMemoryAllocator: public BasicMemoryAllocator {
    const uint64_t number_chunks;

    ChunkMemoryAllocator(RDMAAllocator<> *parent_allocator, uint64_t number_chunks = 1): number_chunks{number_chunks}, BasicMemoryAllocator{parent_allocator} {
    }

    ~ChunkMemoryAllocator() {
        for(uint64_t i = 0; i < prepared_memory.size(); i++) {
            if(prepared_memory[i] != nullptr) {
                delete prepared_memory[i];
            }
        }
    }

    // Overrides pure virtual function of BasicMemoryAllocator
    void add_buffer() noexcept {
        prepared_memory.push_back(parent_allocator->allocate_chunk(number_chunks));
        prepared_memory.back()->operation_timestamp = 0;
    }

    inline uint64_t get_buffer_size() const noexcept {
        return parent_allocator->get_chunk_size();
    }
};

struct SuperchunkMemoryAllocator: public BasicMemoryAllocator {
    SuperchunkMemoryAllocator(RDMAAllocator<> *parent_allocator): BasicMemoryAllocator{parent_allocator} {
    }

    ~SuperchunkMemoryAllocator() {
        for(uint64_t i = 0; i < prepared_memory.size(); i++) {
            if(prepared_memory[i] != nullptr) {
                delete prepared_memory[i];
            }
        }
    }

    // Overrides pure virtual function of BasicMemoryAllocator
    void add_buffer() noexcept {
        prepared_memory.push_back(parent_allocator->allocate_superchunk());
        prepared_memory.back()->operation_timestamp = 0;
    }

    inline uint64_t get_buffer_size() const noexcept {
        return parent_allocator->get_superchunk_size();
    }
};

template<class Allocator>
struct LinearAllocator {
    Allocator *allocator;
    const uint64_t allocator_buffer_size;
    
    RDMAMemory *memory;
    uint64_t offset;

    LinearAllocator(Allocator *allocator): allocator{allocator}, allocator_buffer_size{allocator->get_buffer_size()} {
        memory = nullptr;
        offset = allocator_buffer_size;
    }

    inline RDMAMemory *allocate_outgoing(uint64_t size) noexcept {
        if(offset + size > allocator_buffer_size) {
            if(memory) {
                memory->sticky = false;
            }

            memory = allocator->allocate_outgoing();
            offset = size;

            memory->sticky = true;

            memory->update_offset(0);
            return memory;
        }

        memory->update_offset(offset);
        offset += size;

        return memory;
    }

    inline uint64_t get_buffer_size() const noexcept {
        return allocator_buffer_size;
    }
};

struct MemoryAllocator: public BasicMemoryAllocator {
    const uint64_t initial_number_buffers;
    const uint64_t buffer_size;

    vector<RDMAMemory *> accumulated_memory;

    uint64_t current_multiplier = 1;
#ifdef STATS
    uint64_t number_allocations = 0;
#endif /* STATS */

    MemoryAllocator(RDMAAllocator<> *parent_allocator, uint64_t initial_number_buffers, uint64_t buffer_size): BasicMemoryAllocator{parent_allocator}, initial_number_buffers{initial_number_buffers}, buffer_size{buffer_size} {
    }

    MemoryAllocator(MemoryAllocator &other): MemoryAllocator{other.parent_allocator, other.initial_number_buffers, other.buffer_size} {
    }

    // Overrides pure virtual function of BasicMemoryAllocator
    void add_buffer() noexcept {
        uint64_t requested_size = (current_multiplier * initial_number_buffers) * buffer_size;
        uint64_t number_chunks = requested_size / parent_allocator->get_chunk_size();

        if(requested_size % parent_allocator->get_chunk_size()) {
            number_chunks++;
        }

        RDMAMemory *memory = parent_allocator->allocate_chunk(number_chunks);
        accumulated_memory.push_back(memory);

        for(int i = 0; i < (current_multiplier * initial_number_buffers); i++) {
            prepared_memory.push_back(new RDMAMemory(memory, buffer_size, buffer_size * i));
        }

#ifdef DEBUG
        seriema::print_mutex.lock();
        cout << "MemoryAllocator is allocating " << (current_multiplier * initial_number_buffers) << " new buffers (rounded up)" << endl;
        seriema::print_mutex.unlock();
#endif /* DEBUG */

#ifdef STATS
        number_allocations++;
#endif /* STATS */

        // current_multiplier *= 2;
    }

    ~MemoryAllocator() {
        for(uint64_t i = 0; i < accumulated_memory.size(); i++) {
            if(accumulated_memory[i] != nullptr) {
                delete accumulated_memory[i];
            }
        }
#ifdef STATS
        seriema::print_mutex.lock();
        cout << "Allocated " << number_allocations << " times" << endl;
        seriema::print_mutex.unlock();
#endif /* STATS */
    }

    inline uint64_t get_buffer_size() const noexcept {
        return buffer_size;
    }
};

struct FixedAllocator {
    FixedAllocator *upper_allocator;
    RDMAAllocator<> *topmost_allocator;

    const uint64_t size;
    const uint64_t upper_size;

    vector<RDMAMemory *> accumulated_memory;
    vector<uint64_t> allocation_count;

    RDMAMemory *current_memory = nullptr;
    uint64_t buffer_number = UINT64_MAX;
    uint64_t buffer_watermark = UINT64_MAX;

    vector<RDMAMemory *> chunks;

    FixedAllocator(FixedAllocator *upper_allocator, RDMAAllocator<> *topmost_allocator, uint64_t size, uint64_t upper_size): upper_allocator{upper_allocator}, topmost_allocator{topmost_allocator}, size{size}, upper_size{upper_size} {
    }

    ~FixedAllocator() {
        if(!upper_allocator) {
            for(RDMAMemory *memory: accumulated_memory) {
                topmost_allocator->deallocate_superchunk(memory);
            }
        }
        else {
            for(RDMAMemory *memory: accumulated_memory) {
                delete memory;
            }
        }
    }

    inline RDMAMemory *allocate() noexcept {
        if(chunks.size() > 0) {
            RDMAMemory *result = chunks.back();

            chunks.pop_back();

            result->operation_timestamp = UINT64_MAX;
            return result;
        }

        if(!upper_allocator) {
            return topmost_allocator->allocate_superchunk();
        }

        if(current_memory == nullptr) {
            add_buffer();
        }

        RDMAMemory *result = new RDMAMemory(current_memory, size, buffer_watermark);

        buffer_watermark += size;

        if(buffer_watermark == upper_size) {
            current_memory = nullptr;
        }

        result->operation_timestamp = UINT64_MAX;
        return result;
    }

    inline void deallocate(RDMAMemory *memory) noexcept {
        chunks.push_back(memory);
        // uint64_t *raw_data = reinterpret_cast<uint64_t *>(memory->get_buffer());

        // uint64_t buffer_number = raw_data[-1];

        // if(--allocation_count[buffer_number] == 0) {
        //     parent->deallocaate(accumulated_memory[buffer_number]);

        //     accumulated_memory[buffer_number] = nullptr;
        // }
    }

    inline uint64_t get_size() const noexcept {
        return size;
    }

protected:
    inline void add_buffer() noexcept {
        current_memory = upper_allocator->allocate();

        accumulated_memory.push_back(current_memory);
        allocation_count.push_back(0);

        buffer_number++;
        buffer_watermark = 0;
    }
};

struct LinearMemoryAllocator {
    RDMAAllocator<> *topmost_allocator;
    FixedAllocator *allocators[7];

public:
    LinearMemoryAllocator(RDMAAllocator<> *topmost_allocator): topmost_allocator{topmost_allocator} {
        allocators[6] = new FixedAllocator(nullptr, topmost_allocator, topmost_allocator->get_superchunk_size(), topmost_allocator->get_superchunk_size());

        for(int i = 5; i >= 0; i--) { 
            allocators[i] = new FixedAllocator(allocators[i + 1], topmost_allocator, allocators[i + 1]->get_size() / 8, allocators[i + 1]->get_size());
        }
    }

    ~LinearMemoryAllocator() {
        for(int i = 0; i <= 6; i++) {
            delete allocators[i];
        }
    }

    inline RDMAMemory *allocate(uint64_t size) noexcept {
        for(int i = 0; i <= 6; i++) {
            if(size <= allocators[i]->get_size()) {
                return allocators[i]->allocate();
            }
        }

        return topmost_allocator->allocate_arbitrary(size);
    }

    inline void deallocate(RDMAMemory *memory) noexcept {
        for(int i = 0; i <= 6; i++) {
            if(memory->get_size() <= allocators[i]->get_size()) {
                return allocators[i]->deallocate(memory);
            }
        }

        return topmost_allocator->deallocate_arbitrary(memory);
    }
};

#endif /* MEMORY_ALLOCATION_HPP */