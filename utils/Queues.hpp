/*
 * Copyright (c) 2024, Hammurabi Mendes. All rights reserved.

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

#ifndef QUEUE_HPP
#define QUEUE_HPP

#if defined(LIBNUMA) && defined(__linux__)
#include <numa.h>
#endif /* LIBNUMA and __linux__ */

#include <array>
#include <atomic>

using std::atomic;

template<class T>
class alignas(64) FastQueue {
protected:
    static constexpr int BUFFER_NUMBER_OBJECTS = 1024 * 1024;
    static constexpr int BUFFER_SIZE = BUFFER_NUMBER_OBJECTS * sizeof(T); // sizeof(T) MiB objects (must be multiple of sizeof(T))

    std::vector<T *> buffers;

    T *insert_buffer;
    uint64_t insert_buffer_number;
    uint64_t insert_buffer_watermark;

    T *remove_buffer;
    uint64_t remove_buffer_number;
    uint64_t remove_buffer_watermark;

public:
    FastQueue(): insert_buffer{nullptr}, insert_buffer_number{UINT64_MAX}, insert_buffer_watermark{0} {
        add_buffer();

        remove_buffer = insert_buffer;
        remove_buffer_number = insert_buffer_number;
        remove_buffer_watermark = 0;
    }

    ~FastQueue() {
        for(T *buffer: buffers) {
            if(buffer != nullptr) {
#if defined(LIBNUMA) && defined(__linux__)
                numa_free(buffer, BUFFER_SIZE);
#else
                free(buffer);
#endif /* LIBNUMA and __linux__ */
            }
        }
    }

    inline bool empty() const noexcept {
        return (insert_buffer_watermark == remove_buffer_watermark);
    }

    inline uint64_t size() const noexcept {
        return insert_buffer_watermark - remove_buffer_watermark;
    }

    inline T &front() const noexcept {
        return *(remove_buffer + (remove_buffer_watermark % BUFFER_NUMBER_OBJECTS));
    }

    inline void push(const T &element) noexcept {
        *(insert_buffer + (insert_buffer_watermark % BUFFER_NUMBER_OBJECTS)) = element;

        insert_buffer_watermark++;

        if(insert_buffer_watermark % BUFFER_NUMBER_OBJECTS == 0) {
            add_buffer();
        }
    }

    inline void pop() noexcept {
        remove_buffer_watermark++;

        if(remove_buffer_watermark % BUFFER_NUMBER_OBJECTS == 0) {
            remove_buffer_number++;
            remove_buffer = buffers[remove_buffer_number];

            if(remove_buffer_number >= 2) {
#if defined(LIBNUMA) && defined(__linux__)
                numa_free(buffers[remove_buffer_number - 2], BUFFER_SIZE);
#else
                free(buffers[remove_buffer_number - 2]);
#endif /* LIBNUMA and __linux__ */

                buffers[remove_buffer_number - 2] = nullptr;
            }
        }
    }

    inline T &pop_remove() noexcept {
        T &result = front();

        pop();

        return result;
    }

protected:
    inline void add_buffer() noexcept {
#if defined(LIBNUMA) && defined(__linux__)
        T *memory = reinterpret_cast<T *>(numa_alloc_local(BUFFER_SIZE));
#else
        T *memory = reinterpret_cast<T *>(malloc(BUFFER_SIZE));
#endif /* LIBNUMA and __linux__ */

        buffers.emplace_back(memory);

        insert_buffer_number++;
        insert_buffer = buffers[insert_buffer_number];
    }
};

template<class T>
class alignas(64) FastQueuePC {
protected:
    static constexpr int BUFFER_NUMBER_OBJECTS = 1024 * 1024;
    static constexpr int BUFFER_SIZE = BUFFER_NUMBER_OBJECTS * sizeof(T); // sizeof(T) MiB objects (must be multiple of sizeof(T))

    // Holds 1M * 1M pointers at most (sizeof(T) PB worth of objects)
    std::array<T *, 1024 * 1024> buffers;

    alignas(64) T *insert_buffer;
    uint64_t insert_buffer_number;
    atomic<uint64_t> insert_buffer_watermark;

    alignas(64) T *remove_buffer;
    uint64_t remove_buffer_number;
    uint64_t remove_buffer_watermark;

public:
    FastQueuePC(): insert_buffer{nullptr}, insert_buffer_number{UINT64_MAX}, insert_buffer_watermark{0} {
        add_buffer();

        remove_buffer = insert_buffer;
        remove_buffer_number = insert_buffer_number;
        remove_buffer_watermark = 0;

        insert_buffer_watermark.store(0, std::memory_order_release);
    }

    ~FastQueuePC() {
        for(uint64_t i = 0; i <= insert_buffer_number; i++) {
            T *buffer = buffers[i];

            if(buffer != nullptr) {
#if defined(LIBNUMA) && defined(__linux__)
                numa_free(buffer, BUFFER_SIZE);
#else
                free(buffer);
#endif /* LIBNUMA and __linux__ */
            }
        }
    }

    inline bool empty() const noexcept {
        return (insert_buffer_watermark.load(std::memory_order_acquire) == remove_buffer_watermark);
    }

    inline uint64_t size() const noexcept {
        return insert_buffer_watermark.load(std::memory_order_acquire) - remove_buffer_watermark;
    }

    inline T &front() const noexcept {
        return *(remove_buffer + (remove_buffer_watermark % BUFFER_NUMBER_OBJECTS));
    }

    inline void push(const T &element) noexcept {
        uint64_t insert_buffer_watermark_snapshot = insert_buffer_watermark.load(std::memory_order_acquire);

        *(insert_buffer + (insert_buffer_watermark_snapshot % BUFFER_NUMBER_OBJECTS)) = element;

        if((insert_buffer_watermark_snapshot + 1) % BUFFER_NUMBER_OBJECTS == 0) {
            add_buffer();
        }

        // Essential that this operation is in the end
        insert_buffer_watermark.fetch_add(1, std::memory_order_release);
    }

    inline T *lease(uint64_t &number, uint64_t max_number = BUFFER_NUMBER_OBJECTS) const noexcept {
        uint64_t insert_buffer_watermark_snapshot = insert_buffer_watermark.load(std::memory_order_acquire);

        number = BUFFER_NUMBER_OBJECTS - (insert_buffer_watermark_snapshot % BUFFER_NUMBER_OBJECTS);

        if(number > max_number) {
            number = max_number;
        }
        
        return reinterpret_cast<T *>(insert_buffer + (insert_buffer_watermark_snapshot % BUFFER_NUMBER_OBJECTS));
    }

    inline void push_lease(uint64_t number) noexcept {
        if((insert_buffer_watermark.load(std::memory_order_acquire) + number) % BUFFER_NUMBER_OBJECTS == 0) {
            add_buffer();
        }

        // Essential that this operation is in the end
        insert_buffer_watermark.fetch_add(number, std::memory_order_release);
    }

    inline void pop() noexcept {
        remove_buffer_watermark++;

        if(remove_buffer_watermark % BUFFER_NUMBER_OBJECTS == 0) {
            remove_buffer_number++;
            remove_buffer = buffers[remove_buffer_number];

            if(remove_buffer_number >= 2) {
#if defined(LIBNUMA) && defined(__linux__)
                numa_free(buffers[remove_buffer_number - 2], BUFFER_SIZE);
#else
                free(buffers[remove_buffer_number - 2]);
#endif /* LIBNUMA and __linux__ */

                buffers[remove_buffer_number - 2] = nullptr;
            }
        }
    }

    inline T &pop_remove() noexcept {
        T &result = front();

        pop();

        return result;
    }

    inline T *receive(uint64_t &number, uint64_t max_number = BUFFER_NUMBER_OBJECTS) noexcept {
        max_number = std::min(max_number, size());

        number = BUFFER_NUMBER_OBJECTS - (remove_buffer_watermark % BUFFER_NUMBER_OBJECTS);

        if(number > max_number) {
            number = max_number;
        }

        return reinterpret_cast<T *>(remove_buffer + (remove_buffer_watermark % BUFFER_NUMBER_OBJECTS));
    }

    inline void pop_receive(uint64_t size) noexcept {
        remove_buffer_watermark += size;

        if(remove_buffer_watermark % BUFFER_NUMBER_OBJECTS == 0) {
            remove_buffer_number++;
            remove_buffer = buffers[remove_buffer_number];

            if(remove_buffer_number >= 2) {
#if defined(LIBNUMA) && defined(__linux__)
                numa_free(buffers[remove_buffer_number - 2], BUFFER_SIZE);
#else
                free(buffers[remove_buffer_number - 2]);
#endif /* LIBNUMA and __linux__ */

                buffers[remove_buffer_number - 2] = nullptr;
            }
        }
    }

    inline void push_from(FastQueuePC<T> &other, uint64_t size) noexcept {
        uint64_t insert_buffer_watermark_snapshot = insert_buffer_watermark.load(std::memory_order_acquire);

        for(uint64_t i = 0; i < size; i++) {
            *(insert_buffer + ((insert_buffer_watermark_snapshot + i) % BUFFER_NUMBER_OBJECTS)) = other.pop_remove();

            if((insert_buffer_watermark_snapshot + i + 1) % BUFFER_NUMBER_OBJECTS == 0) {
                add_buffer();
            }
        }

        // Essential that this operation is in the end
        insert_buffer_watermark.fetch_add(size, std::memory_order_release);
    }

protected:
    inline void add_buffer() noexcept {
#if defined(LIBNUMA) && defined(__linux__)
        T *memory = reinterpret_cast<T *>(numa_alloc_local(BUFFER_SIZE));
#else
        T *memory = reinterpret_cast<T *>(malloc(BUFFER_SIZE));
#endif /* LIBNUMA and __linux__ */

        buffers[insert_buffer_number + 1] = memory;

        insert_buffer_number++;
        insert_buffer = buffers[insert_buffer_number];
    }
};

template<class T>
struct PointerTimestamp {
    T *pointer;
    uint64_t timestamp;
};

template<class T>
struct PointerSize {
    T *pointer;
    uint64_t size;
};

template<class T>
class alignas(64) FastQueueBare: public FastQueue<T *> {};

template<class T>
class alignas(64) FastQueueTimestamp: public FastQueue<PointerTimestamp<T>> {
public:
    using FastQueue<PointerTimestamp<T>>::BUFFER_NUMBER_OBJECTS;

    using FastQueue<PointerTimestamp<T>>::insert_buffer;
    using FastQueue<PointerTimestamp<T>>::insert_buffer_watermark;

    using FastQueue<PointerTimestamp<T>>::add_buffer;

    FastQueueTimestamp(): FastQueue<PointerTimestamp<T>>() {
    }

    inline void push(T *const pointer, uint64_t timestamp) noexcept {
        PointerTimestamp<T> *entry = insert_buffer + (insert_buffer_watermark % BUFFER_NUMBER_OBJECTS);

        entry->pointer = pointer;
        entry->timestamp = timestamp;

        insert_buffer_watermark++;

        if(insert_buffer_watermark % BUFFER_NUMBER_OBJECTS == 0) {
            add_buffer();
        }
    }
};

template<class T>
class alignas(64) FastQueueSize: public FastQueue<PointerSize<T>> {
public:
    using FastQueue<PointerSize<T>>::BUFFER_NUMBER_OBJECTS;

    using FastQueue<PointerSize<T>>::insert_buffer;
    using FastQueue<PointerSize<T>>::insert_buffer_watermark;

    using FastQueue<PointerSize<T>>::add_buffer;

    FastQueueSize(): FastQueue<PointerSize<T>>() {
    }

    inline void push(T *const pointer, uint64_t size) noexcept {
        PointerSize<T> *entry = insert_buffer + (insert_buffer_watermark % BUFFER_NUMBER_OBJECTS);

        entry->pointer = pointer;
        entry->size = size;

        insert_buffer_watermark++;

        if(insert_buffer_watermark % BUFFER_NUMBER_OBJECTS == 0) {
            add_buffer();
        }
    }
};

template<class T>
class alignas(64) FastQueueBarePC: public FastQueuePC<T *> {};

template<class T>
class alignas(64) FastQueueTimestampPC: public FastQueuePC<PointerTimestamp<T>> {
public:
    using FastQueuePC<PointerTimestamp<T>>::BUFFER_NUMBER_OBJECTS;

    using FastQueuePC<PointerTimestamp<T>>::insert_buffer;
    using FastQueuePC<PointerTimestamp<T>>::insert_buffer_watermark;

    using FastQueuePC<PointerTimestamp<T>>::add_buffer;

    FastQueueTimestampPC(): FastQueue<PointerTimestamp<T>>() {
    }

    inline void push(T *const pointer, uint64_t timestamp) noexcept {
        PointerTimestamp<T> *entry = insert_buffer + (insert_buffer_watermark % BUFFER_NUMBER_OBJECTS);

        entry->pointer = pointer;
        entry->timestamp = timestamp;

        insert_buffer_watermark++;

        if(insert_buffer_watermark % BUFFER_NUMBER_OBJECTS == 0) {
            add_buffer();
        }
    }
};

template<class T>
class alignas(64) FastQueueSizePC: public FastQueuePC<PointerSize<T>> {
public:
    using FastQueuePC<PointerSize<T>>::BUFFER_NUMBER_OBJECTS;

    using FastQueuePC<PointerSize<T>>::insert_buffer;
    using FastQueuePC<PointerSize<T>>::insert_buffer_watermark;

    using FastQueuePC<PointerSize<T>>::add_buffer;

    FastQueueSizePC(): FastQueuePC<PointerSize<T>>() {
    }

    inline void push(T *const pointer, uint64_t size) noexcept {
        PointerSize<T> *entry = insert_buffer + (insert_buffer_watermark % BUFFER_NUMBER_OBJECTS);

        entry->pointer = pointer;
        entry->size = size;

        insert_buffer_watermark++;

        if(insert_buffer_watermark % BUFFER_NUMBER_OBJECTS == 0) {
            add_buffer();
        }
    }
};

#endif /* QUEUE_HPP */
