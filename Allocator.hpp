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

#ifndef ALLOCATOR_HPP
#define ALLOCATOR_HPP

#include "utils/Queues.hpp"

#include <iostream>
#include <cstdlib>
#include <new>

#include <vector>

using std::cerr;
using std::endl;

using std::vector;

#if defined(LIBNUMA) && defined(__linux__)
#include <numa.h>
#endif /* LIBNUMA and __linux__ */

// template<class T>
// class MallocAllocator {
// private:
//     static constexpr int PADDING_SIZE = sizeof(T) + (sizeof(T) % 64);

//     using AlignedT = union alignas(64) {
//         T data;
//         char padding[PADDING_SIZE];
//     };

//     static constexpr int BUFFER_NUMBER_OBJECTS = 1024 * 1024;
//     static constexpr int BUFFER_SIZE = BUFFER_NUMBER_OBJECTS * sizeof(AlignedT); // sizeof(T) MiB blocks (must be multiple of sizeof(AlignedT) and 4K)

//     std::vector<AlignedT *> buffers;

//     FastQueue<T *> queue;
//     FastQueueSize<T> queue_buffers;

// public:
//     MallocAllocator() {
//     }

// #ifdef RECLAMATION
//     ~MallocAllocator() {
//         cleanup();
//     }

//     inline void cleanup() noexcept {
//         while(queue.size() > 0) {
//             free(queue.pop_remove());
//         }

//         while(queue_buffers.size() > 0) {
//             free(queue_buffers.front().pointer);

//             queue_buffers.pop();
//         }
//     }
// #endif /* RECLAMATION */

//     inline T *allocate() noexcept {
//         if(queue.size() > 0) {
//             return queue.pop_remove();
//         }

//         AlignedT *result = (AlignedT *) malloc(sizeof(AlignedT));

//         return reinterpret_cast<T *>(result);
//     }

//     inline void deallocate(T *pointer, int source_id) noexcept {
//         queue.push(pointer);
//     }

//     inline T *allocate_many(uint64_t size) noexcept {
//         if(size == 1) {
//             return allocate();
//         }

//         if(queue_buffers.size() > 0) {
//             PointerSize<T> &front = queue_buffers.front();

//             if(front.size >= size) {
//                 T *result = front.pointer;

//                 queue_buffers.pop();

//                 return reinterpret_cast<T *>(result);
//             }
//         }

//         AlignedT *result = (AlignedT *) malloc(size * sizeof(AlignedT));

//         return reinterpret_cast<T *>(result);
//     }

//     inline void deallocate_many(T *pointer, uint64_t size, int source_id) noexcept {
//         if(size == 1) {
//             return deallocate(pointer, source_id);
//         }

//         queue_buffers.push(pointer, size);
//     }
// };

template<class T>
class NUMAAllocator {
private:
    static constexpr int PADDING_SIZE = sizeof(T) + (sizeof(T) % 64);

    using AlignedT = union alignas(64) {
        T data;
        char padding[PADDING_SIZE];
    };

    static constexpr int BUFFER_NUMBER_OBJECTS = 1024;
    static constexpr uint64_t BUFFER_SIZE = BUFFER_NUMBER_OBJECTS * sizeof(AlignedT); // sizeof(AlignedT) MiB blocks (must be multiple of sizeof(T) and 4K)

    std::vector<AlignedT *> buffers;

    FastQueue<T *> queue;
    FastQueueSize<T> queue_buffers;

    AlignedT *current_buffer;
    uint64_t current_buffer_number;
    uint64_t current_buffer_watermark;

public:
    NUMAAllocator(): current_buffer{nullptr}, current_buffer_number{UINT64_MAX}, current_buffer_watermark{0} {
        add_buffer();
    }

#ifdef RECLAMATION
    ~NUMAAllocator() {
        cleanup();
    }

    inline void cleanup() noexcept {
        for(AlignedT *buffer: buffers) {
#if defined(LIBNUMA) && defined(__linux__)
            numa_free(buffer, BUFFER_SIZE);
#else
            free(buffer);
#endif /* LIBNUMA and __linux__ */
        }
    }
#endif /* RECLAMATION */

    inline T *allocate() noexcept {
        if(queue.size() > 0) {
            return queue.pop_remove();
        }

        AlignedT *result = current_buffer + current_buffer_watermark;

        current_buffer_watermark++;

        if(current_buffer_watermark % BUFFER_NUMBER_OBJECTS == 0) {
            add_buffer();
        }

        return reinterpret_cast<T *>(result);
    }

    inline void deallocate(T *pointer, int source_id) noexcept {
        queue.push(pointer);
    }

    inline T *allocate_many(uint64_t size) noexcept {
        if(size == 1) {
            return allocate();
        }

        if(queue_buffers.size() > 0) {
            PointerSize<T> &front = queue_buffers.front();

            if(front.size >= size) {
                T *result = front.pointer;

                front.pointer += size;
                front.size -= size;

                if(front.size == 0) {
                    queue_buffers.pop();
                }

                return reinterpret_cast<T *>(result);
            }
        }

        if(size > BUFFER_NUMBER_OBJECTS) {
#if defined(LIBNUMA) && defined(__linux__)
            return reinterpret_cast<T *>(numa_alloc_local(size * sizeof(T)));
#else
            return reinterpret_cast<T *>(malloc(size * sizeof(T)));
#endif /* LIBNUMA and __linux__ */
        }

        if(current_buffer_watermark + size > BUFFER_NUMBER_OBJECTS) {
            deallocate_many(reinterpret_cast<T *>(current_buffer + current_buffer_watermark), BUFFER_NUMBER_OBJECTS - current_buffer_watermark, thread_id);
            add_buffer();
        }

        AlignedT *result = current_buffer + current_buffer_watermark;

        current_buffer_watermark += size;

        if(current_buffer_watermark % BUFFER_NUMBER_OBJECTS == 0) {
            add_buffer();
        }

        return reinterpret_cast<T *>(result);
    }

    inline void deallocate_many(T *pointer, uint64_t size, int source_id) noexcept {
        if(size == 1) {
            return deallocate(pointer, source_id);
        }

        queue_buffers.push(pointer, size);
    }

private:
    inline void add_buffer() noexcept {
#if defined(LIBNUMA) && defined(__linux__)
        AlignedT *memory = reinterpret_cast<AlignedT *>(numa_alloc_local(BUFFER_SIZE));
#else
        AlignedT *memory = reinterpret_cast<AlignedT *>(malloc(BUFFER_SIZE));
#endif /* LIBNUMA and __linux__ */

        buffers.emplace_back(memory);

        current_buffer = memory;
        current_buffer_number++;
        current_buffer_watermark = 0;
    }
};

template<class T>
class NUMAAllocatorStealing {
private:
    static constexpr int MAX_THREADS = 128;
    static constexpr int PADDING_SIZE = sizeof(T) + (sizeof(T) % 64);

    using AlignedT = union alignas(64) {
        T data;
        char padding[PADDING_SIZE];
    };

    static constexpr int BUFFER_NUMBER_OBJECTS = 1024 * 1024;
    static constexpr int BUFFER_SIZE = BUFFER_NUMBER_OBJECTS * sizeof(AlignedT); // sizeof(AlignedT) MiB blocks (must be multiple of sizeof(T) and 4K)

    std::vector<AlignedT *> buffers;

    FastQueue<T *> queue;
    FastQueueSize<T> queue_buffers;

    // Queues are 64-byte aligned, so there should not be false sharing
    FastQueuePC<T *> donation_queue[MAX_THREADS];
    FastQueueSizePC<T> donation_queue_buffers[MAX_THREADS];

    AlignedT *current_buffer;
    uint64_t current_buffer_number;
    uint64_t current_buffer_watermark;

public:
    NUMAAllocatorStealing(): current_buffer{nullptr}, current_buffer_number{UINT64_MAX}, current_buffer_watermark{0} {
        add_buffer();
    }

#ifdef RECLAMATION
    ~NUMAAllocatorStealing() {
        cleanup();
    }

    inline void cleanup() noexcept {
        for(AlignedT *buffer: buffers) {
#if defined(LIBNUMA) && defined(__linux__)
            numa_free(buffer, BUFFER_SIZE);
#else
            free(buffer);
#endif /* LIBNUMA and __linux__ */
        }
    }
#endif /* RECLAMATION */

    inline T *allocate() noexcept {
        if(queue.size() > 0) {
            return queue.pop_remove();
        }

        AlignedT *result = current_buffer + current_buffer_watermark;

        current_buffer_watermark++;

        if(current_buffer_watermark % BUFFER_NUMBER_OBJECTS == 0) {
            add_buffer();
        }

        return reinterpret_cast<T *>(result);
    }

    inline void deallocate(T *pointer, int source_id) noexcept {
        if(source_id == thread_id) {
            queue.push(pointer);
        }
        else {
            donation_queue[source_id].push(pointer);
        }
    }

    inline T *allocate_many(uint64_t size) noexcept {
        if(size == 1) {
            return allocate();
        }

        if(queue_buffers.size() > 0) {
            PointerSize<T> &front = queue_buffers.front();

            if(front.size >= size) {
                T *result = front.pointer;

                front.pointer += size;
                front.size -= size;

                if(front.size == 0) {
                    queue_buffers.pop();
                }

                return reinterpret_cast<T *>(result);
            }
        }

        if(size > BUFFER_NUMBER_OBJECTS) {
#if defined(LIBNUMA) && defined(__linux__)
            return reinterpret_cast<T *>(numa_alloc_local(size * sizeof(T)));
#else
            return reinterpret_cast<T *>(malloc(size * sizeof(T)));
#endif /* LIBNUMA and __linux__ */
        }

        if(current_buffer_watermark + size > BUFFER_NUMBER_OBJECTS) {
            deallocate_many(reinterpret_cast<T *>(current_buffer + current_buffer_watermark), BUFFER_NUMBER_OBJECTS - current_buffer_watermark, thread_id);
            add_buffer();
        }

        AlignedT *result = current_buffer + current_buffer_watermark;

        current_buffer_watermark += size;

        if(current_buffer_watermark % BUFFER_NUMBER_OBJECTS == 0) {
            add_buffer();
        }

        return reinterpret_cast<T *>(result);
    }

    inline void deallocate_many(T *pointer, uint64_t size, int source_id) noexcept {
        if(size == 1) {
            return deallocate(pointer, source_id);
        }

        if(source_id == thread_id) {
            queue_buffers.push(pointer, size);
        }
        else {
            donation_queue_buffers[source_id].push(pointer, size);
        }
    }

    template<class U>
    inline void steal(NUMAAllocatorStealing<U> *other_allocator) {
        uint64_t size_snapshot = (uint64_t) other_allocator->donation_queue[thread_id].size();

        for(uint64_t i = 0; i < size_snapshot; i++) {
            queue.push(other_allocator->donation_queue[thread_id].pop_remove());
        }
    }

private:
    inline void add_buffer() noexcept {
#if defined(LIBNUMA) && defined(__linux__)
        AlignedT *memory = reinterpret_cast<AlignedT *>(numa_alloc_local(BUFFER_SIZE));
#else
        AlignedT *memory = reinterpret_cast<AlignedT *>(malloc(BUFFER_SIZE));
#endif /* LIBNUMA and __linux__ */

        buffers.emplace_back(memory);

        current_buffer = memory;
        current_buffer_number++;
        current_buffer_watermark = 0;
    }
};

#endif /* ALLOCATOR_HPP */
