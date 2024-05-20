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

#ifndef SERIALIZATION_BUFFER_H
#define SERIALIZATION_BUFFER_H

#include <vector>

#include <cstdlib>
#include <cstring>

#include <algorithm>

#include "addressing.hpp"

#include "Allocators.hpp"

using std::vector;

using std::max;

using std::accumulate;

using dsys::GlobalAddress;

/**
 * Class that manages a dynamically allocated buffer (in manager mode),
 * or simply provides size/usage tracking information (in non-manager mode).
 *
 * In the latter case, the buffer allocations/reallocations/frees do not work.
 */
template<typename Allocator = MallocAllocator<char>>
struct BaseSerializationBuffer {
    /// Data managed or operated by the buffer
    char *data;

    /// Size of the allocated buffer
    size_t size;

    /// Used size on the allocated buffer
    size_t used;

    /// Indicates whether the object is created in manager mode
    bool manager;

    /// Allocator used to obtain/release memory.
    Allocator allocator;

    /**
	 * Default constructor in manager mode.
	 */
    BaseSerializationBuffer(bool manager = true) : manager(manager) {
        data = nullptr;

        size = 0;
        used = 0;
    }

    /**
	 * Size-initialized constructor in manager mode.
	 */
    BaseSerializationBuffer(size_t initialSize) : BaseSerializationBuffer(true) {
        reserve(initialSize);
    }

    /**
	 * Data-initialized constructor in non-manager mode.
	 */
    BaseSerializationBuffer(void *data, size_t size, size_t used = 0) : BaseSerializationBuffer(false) {
        this->data = (char *) data;

        this->size = size;
        this->used = used;
    }

    /**
	 * Destructor. Frees the buffer only if in manager mode.
	 */
    ~BaseSerializationBuffer() {
        if(!manager) {
            return;
        }

        if(data) {
            allocator.deallocate(data, size);
        }
    }

    /**
	 * Returns the number of bytes that are free.
	 *
	 * @returns Number of bytes that are free.
	 */
    inline size_t free() const {
        return size - used;
    }

    /**
	 * Ensures that the serialization buffer contains space
	 * for \p extraSpaceRequired bytes.
	 */
    inline void ensure(size_t extraSpaceRequired) {
        if(used + extraSpaceRequired <= size) {
            return;
        }

        reserve(max(used + extraSpaceRequired, 2 * size));
    }

    /**
	 * Resets the serialization buffer's memory and internals.
	 */
    inline void reset() {
        if(data) {
            allocator.deallocate(data, size);
        }

        data = nullptr;
        size = 0;
        used = 0;
    }

    /**
	 * Maskes buffer large enough to contain \p newSize bytes.
	 * Is a no-op in non-manager mode.
	 */
    inline void reserve(size_t newSize) {
        if(!manager) {
            return;
        }

        if(newSize > size) {
            char *newData = (char *) allocator.allocate(newSize);

            if(!newData) {
                PRINT_INFO("Error requesting " << newSize << " bytes!");
            }

            std::memcpy(newData, data, used);

            if(data) {
                allocator.deallocate(data, size);
            }

            data = newData;
            size = newSize;
        }
    }

    /**
	 * Reserve space for the sum of bytes in all entries of the vector \p sizes.
	 */
    inline void reserve(vector<uint64_t> &sizes) {
        reserve(accumulate(sizes.begin(), sizes.end(), 0UL));
    }

    /**
	 * Concatenates all serialization buffers in \p buffers to this one,
	 * resizing memory such that it contains all data from all buffers in sequence.
	 */
    inline void append(vector<BaseSerializationBuffer<Allocator>> &buffers) {
        if(!manager) {
            return;
        }

        size_t extraSize = 0;

        for(BaseSerializationBuffer<Allocator> &other : buffers) {
            extraSize += other.used;
        }

        if(extraSize > free()) {
            reserve(used + extraSize);
        }

        for(BaseSerializationBuffer<Allocator> &other : buffers) {
            memcpy(data + used, other.data, other.used);

            used += other.used;
        }
    }

    /**
	 * Concatenates a serialization buffer \p other,
	 * resizing memory such that it contains all data from both buffers.
	 */
    inline void append(BaseSerializationBuffer<Allocator> &other) {
        if(!manager) {
            return;
        }

        if(other.used > free()) {
            reserve(used + other.used);
        }

        memcpy(data + used, other.data, other.used);

        used += other.used;
    }

    /**
	 * Creates a vector of auxiliary buffers that are backed by the same, single master serialization buffer.
	 * This setup is used in multiple instances of data transfer with all-to-all semantics.
	 *
	 * @param Size of each auxiliary buffer
	 * @param Offset (in the master buffer) where each auxiliary buffer's memory is located
	 */
    inline vector<BaseSerializationBuffer<Allocator>> setupAuxiliaryBuffers(const vector<uint64_t> &sizes, const vector<uint64_t> &offsets) {
        uint64_t sendSizeTotal = accumulate(sizes.begin(), sizes.end(), 0UL);

        reserve(sendSizeTotal);

        vector<BaseSerializationBuffer<Allocator>> buffers;

        for(int i = 0; i < offsets.size(); i++) {
            buffers.emplace_back(data + offsets[i], sizes[i]);
        }

        return buffers;
    }

    /**
	 * Copy object \p object into the serialization buffer.
	 *
	 * Assumes that the buffer has space to store the object.
	 */
    template<typename T>
    inline void copyIn(T &object) {
        copyPointerIn(&object, 1);
    }

    /**
	 * Copy vector of objects \p objects into the serialization buffer.
	 *
	 * Assumes that the buffer has space to store all the objects.
	 */
    template<typename T>
    inline void copyVectorIn(vector<T> &objects) {
        size_t vectorSize = objects.size();

        copyIn<size_t>(vectorSize);
        copyPointerIn<T>(objects.data(), objects.size());
    }

    /**
	 * Copy memory block of \p number objects \p objects into the serialization buffer.
	 *
	 * Assumes that the buffer has space to store all the objects.
	 */
    template<typename T>
    inline void copyPointerIn(T *objects, uint64_t number = 1) {
        ensure(sizeof(T) * number);

        memcpy(data + used, objects, sizeof(T) * number);

        used += (sizeof(T) * number);
    }

    /**
	 * Extracts an object of type T from the serialization buffer.
	 */
    template<typename T>
    inline T copyOut() {
        // Default construction required
        T destination;

        copyPointerOut(&destination, 1);

        return destination;
    }

    /**
	 * Extracts a vector of type T from the serialization buffer.
	 */
    template<typename T>
    inline vector<T> copyVectorOut() {
        size_t vectorSize = copyOut<size_t>();
        T *vectorData = movePointerOut<T>(vectorSize);

        vector<T> destination(vectorData, vectorData + vectorSize);

        return destination;
    }

    /**
	 * Copies a memory block of \p number objects of type T into \p destination.
	 */
    template<typename T>
    inline void copyPointerOut(T *destination, uint64_t number = 1) {
        memcpy(destination, data + used, sizeof(T) * number);

        used += (sizeof(T) * number);
    }

    /**
	 * Returns (without copying) a memory block of \p number objects stored in the
	 * serialization buffer itself.
	 */
    template<typename T>
    inline T *movePointerOut(uint64_t number = 1) {
        T *pointer = reinterpret_cast<T *>(data + used);

        used += (sizeof(T) * number);

        return pointer;
    }

    /**
	 * Serializes a global address \p address into the serialization buffer.
	 */
    template<typename T>
    inline void serializeAddress(GlobalAddress<T> address) {
        copyPointerIn<uint64_t>(&address.address);
    }
};

using SerializationBuffer = BaseSerializationBuffer<MallocAllocator<char>>;

#endif /* SERIALIZATION_BUFFER_H */
