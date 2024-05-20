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

#ifndef REMOTE_OBJECTS_HPP
#define REMOTE_OBJECTS_HPP

#include "ibutils.hpp"

#include <unordered_map>
#include <shared_mutex>

using std::unordered_map;
using std::shared_mutex;

template<typename K>
struct Mapper;

template<typename K>
class Mapper {
    unordered_map<uint64_t, void *> memory_map;

    shared_mutex mutex_memory_map;

public:
    template<typename T>
    inline T *get_object(K id) {
        std::shared_lock lock(mutex_memory_map);

        return reinterpret_cast<T *>(memory_map[id]);
    }

    inline void put_object(K id, void *object) {
        std::unique_lock lock(mutex_memory_map);

        memory_map[id] = object;
    }
};

template<class T>
class RDMAObject {
public:
    RDMAMemory *memory;
    T *object;

    RDMAMemoryLocator object_locator;

    template<typename... Args>
    RDMAObject(RDMAMemory *memory, uint64_t offset, Args &&... arguments): memory{memory} {
        object = new(memory->get_buffer(offset)) T(std::forward<Args>(arguments)...);

        object = memory->buffer(offset);
        object_locator = RDMAMemoryLocator(memory);
    }

    virtual ~RDMAObject() {
        object->~T();
    }
    
    inline T *operator->() const {
        return object;
    }

    inline T &operator*() const {
        return *object;
    }
};

#endif /* REMOTE_OBJECTS_HPP */
