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

#ifndef ADDRESSING_HPP
#define ADDRESSING_HPP

#include <cstdint>

#include "thread_handler.h"

#include <atomic>
#include <future>

using std::atomic;
using std::cerr;
using std::errc;
using std::system_error;

namespace seriema {

constexpr uint64_t FLAGS_MASK = 0xffff000000000000;
constexpr uint64_t REFERENCE_MASK = 0x0000ffffffffffff;
constexpr uint64_t PROCESS_MASK = 0xffffffff00000000;
constexpr uint64_t THREAD_MASK = 0x00000000ffffffff;

template<typename T>
class GlobalAddress {
public:
    uint64_t address; // 16 bits of flags, 48 bits of address
    uint64_t process_thread; // 32 bits of process, 32 bits of threads

    GlobalAddress<T>(): GlobalAddress(nullptr) {
        address = 0;

        process_thread = ((seriema::process_rank & THREAD_MASK) << 32);
        process_thread = process_thread | (seriema::thread_rank);
    }

    GlobalAddress<T>(T *object_address) {
        address = reinterpret_cast<uint64_t>(object_address);

        process_thread = ((seriema::process_rank & THREAD_MASK) << 32);
        process_thread = process_thread | (seriema::thread_rank & THREAD_MASK);
    }

    GlobalAddress<T>(T &object_reference) {
        address = reinterpret_cast<uint64_t>(&object_reference);

        process_thread = ((seriema::process_rank & THREAD_MASK) << 32);
        process_thread = process_thread | (seriema::thread_rank & THREAD_MASK);
    }

    GlobalAddress<T>(const GlobalAddress<T> &other) {
        address = other.address;
        process_thread = other.process_thread;
    }

    // Gets the reference of the GlobalAddress
    inline uint64_t get_reference() const noexcept {
        return address & REFERENCE_MASK;
    }

    // Gets the process of the Global Address
    inline uint64_t get_process() const noexcept {
        return (process_thread & PROCESS_MASK) >> 32;
    }

    inline uint64_t get_thread_id() const noexcept {
        return (get_process() * seriema::number_threads_process) + get_thread_rank();
    }

    inline uint64_t get_thread_rank() const noexcept {
        return process_thread & THREAD_MASK;
    }

    inline T *get_address() const noexcept {
        return reinterpret_cast<T *>(get_reference());
    }

    inline bool operator==(const GlobalAddress<T> &other) const noexcept {
        return (address == other.address) && (process_thread == other.process_thread);
    }

    inline bool operator<(const GlobalAddress<T> &other) const noexcept {
        return address < other.address;
    }

    inline GlobalAddress<T> &operator=(const GlobalAddress<T> &other) noexcept {
        address = other.address;
        process_thread = other.process_thread;

        return *this;
    }

    // Pointer emulation
    inline T *operator->() const {
        return get_address();
    }

    // Pointer emulation
    inline T &operator*() const {
        return *get_address();
    }
};

} // namespace seriema

template<typename T>
std::ostream &operator<<(std::ostream &output, const seriema::GlobalAddress<T> &address) {
    output << "(" << address.get_process() << "/" << address.get_address() << ")";

    return output;
}

#endif /* ADDRESSING_HPP */