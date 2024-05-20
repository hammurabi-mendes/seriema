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

#ifndef SYNCHRONIZER_HPP
#define SYNCHRONIZER_HPP

#include <mutex>
#include <condition_variable>
#include <atomic>
#include <functional>

using std::mutex;
using std::condition_variable;
using std::unique_lock;
using std::defer_lock;

using std::atomic;

namespace dsys {
    extern void flush_send_completion_queues();
};

class RDMAMemory;

class Synchronizer {
protected:
    atomic<uint64_t> number_operations_left;
    atomic<uint64_t> flags;

    condition_variable *completed;
    mutex *lock_completed;

    std::function<void()> *callback_data = nullptr;

public:
    static constexpr int FLAGS_MONITOR = 0x1;
    static constexpr int FLAGS_CALLBACK = 0x2;
    static constexpr int FLAGS_SELFDESTRUCT = 0x4;

    Synchronizer(uint64_t total_operations, uint64_t flags = 0): number_operations_left{total_operations}, flags{flags} {
        if(flags & FLAGS_MONITOR) {
            completed = new condition_variable();
            lock_completed = new mutex();
        }
    };

    Synchronizer(const Synchronizer &other): Synchronizer(other.number_operations_left, other.flags) {
    }

    ~Synchronizer() {
        if(flags & FLAGS_MONITOR) {
            delete lock_completed;
            delete completed;
        }

        if(flags & FLAGS_CALLBACK) {
            if(callback_data != nullptr) {
                delete callback_data;
            }
        }
    }

    inline void increase(uint64_t operations = 1) noexcept {
        number_operations_left.fetch_add(operations, std::memory_order_relaxed);
    }

    inline void decrease(uint64_t operations = 1) noexcept {
        uint64_t flags_snapshot = flags;

        if(number_operations_left.fetch_sub(operations, std::memory_order_relaxed) == 1) {
            if(flags_snapshot & FLAGS_MONITOR) {
                unique_lock<mutex> lock_completed_holder(*lock_completed);

                completed->notify_all();
            }

            if(flags_snapshot & FLAGS_CALLBACK) {
                if(callback_data != nullptr) {
                    (*callback_data)();
                }
            }

            if(flags_snapshot & FLAGS_SELFDESTRUCT) {
                delete this;
            }
        }
    }

    inline uint64_t get_number_operations_left() const noexcept {
        return number_operations_left.load(std::memory_order_relaxed);
    }

    inline uint64_t spin_nonzero_operations_left() const noexcept {
        while(get_number_operations_left() > 0) {
            dsys::flush_send_completion_queues();
        }
    }

    inline void reset(uint64_t total_operations = 0) noexcept {
        number_operations_left.store(total_operations, std::memory_order_relaxed);
    }

    inline void operator=(const Synchronizer &other) noexcept {
        reset(other.number_operations_left);
    }

    ///////////////////////
    // Monitor functions //
    ///////////////////////

    inline void wait() {
        unique_lock<mutex> lock_completed_holder(*lock_completed);

        while(number_operations_left.load(std::memory_order_relaxed) == 0) {
            completed->wait(lock_completed_holder);
        }
    }

    ////////////////////////
    // Callback functions //
    ////////////////////////

    template<typename F>
    inline void set_callback(F &&function) noexcept {
        if(callback_data) {
            delete callback_data;
        }

        callback_data = new atomic<std::function<void()>>{function};

        flags |= FLAGS_CALLBACK; // Essential that this is the last operation for synchronization
    }
};

#endif /* SYNCHRONIZER_HPP */