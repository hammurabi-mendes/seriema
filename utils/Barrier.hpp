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

#ifndef BARRIER_HPP
#define BARRIER_HPP

#include <mutex>
#include <condition_variable>
#include <atomic>

using std::mutex;
using std::condition_variable;
using std::atomic;
using std::unique_lock;
using std::defer_lock;

class Barrier {
private:
    uint64_t number_threads;
    atomic<uint64_t> number_threads_missing;

    uint64_t barrier_iteration;

    // Condition variable that is used to signal when work is completed (i.e. number_operations_left is 0)
    condition_variable condition_done;

    // A mutex that guards number_operations_left
    mutex lock_condition_done;

public:
    Barrier(uint64_t expected_number_threads = 1): number_threads{expected_number_threads}, number_threads_missing{expected_number_threads} {
        barrier_iteration = 0;
    };

    inline void wait() {
        unique_lock<mutex> ul_notification_mutex(lock_condition_done);

        uint64_t snapshot = barrier_iteration;

        if(number_threads_missing.fetch_sub(1) == 1) {
            reset();

            condition_done.notify_all();
        }
        else {
            condition_done.wait(ul_notification_mutex, [this, snapshot] {
                return snapshot + 1 == barrier_iteration;
            });
        }
    }

    void reset() {
        barrier_iteration++;
        number_threads_missing = number_threads;
    }

    void setup(uint64_t new_number_threads) {
        barrier_iteration = 0;
        number_threads = number_threads_missing = new_number_threads;
    }
};

#endif /* BARRIER_HPP */