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

#ifndef TIMEHOLDER_H
#define TIMEHOLDER_H

#include <cstdint>

#include <time.h>

struct TimeHolder {
    alignas(64) struct timespec time;

    const clockid_t mode;

    TimeHolder(clockid_t mode = CLOCK_THREAD_CPUTIME_ID): mode{mode} {
        start();
    }

    inline void start() {
        clock_gettime(mode, &time);
    }

    inline uint64_t tick() {
        struct timespec new_time;

        clock_gettime(mode, &new_time);

        return (new_time.tv_sec - time.tv_sec) * 1000000000 + (new_time.tv_nsec - time.tv_nsec);
    }

    inline static uint64_t getTimestamp() {
        struct timespec new_time;

        clock_gettime(CLOCK_MONOTONIC_RAW, &new_time);

        return (new_time.tv_sec * 1000000000) + new_time.tv_nsec;
    }

    inline static uint64_t getCPUTimestamp() {
        unsigned int low, high;

        __asm__ __volatile__("rdtsc" : "=a"(low), "=d"(high));

        return ((uint64_t) high << 32) | ((uint64_t) low);
    }
};

#endif /* TIMEHOLDER_H */
