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

#ifndef RANDOM_HPP
#define RANDOM_HPP

#include <random>
#include <vector>

#include "utils/TimeHolder.hpp"

using std::vector;

#ifdef DSYS_H
namespace dsys {
  extern thread_local int thread_id;
};

using dsys::thread_id;
#else
extern thread_local int thread_id;
#endif /* DSYS_H */

template<class T>
class Random {
public:
    static inline T getRandom(const T &min, const T &max) {
        static thread_local std::mt19937_64 engine(thread_id + TimeHolder::getCPUTimestamp());

        std::uniform_int_distribution<T> distribution(min, max);

        return reinterpret_cast<T>(distribution(engine));
    }

    static inline T getRandom(vector<double> &dicrete_probability_vector) {
        static thread_local std::mt19937_64 engine(thread_id + TimeHolder::getCPUTimestamp());

        std::discrete_distribution<T> distribution(dicrete_probability_vector.begin(), dicrete_probability_vector.end());

        return reinterpret_cast<T>(distribution(engine));
    }
};

#endif /* RANDOM_HPP */