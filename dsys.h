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

#ifndef DSYS_H
#define DSYS_H

#include "ibutils.hpp"
#include "thread_handler.h"

#include "addressing.hpp"
#include "remote_calls.hpp"
#include "rdma_messengers.hpp"
#include "rdma_aggregators.hpp"

#include "utils/Synchronizer.hpp"

#include <iostream>

using std::cout;
using std::cerr;
using std::cout;

/// Useful print functions
#define PRINT_INFO(str)                                                        \
  {                                                                            \
    dsys::print_mutex.lock();                                                  \
    cout << str << std::endl;                                                  \
    dsys::print_mutex.unlock();                                                \
  }
#define PRINT_ERROR(str)                                                       \
  {                                                                            \
    dsys::print_mutex.lock();                                                  \
    cerr << str << std::endl;                                                  \
    dsys::print_mutex.unlock();                                                \
  }

#define PRINTVAR_INFO(x) PRINT_INFO(#x ": " << x)
#define PRINTVAR_ERROR(x) PRINT_ERROR(#x ": " << x)

/// Useful timestamp macros
#define START_TIME(var) (var = TimeHolder::getNanoseconds())
#define ELAPSED_TIME(var) (TimeHolder::getNanoseconds() - var)

#endif /* DSYS_H */
