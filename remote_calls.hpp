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

#ifndef REMOTE_CALLS_HPP
#define REMOTE_CALLS_HPP

#include <iostream>
#include <thread>
#include <condition_variable>
#include <mutex>
#include <future>

#ifdef DEBUG
#include <cassert>
#endif /* DEBUG */

#include "memory_allocation.hpp"

#include "utils/Synchronizer.hpp"
#include "utils/Random.hpp"
#include "utils/Barrier.hpp"

using std::mutex;
using std::unique_lock;
using std::condition_variable;
using std::defer_lock;

using std::cout;
using std::endl;

namespace dsys {

template<typename T>
class FunctionWrapper {
public:
    // The address of the deserialize_call from the other system.
    uint64_t deserialize_function;

    // Lambda function storage
    T function;

    template<typename U>
    FunctionWrapper(U &&function): deserialize_function{reinterpret_cast<uint64_t>(FunctionWrapper<T>::deserialize_call)}, function{function} {};

    static inline uint64_t deserialize_call(void *message) {
        FunctionWrapper<T> *deserialize_wrapper = reinterpret_cast<FunctionWrapper<T> *>(message);

        (deserialize_wrapper->function)();

        return sizeof(FunctionWrapper<T>);
    }
};

template<typename T>
class FunctionWrapperBuffer {
public:
    // The address of the deserialize_call from the other system.
    uint64_t deserialize_function;

    // Lambda function storage
    T function;

    // Size of buffer that follows
    uint64_t buffer_size;

    template<typename U>
    FunctionWrapperBuffer(U &&function, uint64_t buffer_size): deserialize_function{reinterpret_cast<uint64_t>(FunctionWrapperBuffer<T>::deserialize_call)}, function{function}, buffer_size{buffer_size} {};

    static inline uint64_t deserialize_call(void *message) {
        FunctionWrapperBuffer<T> *deserialize_wrapper = reinterpret_cast<FunctionWrapperBuffer<T> *>(message);

        (deserialize_wrapper->function)(deserialize_wrapper + 1, deserialize_wrapper->buffer_size);

        return sizeof(FunctionWrapperBuffer<T>) + deserialize_wrapper->buffer_size;
    }
};

template<typename T>
class FlaggedFunctionWrapper {
public:
    // The address of the deserialize_call from the other system.
    volatile uint64_t deserialize_function;

    // Lambda function storage
    T function;

    // Flags
    volatile uint8_t marker;

    template<typename U>
    FlaggedFunctionWrapper(U &&function): deserialize_function{(reinterpret_cast<uint64_t>(FlaggedFunctionWrapper<T>::deserialize_call) << 8) | 0xff000000000000ff}, function{function}, marker{0xff} {};

    static inline uint64_t deserialize_call(void *message) {
        FlaggedFunctionWrapper<T> *deserialize_wrapper = reinterpret_cast<FlaggedFunctionWrapper<T> *>(message);

        if(deserialize_wrapper->marker != 0xff) {
            return 0;
        }

        (deserialize_wrapper->function)();

        return sizeof(FlaggedFunctionWrapper<T>);
    }
};

template<typename T>
class FlaggedFunctionWrapperBuffer {
public:
    // The address of the deserialize_call from the other system.
    volatile uint64_t deserialize_function;

    // Lambda function storage
    T function;

    // Size of buffer that follows
    uint64_t buffer_size;

    // Indicates position and byte value of the last non-zero byte
    uint64_t buffer_checkmark;

    volatile uint8_t marker;

    template<typename U>
    FlaggedFunctionWrapperBuffer(U &&function, uint64_t buffer_size): deserialize_function{(reinterpret_cast<uint64_t>(FlaggedFunctionWrapperBuffer<T>::deserialize_call) << 8) | 0xff000000000000ff}, function{function}, buffer_size{buffer_size}, marker{0xff} {};

    static inline uint64_t deserialize_call(void *message) {
        FlaggedFunctionWrapperBuffer<T> *deserialize_wrapper = reinterpret_cast<FlaggedFunctionWrapperBuffer<T> *>(message);

        if(deserialize_wrapper->marker != 0xff) {
            return 0;
        }

        volatile uint64_t buffer_checkmark = deserialize_wrapper->buffer_checkmark;

        if(buffer_checkmark == 0) {
            return 0;
        }

        if(buffer_checkmark != UINT64_MAX) {
            volatile uint8_t *flags = reinterpret_cast<uint8_t *>(reinterpret_cast<char *>(message) + sizeof(FlaggedFunctionWrapperBuffer<T>) + (buffer_checkmark & 0x00ffffffffffffff));

            if(*flags != ((buffer_checkmark & 0xff00000000000000) >> 56)) {
                return 0;
            }
        }

        (deserialize_wrapper->function)(deserialize_wrapper + 1, deserialize_wrapper->buffer_size);

        return sizeof(FlaggedFunctionWrapperBuffer<T>) + deserialize_wrapper->buffer_size;
    }
};

template<typename F>
inline RDMAMemory *get_function_wrapper(F &&function, uint64_t &flags) noexcept {
    using G = typename std::remove_reference<F>::type;

#ifndef WITHOUT_INLINE_CALLS
    if constexpr(sizeof(FunctionWrapper<G>) <= MAX_INLINE_DATA) {
        FunctionWrapper<G> *function_wrapper = new(thread_context->inline_memory.get_buffer()) FunctionWrapper<G>(function);

        flags |= IBV_SEND_INLINE;
        return &(thread_context->inline_memory);
    }
    else {
#endif /* WITHOUT_INLINE_CALLS */
        RDMAMemory *memory = thread_context->fine_outgoing_allocator->allocate_outgoing(sizeof(FunctionWrapper<G>));

        FunctionWrapper<G> *function_wrapper = new(memory->get_buffer()) FunctionWrapper<G>(function);

        return memory;
#ifndef WITHOUT_INLINE_CALLS
    }
#endif /* WITHOUT_INLINE_CALLS */
}

template<typename F>
inline RDMAMemory *get_function_wrapper_buffer(F &&function, void *data, uint64_t size, uint64_t &flags) noexcept {
    using G = typename std::remove_reference<F>::type;

#ifndef WITHOUT_INLINE_CALLS
    if(sizeof(FunctionWrapperBuffer<G>) + size <= MAX_INLINE_DATA) {
        FunctionWrapperBuffer<G> *function_wrapper = new(thread_context->inline_memory.get_buffer()) FunctionWrapperBuffer<G>(function, size);

        memcpy(thread_context->inline_memory.get_buffer(sizeof(FunctionWrapperBuffer<G>)), data, size);

        flags |= IBV_SEND_INLINE;
        return &(thread_context->inline_memory);
    }
    else {
#endif /* WITHOUT_INLINE_CALLS */
        RDMAMemory *memory = thread_context->fine_outgoing_allocator->allocate_outgoing(sizeof(FunctionWrapperBuffer<G>) + size);

        FunctionWrapperBuffer<G> *function_wrapper = new(memory->get_buffer()) FunctionWrapperBuffer<G>(function, size);

        memcpy(memory->get_buffer(sizeof(FunctionWrapperBuffer<G>)), data, size);

        return memory;
#ifndef WITHOUT_INLINE_CALLS
    }
#endif /* WITHOUT_INLINE_CALLS */
}

template<typename F>
inline RDMAMemory *get_function_wrapper_buffer(F &&function, uint64_t size, uint64_t &flags) noexcept {
    using G = typename std::remove_reference<F>::type;

#ifndef WITHOUT_INLINE_CALLS
    if constexpr(sizeof(FunctionWrapperBuffer<G>) <= MAX_INLINE_DATA) {
        FunctionWrapperBuffer<G> *function_wrapper = new(thread_context->inline_memory.get_buffer()) FunctionWrapperBuffer<G>(function, size);

        flags |= IBV_SEND_INLINE;
        return &(thread_context->inline_memory);
    }
    else {
#endif /* WITHOUT_INLINE_CALLS */
        RDMAMemory *memory = thread_context->fine_outgoing_allocator->allocate_outgoing(sizeof(FunctionWrapperBuffer<G>));

        FunctionWrapperBuffer<G> *function_wrapper = new(memory->get_buffer()) FunctionWrapperBuffer<G>(function, size);

        return memory;
#ifndef WITHOUT_INLINE_CALLS
    }
#endif /* WITHOUT_INLINE_CALLS */
}

template<typename F>
inline RDMAMemory *get_flagged_function_wrapper(F &&function, uint64_t &flags) noexcept {
    using G = typename std::remove_reference<F>::type;

#ifndef WITHOUT_INLINE_CALLS
    if constexpr(sizeof(FlaggedFunctionWrapper<G>) <= MAX_INLINE_DATA) {
        FlaggedFunctionWrapper<G> *function_wrapper = new(thread_context->inline_memory.get_buffer()) FlaggedFunctionWrapper<G>(function);

        flags |= IBV_SEND_INLINE;
        return &(thread_context->inline_memory);
    }
    else {
#endif /* WITHOUT_INLINE_CALLS */
        RDMAMemory *memory = thread_context->fine_outgoing_allocator->allocate_outgoing(sizeof(FlaggedFunctionWrapper<G>));

        FlaggedFunctionWrapper<G> *function_wrapper = new(memory->get_buffer()) FlaggedFunctionWrapper<G>(function);

        return memory;
#ifndef WITHOUT_INLINE_CALLS
    }
#endif /* WITHOUT_INLINE_CALLS */
}

template<typename F>
inline RDMAMemory *get_flagged_function_wrapper_buffer(F &&function, void *data, uint64_t size, uint64_t &flags) noexcept {
    using G = typename std::remove_reference<F>::type;

#ifndef WITHOUT_INLINE_CALLS
    if(sizeof(FlaggedFunctionWrapperBuffer<G>) + size <= MAX_INLINE_DATA) {
        FlaggedFunctionWrapperBuffer<G> *function_wrapper = new(thread_context->inline_memory.get_buffer()) FlaggedFunctionWrapperBuffer<G>(function, size);

        memcpy(thread_context->inline_memory.get_buffer(sizeof(FlaggedFunctionWrapperBuffer<G>)), data, size);

        flags |= IBV_SEND_INLINE;
        return &(thread_context->inline_memory);
    }
    else {
#endif /* WITHOUT_INLINE_CALLS */
        RDMAMemory *memory = thread_context->fine_outgoing_allocator->allocate_outgoing(sizeof(FlaggedFunctionWrapperBuffer<G>) + size);

        FlaggedFunctionWrapperBuffer<G> *function_wrapper = new(memory->get_buffer()) FlaggedFunctionWrapperBuffer<G>(function, size);

        memcpy(memory->get_buffer(sizeof(FlaggedFunctionWrapperBuffer<G>)), data, size);

        return memory;
#ifndef WITHOUT_INLINE_CALLS
    }
#endif /* WITHOUT_INLINE_CALLS */
}

template<typename F>
inline RDMAMemory *get_flagged_function_wrapper_buffer(F &&function, uint64_t size, uint64_t &flags) noexcept {
    using G = typename std::remove_reference<F>::type;

#ifndef WITHOUT_INLINE_CALLS
    if constexpr(sizeof(FlaggedFunctionWrapperBuffer<G>) <= MAX_INLINE_DATA) {
        FlaggedFunctionWrapperBuffer<G> *function_wrapper = new(thread_context->inline_memory.get_buffer()) FlaggedFunctionWrapperBuffer<G>(function, size);

        flags |= IBV_SEND_INLINE;
        return &(thread_context->inline_memory);
    }
    else {
#endif /* WITHOUT_INLINE_CALLS */
        RDMAMemory *memory = thread_context->fine_outgoing_allocator->allocate_outgoing(sizeof(FlaggedFunctionWrapperBuffer<G>));

        FlaggedFunctionWrapperBuffer<G> *function_wrapper = new(memory->get_buffer()) FlaggedFunctionWrapperBuffer<G>(function, size);

        return memory;
#ifndef WITHOUT_INLINE_CALLS
    }
#endif /* WITHOUT_INLINE_CALLS */
}

inline void process_call(void *data) noexcept {
    uint64_t *message_header = reinterpret_cast<uint64_t *>(data);

    // The contents of the first integer is the address of the deserializing function
    reinterpret_cast<uint64_t (*)(void *)>(message_header[0])(message_header);
}

inline uint64_t process_multiple_calls_flagged(void *data, uint64_t &total_consumed) noexcept {
    uint64_t position = 0;
    uint64_t number_processed = 0;

    uint64_t *message_header;

    uint64_t deserialized_payload;
    uint64_t deserialized_payload_effective;
    uint64_t consumed;

    while(true) {
        message_header = reinterpret_cast<uint64_t *>(reinterpret_cast<char *>(data) + position);

        deserialized_payload = *message_header;

        if((deserialized_payload & 0xff000000000000ff) != 0xff000000000000ff) {
            break;
        }

        deserialized_payload_effective = (deserialized_payload & 0x00ffffffffffff00) >> 8;

        // The contents of the first integer is the address of the deserializing function
        consumed = reinterpret_cast<uint64_t (*)(void *)>(deserialized_payload_effective)(reinterpret_cast<void *>(message_header));

        if(consumed == 0) {
            break;
        }

        position += consumed;

        number_processed++;
    }

    total_consumed = position;

    return number_processed;
}

inline uint64_t process_multiple_calls(void *data, uint64_t size) noexcept {
    uint64_t position = 0;
    uint64_t number_processed = 0;

    while(position < size) {
        uint64_t *message_header = reinterpret_cast<uint64_t *>(reinterpret_cast<char *>(data) + position);

        // The contents of the first integer is the address of the deserializing function
        position += reinterpret_cast<uint64_t (*)(void *)>(message_header[0])(message_header);

        number_processed++;
    }

    return number_processed;
}

inline uint64_t process_received_messages(ReceivedMessageInformation *received_message_information, uint64_t number_received) noexcept {
    uint64_t number_processed = number_received;

    for(uint64_t i = 0; i < number_received; i++) {
        ReceivedMessageInformation *work = received_message_information + i;

        if(work->has_immediate()) {
            uint64_t immediate = work->get_immediate();

            if(immediate & FLAG_SINGLE) {
                process_call(work->get_buffer());

                work->get_memory()->ready = true;
            }
            else if(immediate & FLAG_MULTIPLE) {
                uint64_t multiplicity = process_multiple_calls(work->get_buffer(), work->get_size());

                number_processed += (multiplicity - 1);

                work->get_memory()->ready = true;
            }
        }
#ifdef DEBUG
        else {
            assert(false); // Helps finding missing messages on debugging
        }
#endif /* DEBUG */
    }

    return number_processed;
}

inline uint64_t process_work_queue(FastQueuePC<ReceivedMessageInformation> &work_queue) noexcept {
    uint64_t outstanding_requests = 0;

    ReceivedMessageInformation *received_message_information = work_queue.receive(outstanding_requests);

    uint64_t processed_requests = dsys::process_received_messages(received_message_information, outstanding_requests);

    if(outstanding_requests > 0) {
        work_queue.pop_receive(outstanding_requests);
    }

    return processed_requests;
}

inline thread *get_work_queue_consumer(uint64_t offset, atomic<bool> &finished, FastQueuePC<ReceivedMessageInformation> &work_queue, const uint64_t sleep_nanoseconds = 0) {
    thread *work_queue_consumer = new thread([offset, &finished, &work_queue, sleep_nanoseconds] {
        dsys::init_thread(offset);

        while(!finished) {
            if(dsys::process_work_queue(work_queue) == 0) {
                if(sleep_nanoseconds) {
                    usleep(sleep_nanoseconds);
                }
            }
        }

        dsys::finalize_thread();
    });

    return work_queue_consumer;
}

///////////////////////
// Regular functions //
///////////////////////

template<typename F, uint64_t CALL_IMMEDIATE = FLAG_SINGLE>
inline void call(int destination_thread_id, F &&function, Synchronizer *synchronizer = nullptr) noexcept {
    uint32_t call_immediate = CALL_IMMEDIATE | destination_thread_id;

#ifdef DIRECT_LOCAL_CALLS
    int destination_process_rank = get_process_rank(destination_thread_id);

    if(destination_process_rank == process_rank) {
        function();

        if(synchronizer) {
            synchronizer->decrease();
        }

        return;
    }
#endif /* DIRECT_LOCAL_CALLS */

    using G = typename std::remove_reference<F>::type;

    uint64_t flags = 0;
    RDMAMemory *memory = get_function_wrapper(function, flags);

    get_transmitter(destination_thread_id)->send(memory, 0, sizeof(FunctionWrapper<G>), flags, &call_immediate, synchronizer);
}

template<typename F>
inline void call_service(int destination_thread_id, F &&function, Synchronizer *synchronizer = nullptr) noexcept {
    call<F, FLAG_SERVICE>(destination_thread_id, std::forward<F>(function), synchronizer);
}

inline void call(int destination_thread_id, RDMAMemory *memory, uint64_t size, Synchronizer *synchronizer = nullptr) noexcept {
    uint32_t call_immediate = FLAG_SINGLE | destination_thread_id;

#ifdef DIRECT_LOCAL_CALLS
    int destination_process_rank = get_process_rank(destination_thread_id);

    if(destination_process_rank == process_rank) {
        process_call(memory->get_buffer());

        if(memory->internal_memory) {
            delete memory;
        }
        else {
            memory->operation_timestamp.store(0, std::memory_order::memory_order_relaxed);
        }

        if(synchronizer) {
            synchronizer->decrease();
        }

        return;
    }
#endif /* DIRECT_LOCAL_CALLS */

#ifndef WITHOUT_INLINE_CALLS
    if (size <= MAX_INLINE_DATA) {
        get_transmitter(destination_thread_id)->send(memory, 0, size, IBV_SEND_INLINE, &call_immediate, synchronizer);
    }
    else {
#endif /* WITHOUT_INLINE_CALLS */
        get_transmitter(destination_thread_id)->send(memory, 0, size, 0, &call_immediate, synchronizer);
#ifndef WITHOUT_INLINE_CALLS
    }
#endif /* WITHOUT_INLINE_CALLS */
}

//////////////////////
// Return functions //
//////////////////////

template<typename F, typename R>
inline void call_return_sends(int destination_thread_id, F &&function, R *result, Synchronizer *synchronizer = nullptr) noexcept {
#ifdef DIRECT_LOCAL_CALLS
    int destination_process_rank = get_process_rank(destination_thread_id);

    if(destination_process_rank == process_rank) {
        *result = function();

        if(synchronizer) {
            synchronizer->decrease();
        }

        return;
    }
#endif /* DIRECT_LOCAL_CALLS */

    if(synchronizer) {
        call(destination_thread_id, [source_thread_id = thread_id, function, result, synchronizer]() {
            call(source_thread_id, [synchronizer, result_address = result, result_value = function()] {
                *result_address = result_value;

                synchronizer->decrease();
            });
        });
    }
    else {
        call(destination_thread_id, [source_thread_id = thread_id, function, result]() {
            call(source_thread_id, [result_address = result, result_value = function()] {
                *result_address = result_value;
            });
        });
    }
}

template<typename F>
inline void call_return(int destination_thread_id, F &&function, const RDMAMemoryLocator &local_memory_locator) noexcept {
    using R = typename std::remove_reference<decltype(function())>::type;

#ifdef DIRECT_LOCAL_CALLS
    int destination_process_rank = get_process_rank(destination_thread_id);

    if(destination_process_rank == process_rank) {
        *(reinterpret_cast<R *>(local_memory_locator.get_buffer())) = function();

        if(synchronizer) {
            synchronizer->decrease();
        }

        return;
    }
#endif /* DIRECT_LOCAL_CALLS */

    call(destination_thread_id, [source_thread_id = thread_id, function, remote_memory_locator = local_memory_locator]() {
        RDMAMemory *memory = thread_context->fine_outgoing_allocator->allocate_outgoing(sizeof(R));
        R *result_pointer = reinterpret_cast<R *>(memory->get_buffer());

        *result_pointer = function();

        get_transmitter(source_thread_id)->rdma_write(memory, 0, sizeof(R), &remote_memory_locator);
    });
}

template<typename F>
inline void call_return_array(int destination_thread_id, F &&function, const RDMAMemoryLocator &local_memory_locator, const uint64_t local_size) noexcept {
    using RP = typename std::remove_reference<decltype(function(nullptr))>::type;

#ifdef DIRECT_LOCAL_CALLS
    int destination_process_rank = get_process_rank(destination_thread_id);

    if(destination_process_rank == process_rank) {
        *(reinterpret_cast<R *>(local_memory_locator.get_buffer())) = function();

        if(synchronizer) {
            synchronizer->decrease();
        }

        return;
    }
#endif /* DIRECT_LOCAL_CALLS */

    call(destination_thread_id, [source_thread_id = thread_id, function, remote_memory_locator = local_memory_locator, remote_size = local_size]() {
        RDMAMemory *memory = thread_context->fine_outgoing_allocator->allocate_outgoing(remote_size);
        RP result_pointer = reinterpret_cast<RP>(memory->get_buffer());

        function(result_pointer);

        get_transmitter(source_thread_id)->rdma_write(memory, 0, remote_size, &remote_memory_locator);
    });
}

template<typename F>
inline void call_return(int destination_thread_id, F &&function, RDMAMemory *memory, uint64_t offset) noexcept {
    RDMAMemoryLocator memory_locator(memory);

    call_return(destination_thread_id, function, memory_locator);
}

template<typename F>
inline void call_return_array(int destination_thread_id, F &&function, RDMAMemory *memory, uint64_t offset, uint64_t size) noexcept {
    RDMAMemoryLocator memory_locator(memory);

    call_return_array(destination_thread_id, function, memory_locator, size);
}

//////////////////////
// Thread functions //
//////////////////////

template<typename F>
inline void call_thread(int destination_thread_id, F &&function) noexcept {
    call(destination_thread_id, [function]() {
        thread spawned(function);

        spawned.detach();
    });
}

////////////////////////////////////
// Notification-related functions //
////////////////////////////////////

template<typename F>
inline void call_register(int destination_thread_id, F &&function, Synchronizer *synchronizer) noexcept {
    if(!synchronizer) {
        call(destination_thread_id, function);

        return;
    }

    call(destination_thread_id, [function, source_thread_id = thread_id, synchronizer]() {
        function();

        call(source_thread_id, [synchronizer]() {
            synchronizer->decrease();
        });
    });
}

template<typename F, typename G>
inline void call_register(int destination_thread_id, F &&function, G &&user_notify_function) noexcept {
    Synchronizer *callback_synchronizer = new Synchronizer(1, Synchronizer::FLAGS_CALLBACK | Synchronizer::FLAGS_SELFDESTRUCT);

    callback_synchronizer->set_callback([thread_id = thread_id, user_notify_function] {
        dsys::call(thread_id, user_notify_function);
    });

    call_register(destination_thread_id, function, callback_synchronizer);
}

//////////////////////
// Buffer functions //
//////////////////////

template<typename F>
inline void sync_read_call(int source_thread_id, F &&function, RDMAMemory *local_memory, uint64_t offset, uint64_t size, const RDMAMemoryLocator &remote_memory_locator, Synchronizer *synchronizer = nullptr) noexcept {
    Synchronizer rdma_synchronizer{1};

    get_transmitter(source_thread_id)->rdma_read_batches(local_memory, 0, size, &remote_memory_locator, IBV_SEND_SIGNALED, &rdma_synchronizer);

    rdma_synchronizer.spin_nonzero_operations_left();

    if(synchronizer) {
        synchronizer->decrease();
    }

    function(local_memory->get_buffer(), size);

    thread_context->linear_allocator->deallocate(local_memory);
}

template<typename F>
inline void sync_write_call(int destination_thread_id, F &&function, RDMAMemory *local_memory, uint64_t offset, uint64_t size, const RDMAMemoryLocator &remote_memory_locator, Synchronizer *synchronizer = nullptr) noexcept {
    Synchronizer rdma_synchronizer{1};

    get_transmitter(destination_thread_id)->rdma_write_batches(local_memory, offset, size, &remote_memory_locator, IBV_SEND_SIGNALED, nullptr, &rdma_synchronizer);

    rdma_synchronizer.spin_nonzero_operations_left();

    call(destination_thread_id, [function, buffer = remote_memory_locator.get_buffer(), size]() {
        function(buffer, size);
    }, synchronizer);
}

template<typename F>
inline void async_read_call(int source_thread_id, F &&function, RDMAMemory *local_memory, uint64_t offset, uint64_t size, const RDMAMemoryLocator &remote_memory_locator, Synchronizer *synchronizer = nullptr) noexcept {
    Synchronizer *rdma_synchronizer = new Synchronizer(1, Synchronizer::FLAGS_CALLBACK | Synchronizer::FLAGS_SELFDESTRUCT);

    rdma_synchronizer->set_callback([thread_id = thread_id, function, local_memory, size, source_thread_id, synchronizer] {
        dsys::call(thread_id, [function, local_memory, size, source_thread_id, synchronizer] {
            function(local_memory->get_buffer(), size);

            thread_context->linear_allocator->deallocate(local_memory);
        }, synchronizer);
    });

    std::async(std::launch::async, [source_thread_id, local_memory, size, remote_memory_locator, rdma_synchronizer] {
        get_transmitter(source_thread_id)->rdma_read_batches(local_memory, 0, size, &remote_memory_locator, IBV_SEND_SIGNALED, rdma_synchronizer);
    });
}

template<typename F>
inline auto async_write_call(int destination_thread_id, F &&function, RDMAMemory *local_memory, uint64_t offset, uint64_t size, const RDMAMemoryLocator &remote_memory_locator, Synchronizer *synchronizer = nullptr) noexcept {
    Synchronizer *rdma_synchronizer = new Synchronizer(1, Synchronizer::FLAGS_CALLBACK | Synchronizer::FLAGS_SELFDESTRUCT);

    rdma_synchronizer->set_callback([destination_thread_id, function, buffer = remote_memory_locator.get_buffer(), size, source_thread_id = thread_id, synchronizer] {
        dsys::call(destination_thread_id, [function, buffer, size, source_thread_id, synchronizer]() {
            function(buffer, size);
        }, synchronizer);
    });

    return std::async(std::launch::async, [destination_thread_id, local_memory, offset, size, remote_memory_locator, rdma_synchronizer] {
        get_transmitter(destination_thread_id)->rdma_write_batches(local_memory, offset, size, &remote_memory_locator, IBV_SEND_SIGNALED, nullptr, rdma_synchronizer);
    });
}

template<typename F, bool ASYNCHRONOUS = true>
inline void call_buffer(int destination_thread_id, F &&function, RDMAMemory *local_memory, uint64_t offset, uint64_t size, Synchronizer *synchronizer = nullptr) noexcept {
#ifdef DIRECT_LOCAL_CALLS
    int destination_process_rank = get_process_rank(destination_thread_id);

    if(destination_process_rank == process_rank) {
        function(buffer, size);

        if(synchronizer) {
            synchronizer->decrease();
        }

        return;
    }
#endif /* DIRECT_LOCAL_CALLS */

    RDMAMemoryLocator local_memory_locator{local_memory, offset};

    if constexpr(ASYNCHRONOUS) {
        call(destination_thread_id, [function, remote_memory_locator = local_memory_locator, size, source_thread_id = thread_id] {
            RDMAMemory *local_memory = thread_context->linear_allocator->allocate(size);

            async_read_call(source_thread_id, function, local_memory, 0, size, remote_memory_locator);
        }, synchronizer);
    }
    else {
        call(destination_thread_id, [function, remote_memory_locator = local_memory_locator, size, source_thread_id = thread_id] {
            RDMAMemory *local_memory = thread_context->linear_allocator->allocate(size);

            sync_read_call(source_thread_id, function, local_memory, 0, size, remote_memory_locator);
        }, synchronizer);
    }
}

template<typename F, bool ASYNCHRONOUS = true>
inline auto call_buffer(int destination_thread_id, F &&function, RDMAMemory *local_memory, uint64_t offset, uint64_t size, RDMAMemoryLocator *remote_memory_locator, Synchronizer *synchronizer = nullptr) noexcept {
#ifdef DIRECT_LOCAL_CALLS
    int destination_process_rank = get_process_rank(destination_thread_id);

    if(destination_process_rank == process_rank) {
        function(local_memory->get_buffer(offset), size);

        if(synchronizer) {
            synchronizer->decrease();
        }

        return;
    }
#endif /* DIRECT_LOCAL_CALLS */

    if constexpr(ASYNCHRONOUS) {
        return async_write_call(destination_thread_id, function, local_memory, offset, size, *remote_memory_locator, synchronizer);
    }
    else {
        sync_write_call(destination_thread_id, function, local_memory, offset, size, *remote_memory_locator, synchronizer);
    }
}

template<typename F>
inline bool call_buffer(int destination_thread_id, F &&function, void *data, uint64_t size, Synchronizer *synchronizer = nullptr) noexcept {
    uint32_t call_immediate = FLAG_SINGLE | destination_thread_id;

#ifdef DIRECT_LOCAL_CALLS
    int destination_process_rank = get_process_rank(destination_thread_id);

    if(destination_process_rank == process_rank) {
        function(data, size);

        if(synchronizer) {
            synchronizer->decrease();
        }

        return;
    }
#endif /* DIRECT_LOCAL_CALLS */

    using G = typename std::remove_reference<F>::type;

    if(sizeof(FunctionWrapperBuffer<G>) + size <= RDMA_MEMORY_SIZE) {
        uint64_t flags = 0;
        RDMAMemory *memory = get_function_wrapper_buffer(function, data, size, flags);

        get_transmitter(destination_thread_id)->send(memory, 0, sizeof(FunctionWrapperBuffer<G>) + size, flags, &call_immediate, synchronizer);

        return true;
    }

#ifdef DEBUG
    cerr << "Warning: call buffer does not fit RDMA_MEMORY_SIZE" << endl;
#endif /* DEBUG */
    return false;
}

/////////////////////////
// Broadcast functions //
/////////////////////////

// TODO: Create "hierarchical" versions of those functions

template<typename F>
inline void call_everyone_processes(F &&function) noexcept {
    for(int destination_process_rank = 0; destination_process_rank < number_processes; destination_process_rank++) {
        int manager_thread_id = get_random_thread_id(destination_process_rank);

        call(manager_thread_id, function);
    }
}

template<typename F>
inline void call_everyone(F &&function) noexcept {
    call_everyone_processes([function] {
        for(int local_thread_id: get_local_thread_ids()) {
            call(local_thread_id, function);
        }
    });
}

// The remote memory locators have one entry per process rank
template<typename F>
inline void call_buffer_everyone_processes(F &&function, RDMAMemory *local_memory, uint64_t offset, uint64_t size, RDMAMemoryLocator **remote_memory_locators, Synchronizer *synchronizer = nullptr) noexcept {
    Synchronizer rdma_synchronizer{number_processes};

    int manager_thread_ids[number_processes];

    for(int destination_process_rank = 0; destination_process_rank < number_processes; destination_process_rank++) {
        manager_thread_ids[destination_process_rank] = get_random_thread_id(destination_process_rank);

        transmitters[manager_thread_ids[destination_process_rank]]->rdma_write(local_memory, offset, size, remote_memory_locators[destination_process_rank], IBV_SEND_SIGNALED, nullptr, &rdma_synchronizer);
    }

    // TODO: If this call is made asynchronous, the synchronizer cannot be on the stack anymore
    rdma_synchronizer.spin_nonzero_operations_left();

    for(int destination_process_rank = 0; destination_process_rank < number_processes; destination_process_rank++) {
        call(manager_thread_ids[destination_process_rank], [function, buffer = remote_memory_locators[destination_process_rank]->get_buffer(), size]() {
            function(buffer, size);
        }, synchronizer);
    }
}

// The remote memory locators have one entry per thread
template<typename F>
inline void call_buffer_everyone(F &&function, RDMAMemory *local_memory, uint64_t offset, uint64_t size, RDMAMemoryLocator **remote_memory_locators, Synchronizer *synchronizer = nullptr) noexcept {
    Synchronizer rdma_synchronizer{number_threads};

    for(int destination_thread_id = 0; destination_thread_id < number_threads; destination_thread_id++) {
        get_transmitter(destination_thread_id)->rdma_write(local_memory, offset, size, remote_memory_locators[destination_thread_id], IBV_SEND_SIGNALED, nullptr, &rdma_synchronizer);
    }

    // TODO: If this call is made asynchronous, the synchronizer cannot be on the stack anymore
    rdma_synchronizer.spin_nonzero_operations_left();

    for(int destination_thread_id = 0; destination_thread_id < number_threads; destination_thread_id++) {
        call(destination_thread_id, [function, buffer = remote_memory_locators[destination_thread_id]->get_buffer(), size]() {
            function(buffer, size);
        }, synchronizer);
    }
}

// The local memory pointers have one entry per process rank
// The offsets have one entry per process rank
// The sizes have one entry per process rank
// The remote memory locators have one entry per process rank
template<typename F>
inline void call_buffer_everyone_processes(F &&function, RDMAMemory **local_memories, uint64_t *offsets, uint64_t *sizes, RDMAMemoryLocator **remote_memory_locators, Synchronizer *synchronizer = nullptr) noexcept {
    Synchronizer rdma_synchronizer{number_processes};

    int manager_thread_ids[number_processes];

    for(int destination_process_rank = 0; destination_process_rank < number_processes; destination_process_rank++) {
        manager_thread_ids[destination_process_rank] = get_random_thread_id(destination_process_rank);

        transmitters[manager_thread_ids[destination_process_rank]]->rdma_write(
            local_memories[destination_process_rank], offsets[destination_process_rank], sizes[destination_process_rank], remote_memory_locators[destination_process_rank], IBV_SEND_SIGNALED, nullptr, &rdma_synchronizer);
    }

    // TODO: If this call is made asynchronous, the synchronizer cannot be on the stack anymore
    rdma_synchronizer.spin_nonzero_operations_left();

    for(int destination_process_rank = 0; destination_process_rank < number_processes; destination_process_rank++) {
        call(manager_thread_ids[destination_process_rank], [function, buffer = remote_memory_locators[destination_process_rank]->get_buffer(), size = sizes[destination_process_rank]]() {
            function(buffer, size);
        }, synchronizer);
    }
}

// The local memory pointers have one entry per thread
// The offsets have one entry per thread
// The sizes have one entry per thread
// The remote memory locators have one entry per thread
template<typename F>
inline void call_buffer_everyone(F &&function, RDMAMemory **local_memories, uint64_t *offsets, uint64_t *sizes, RDMAMemoryLocator **remote_memory_locators, Synchronizer *synchronizer = nullptr) noexcept {
    Synchronizer rdma_synchronizer{(uint64_t) number_threads};

    for(int destination_thread_id = 0; destination_thread_id < number_threads; destination_thread_id++) {
        get_transmitter(destination_thread_id)->rdma_write(
            local_memories[destination_thread_id], offsets[destination_thread_id], sizes[destination_thread_id], remote_memory_locators[destination_thread_id], IBV_SEND_SIGNALED, nullptr, &rdma_synchronizer);
    }

    // TODO: If this call is made asynchronous, the synchronizer cannot be on the stack anymore
    rdma_synchronizer.spin_nonzero_operations_left();

    for(int destination_thread_id = 0; destination_thread_id < number_threads; destination_thread_id++) {
        call(destination_thread_id, [function, buffer = remote_memory_locators[destination_thread_id]->get_buffer(), size = sizes[destination_thread_id]]() {
            function(buffer, size);
        },
        synchronizer);
    }
}

}; // namespace dsys

#endif /* REMOTE_CALLS_HPP */