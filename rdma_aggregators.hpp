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

#ifndef RDMA_AGGREGATOR_H
#define RDMA_AGGREGATOR_H

#include <vector>
#include <queue>
#include <mutex>

#include "remote_calls.hpp"
#include "rdma_messengers.hpp"

using dsys::RDMAMessenger;
using dsys::RDMAMessengerGlobal;

using std::vector;
using std::queue;

using std::mutex;
using std::lock_guard;

namespace dsys {

template<typename F>
constexpr int pack_size(F &&function) noexcept {
    using G = typename std::remove_reference<F>::type;

    return sizeof(FunctionWrapper<G>);
}

template<typename F>
constexpr int pack_size_buffer(F &&function, uint64_t size) noexcept {
    using G = typename std::remove_reference<F>::type;

    return sizeof(FunctionWrapperBuffer<G>) + size;
}

template<typename F>
inline void pack_function_wrapper(F &&function, RDMAMemory *memory, uint64_t offset) noexcept {
    using G = typename std::remove_reference<F>::type;

    FunctionWrapper<G> *function_wrapper = new(memory->get_buffer(offset)) FunctionWrapper<G>(function);
}

template<typename F>
inline void pack_function_wrapper_buffer(F &&function, void *buffer, uint64_t size, RDMAMemory *memory, uint64_t offset) noexcept {
    using G = typename std::remove_reference<F>::type;

    FunctionWrapperBuffer<G> *function_wrapper = new(memory->get_buffer(offset)) FunctionWrapperBuffer<G>(function, size);

    memcpy(memory->get_buffer(offset + sizeof(FunctionWrapperBuffer<G>)), buffer, size);
}

template<typename Allocator, typename Messenger, uint64_t NUMBER_BUFFERS_MAXIMUM = UINT64_MAX, uint64_t RECURRENT_FLUSH_SIZE = 4000>
class RDMAAggregator {
    constexpr static double BUFFER_REPLACEMENT_PERCENT = 0.875;

protected:
    Allocator *flush_allocator;
    Messenger *flush_messenger;

    const int destination_thread_id;

    const uint64_t PACK_UPPER_LIMIT;

    RDMAMemory *memory = nullptr;
    uint64_t initial_position = 0;
    uint64_t current_position = 0;

    struct AggregationDescriptor {
        RDMAMemory *memory;
        uint64_t offset;
        uint64_t size;

        vector<Synchronizer *> notification_synchronizers;

        bool unstick_memory = false;
        bool perform_shutdown = false;

        Synchronizer *synchronizer = nullptr;

        AggregationDescriptor(RDMAMemory *memory, uint64_t offset, uint64_t size, vector<Synchronizer *> &notification_synchronizers, bool unstick_memory, bool perform_shutdown = false, Synchronizer *synchronizer = nullptr): memory{memory}, offset{offset}, size{size}, notification_synchronizers{std::move(notification_synchronizers)}, unstick_memory{unstick_memory}, perform_shutdown{perform_shutdown}, synchronizer{synchronizer} {}
    };

    vector<Synchronizer *> notification_synchronizers;

    queue<AggregationDescriptor> queued_transmissions;
    bool empty_queue= true;

    constexpr static bool OVERFLOW_MODE = false;

#ifdef STATS
    uint64_t number_allocations = 0;
    uint64_t maximum_allocations = 0;
#endif /* STATS */
public:
    RDMAAggregator(Allocator *flush_allocator, Messenger *flush_messenger, int destination_thread_id): flush_allocator{flush_allocator}, flush_messenger{flush_messenger}, destination_thread_id{destination_thread_id}, PACK_UPPER_LIMIT{flush_allocator->get_buffer_size() - 128} {
        // TODO: Consider lazy reset
        reset();
    }

    virtual ~RDMAAggregator() {
    #ifdef STATS
        dsys::print_mutex.lock();
        cout << "Stats for aggregator " << thread_id << "/" << destination_thread_id << endl;
        cout << "number_allocations: " << number_allocations << endl;
        cout << "maximum_allocations: " << maximum_allocations << endl;
        dsys::print_mutex.unlock();
    #endif /* STATS */
    }

    ///////////////////////
    // Packing functions //
    ///////////////////////

    template<typename F>
    inline bool pack(int destination_thread_id, F &&function, Synchronizer *synchronizer = nullptr) noexcept {
        if(current_position + pack_size(function) > PACK_UPPER_LIMIT) {
            return false;
        }

        pack_function_wrapper(function, get_memory(), get_current_position());

        current_position += pack_size(function);

        if(synchronizer) {
            notification_synchronizers.emplace_back(synchronizer);
        }

        return true;
    }

    template<typename F>
    inline bool pack_buffer(int destination_thread_id, F &&function, void *buffer, uint64_t size, Synchronizer *synchronizer = nullptr) noexcept {
        if(current_position + pack_size_buffer(function, size) > PACK_UPPER_LIMIT) {
            return false;
        }

        pack_function_wrapper_buffer(function, buffer, size, get_memory(), get_current_position());

        current_position += pack_size_buffer(function, size);

        if(synchronizer) {
            notification_synchronizers.emplace_back(synchronizer);
        }

        return true;
    }

    ////////////////////////
    // Accessor functions //
    ////////////////////////

    inline Allocator *get_flush_allocator() const noexcept {
        return flush_allocator;
    }

    inline Allocator *get_flush_messenger() const noexcept {
        return flush_messenger;
    }

    inline RDMAMemory *get_memory() const noexcept {
        return memory;
    }

    inline uint64_t get_initial_position() const noexcept {
        return initial_position;
    }

    inline uint64_t get_current_position() const noexcept {
        return current_position;
    }

    inline uint64_t get_size() const noexcept {
        return current_position - initial_position;
    }

    inline void reset() noexcept {
        memory = flush_allocator->allocate_outgoing();

        memory->sticky = true;

        initial_position = 0;
        current_position = 0;
    }

    inline void flush_internal(bool perform_shutdown, Synchronizer *synchronizer = nullptr) noexcept {
        if(empty_queued_transmissions()) {
            if(get_size() > 0) {
                if(!transmit_buffer(perform_shutdown, synchronizer)) {
                    queue_buffer(perform_shutdown, synchronizer);
                }
            }
            else {
                if(perform_shutdown) {
                    flush_messenger->shutdown(synchronizer);
                }
                else if(synchronizer) {
                    synchronizer->decrease();
                }
            }
        }
        else {
            if(get_size() > 0) {
                if(synchronizer) {
                    queue_buffer_forced(perform_shutdown, synchronizer);
                }
                else {
                    queue_buffer(perform_shutdown, synchronizer);
                }
            }
            else if(perform_shutdown == true || synchronizer != nullptr) {
                AggregationDescriptor &tail = queued_transmissions.back();

                if(perform_shutdown) {
                    tail.perform_shutdown = true;
                }
                if(tail.synchronizer) {
                    tail.notification_synchronizers.emplace_back(tail.synchronizer);
                }

                tail.synchronizer = synchronizer;
            }
        }
    }

    inline bool flush(Synchronizer *synchronizer = nullptr) noexcept {
        if(empty_queue == true && get_size() == 0) {
            if(synchronizer) {
                synchronizer->decrease();
            }

            return true;
        }

        flush_internal(false, synchronizer);
        return false;
    }

    inline void queue_buffer_forced(bool perform_shutdown, Synchronizer *synchronizer = nullptr) noexcept {
        queued_transmissions.emplace(get_memory(), get_initial_position(), get_size(), notification_synchronizers, true, perform_shutdown, synchronizer);
        empty_queue = false;

#ifdef STATS
        number_allocations++;
        if(number_allocations > maximum_allocations) {
            maximum_allocations = number_allocations;
        }
#endif /* STATS */

        reset();
    }

    inline bool queue_buffer(bool perform_shutdown, Synchronizer *synchronizer = nullptr) noexcept {
        if(get_size() > 0) {
            if(queued_transmissions.size() >= NUMBER_BUFFERS_MAXIMUM) {
                return false;
            }

            queued_transmissions.emplace(get_memory(), get_initial_position(), get_size(), notification_synchronizers, true, perform_shutdown, synchronizer);
            empty_queue = false;

#ifdef STATS
            number_allocations++;
            if(number_allocations > maximum_allocations) {
                maximum_allocations = number_allocations;
            }
#endif /* STATS */

            reset();
        }

        return true;
    }

    inline bool transmit_buffer(bool perform_shutdown, Synchronizer *synchronizer = nullptr) noexcept {
        if(get_size() > 0) {
            Synchronizer *call_synchronizer;

            call_synchronizer = perform_shutdown ? nullptr : synchronizer;

            if(current_position >= BUFFER_REPLACEMENT_PERCENT * PACK_UPPER_LIMIT) {
                get_memory()->operation_timestamp = UINT64_MAX;
            }

            bool result = flush_messenger->call_buffer(destination_thread_id, [](void *data, uint64_t size) {
                process_multiple_calls(data, size);
            }, get_memory(), get_initial_position(), get_size(), call_synchronizer);

            if(!result) {
                return false;
            }

            if(current_position >= BUFFER_REPLACEMENT_PERCENT * PACK_UPPER_LIMIT) {
                memory->sticky = false;

                reset();
            }
            else {
                initial_position = current_position;
            }
        }

        if(perform_shutdown) {
            flush_messenger->shutdown(synchronizer);
        }

        return true;
    }

    inline bool empty_queued_transmissions() noexcept {
        if(empty_queue) {
            return true;
        }

        while(queued_transmissions.size() > 0) {
            AggregationDescriptor &queued_transmission = queued_transmissions.front();

            queued_transmission.memory->operation_timestamp = UINT64_MAX;

            // The queued transmission size can be zero when a flush or shutdown was requested and queued
            if(queued_transmission.size > 0) {
                bool result = flush_messenger->call_buffer(destination_thread_id, [](void *data, uint64_t size) {
                    process_multiple_calls(data, size);
                }, queued_transmission.memory, queued_transmission.offset, queued_transmission.size);

                if(!result) {
                    return false;
                }
            }

            for(Synchronizer *notification_synchronizer: queued_transmission.notification_synchronizers) {
                notification_synchronizer->decrease();
            }

            if(queued_transmission.unstick_memory) {
                queued_transmission.memory->sticky = false;
            }

            if(queued_transmission.perform_shutdown) {
                flush_messenger->shutdown(queued_transmission.synchronizer);
            }
            else if(queued_transmission.synchronizer) {
                queued_transmission.synchronizer->decrease();
            }

            queued_transmissions.pop();
        }

        empty_queue = true;
        return true;
    }

    ///////////////////////
    // Closing functions //
    ///////////////////////

    inline void shutdown(Synchronizer *synchronizer = nullptr) noexcept {
        if(flush_messenger->get_outgoing_shutdown()) {
            if(empty_queue != true || get_size() > 0) {
                cerr << "panic" << endl;
            }
        }

        if(empty_queue == true && get_size() == 0 && flush_messenger->get_outgoing_shutdown()) {
            if(synchronizer) {
                synchronizer->decrease();
            }

            return;
        }

        flush_internal(true, synchronizer);
    }

    ///////////////////////////
    // Transparent functions //
    ///////////////////////////

    template<typename F>
    inline bool call(int destination_thread_id, F &&function, Synchronizer *synchronizer = nullptr) noexcept {
        if(OVERFLOW_MODE) {
            if(empty_queued_transmissions()) {
                if(transmit_buffer(false) && flush_messenger->call(destination_thread_id, function, synchronizer)) {
                    return true;
                }
            }
        }

#ifdef DIRECT_LOCAL_CALLS
        int destination_process_rank = (destination_thread_id / dsys::number_threads_process);

        if(destination_process_rank == process_rank) {
            function();

            if(synchronizer) {
                synchronizer->decrease();
            }

            return true;
        }
#endif /* DIRECT_LOCAL_CALLS */

        if(!pack(destination_thread_id, function, synchronizer)) {
            if(pack_size(function) > PACK_UPPER_LIMIT) {
                cerr << "Error: call does not fit RDMAAggregator's buffer size" << endl;
                exit(EXIT_FAILURE);
            }

            flush_internal(false);

            if(!pack(destination_thread_id, function, synchronizer)) {
#ifdef NO_FAIL_AGGREGATOR
                cerr << "call failed on RDMAAggregator"<< endl;
                exit(EXIT_FAILURE);
#endif /* NO_FAIL_AGGREGATOR */
                return false;
            }
        }

        if constexpr(RECURRENT_FLUSH_SIZE != UINT64_MAX) {
            if(get_size() > RECURRENT_FLUSH_SIZE) {
                flush_internal(false);
            }
        }

        return true;
    }

    template<typename F>
    inline bool call_buffer(int destination_thread_id, F &&function, RDMAMemory *local_memory, uint64_t offset, uint64_t size, Synchronizer *synchronizer = nullptr) noexcept {
        if(OVERFLOW_MODE) {
            if(empty_queued_transmissions()) {
                if(transmit_buffer(false) && flush_messenger->call_buffer(destination_thread_id, function, local_memory, offset, size, synchronizer)) {
                    return true;
                }
            }
        }

#ifdef DIRECT_LOCAL_CALLS
        int destination_process_rank = (destination_thread_id / dsys::number_threads_process);

        if(destination_process_rank == process_rank) {
            function(buffer, size);

            if(synchronizer) {
                synchronizer->decrease();
            }

            return true;
        }
#endif /* DIRECT_LOCAL_CALLS */

        if(!pack_buffer(destination_thread_id, function, local_memory->get_buffer(offset), size, synchronizer)) {
            if(pack_size_buffer(function, size) > PACK_UPPER_LIMIT) {
                cerr << "Error: call does not fit RDMAAggregator's buffer size" << endl;
                exit(EXIT_FAILURE);
            }

            flush_internal(false);

            if(!pack_buffer(destination_thread_id, function, local_memory->get_buffer(offset), size, synchronizer)) {
#ifdef NO_FAIL_AGGREGATOR
                cerr << "call failed on RDMAAggregator"<< endl;
                exit(EXIT_FAILURE);
#endif /* NO_FAIL_AGGREGATOR */
                return false;
            }
        }

        if constexpr(RECURRENT_FLUSH_SIZE != UINT64_MAX) {
            if(get_size() > RECURRENT_FLUSH_SIZE) {
                flush_internal(false);
            }
        }

        return true;
    }

    template<typename F>
    inline bool call_buffer(int destination_thread_id, F &&function, void *data, uint64_t size, Synchronizer *synchronizer = nullptr) noexcept {
        if(OVERFLOW_MODE) {
            if(empty_queued_transmissions()) {
                if(transmit_buffer(false) && flush_messenger->call_buffer(destination_thread_id, function, data, size, synchronizer)) {
                    return true;
                }
            }
        }

#ifdef DIRECT_LOCAL_CALLS
        int destination_process_rank = (destination_thread_id / dsys::number_threads_process);

        if(destination_process_rank == process_rank) {
            function(buffer, size);

            if(synchronizer) {
                synchronizer->decrease();
            }

            return true;
        }
#endif /* DIRECT_LOCAL_CALLS */

        if(!pack_buffer(destination_thread_id, function, data, size, synchronizer)) {
            if(pack_size_buffer(function, size) > PACK_UPPER_LIMIT) {
                cerr << "Error: call does not fit RDMAAggregator's buffer size" << endl;
                exit(EXIT_FAILURE);
            }

            flush_internal(false);

            if(!pack_buffer(destination_thread_id, function, data, size, synchronizer)) {
#ifdef NO_FAIL_AGGREGATOR
                cerr << "call failed on RDMAAggregator"<< endl;
                exit(EXIT_FAILURE);
#endif /* NO_FAIL_AGGREGATOR */
                return false;
            }
        }

        if constexpr(RECURRENT_FLUSH_SIZE != UINT64_MAX) {
            if(get_size() > RECURRENT_FLUSH_SIZE) {
                flush_internal(false);
            }
        }

        return true;
    }
};

template<typename Messenger, typename Allocator>
class RDMATimedAggregator: public RDMAAggregator<Messenger, Allocator> {
protected:
    mutex flush_lock;

    bool sleeper_done = false;
    thread *sleeper = nullptr;

    using RDMAAggregator<Messenger, Allocator>::destination_thread_id;

public:
    RDMATimedAggregator(Messenger *messenger, int destination_thread_id): RDMAAggregator<Messenger, Allocator>{messenger, destination_thread_id} {
    }

    virtual ~RDMATimedAggregator() {
        sleeper_done = true;

        if(sleeper) {
            sleeper->join();

            delete sleeper;
        }
    }

    inline void start_timer(uint64_t microseconds = 1000) noexcept {
        sleeper = new thread([this, microseconds] {
            init_thread(-1);

            while(true) {
                usleep(microseconds);

                flush();

                if(sleeper_done) {
                    break;
                }
            }

            finalize_thread();
        });
    }

    template<typename F>
    inline bool call(int destination_thread_id, F &&function, Synchronizer *synchronizer = nullptr) noexcept {
        lock_guard holder{flush_lock};

        return RDMAAggregator<Messenger, Allocator>::call(destination_thread_id, function, synchronizer);
    }

    template<typename F>
    inline bool call_buffer(int destination_thread_id, F &&function, RDMAMemory *local_memory, uint64_t offset, uint64_t size, Synchronizer *synchronizer = nullptr) noexcept {
        lock_guard holder{flush_lock};

        return RDMAAggregator<Messenger, Allocator>::call_buffer(destination_thread_id, function, local_memory, offset, size, synchronizer);
    }

    template<typename F>
    inline bool call_buffer(int destination_thread_id, F &&function, void *data, uint64_t size, Synchronizer *synchronizer = nullptr) noexcept {
        lock_guard holder{flush_lock};

        return RDMAAggregator<Messenger, Allocator>::call_buffer(destination_thread_id, function, data, size, synchronizer);
    }

    inline void flush(Synchronizer *synchronizer = nullptr) noexcept {
        lock_guard holder{flush_lock};

        RDMAAggregator<Messenger, Allocator>::flush(synchronizer);
    }
};

template<typename Messenger, typename ChildAggregator = RDMAAggregator<ChunkMemoryAllocator, typename Messenger::ChildMessengerType>>
class RDMAAggregatorGlobal {
public:
    using MessengerType = Messenger;
    using ChildAggregatorType = ChildAggregator;
    using ChildMessengerType = typename Messenger::ChildMessengerType;

protected:
    Messenger *flush_messenger;

    vector<ChildAggregator *> aggregators;

public:
    template<typename... Args>
    RDMAAggregatorGlobal(Messenger *flush_messenger, Args &&... args): flush_messenger{flush_messenger} {
        aggregators.resize(dsys::number_threads);

        for(int i = 0; i < dsys::number_threads; i++) {
            aggregators[i] = new ChildAggregator(new ChunkMemoryAllocator(thread_context->global_allocator, 1), flush_messenger->get_messenger(i), i, std::forward<Args>(args)...);
        }
    }

    virtual ~RDMAAggregatorGlobal() {
        for(int i = 0; i < dsys::number_threads; i++) {
            delete aggregators[i];
        }
    }

    ///////////////////////
    // Packing functions //
    ///////////////////////

    template<typename F>
    inline bool pack(int destination_thread_id, F &&function, Synchronizer *synchronizer = nullptr) noexcept {
        return aggregators[destination_thread_id]->pack(destination_thread_id, function, synchronizer);
    }

    //////////////////////////////////
    // Flush and shutdown functions //
    //////////////////////////////////

    inline bool flush(int destination_thread_id, Synchronizer *synchronizer = nullptr) noexcept {
        return aggregators[destination_thread_id]->flush(synchronizer);
    }

    inline bool flush_all(Synchronizer *synchronizer = nullptr) noexcept {
        bool result = true;

        for(int destination_thread_id = 0; destination_thread_id < dsys::number_threads; destination_thread_id++) {
            result &= aggregators[destination_thread_id]->flush(synchronizer);
        }

        return result;
    }

    inline void shutdown(int destination_thread_id, Synchronizer *synchronizer = nullptr) noexcept {
        aggregators[destination_thread_id]->shutdown(synchronizer);
    }

    inline void shutdown_all(Synchronizer *synchronizer = nullptr) noexcept {
        for(int destination_thread_id = 0; destination_thread_id < dsys::number_threads; destination_thread_id++) {
            shutdown(destination_thread_id, synchronizer);
        }
    }

    ////////////////////////
    // Accessor functions //
    ////////////////////////

    inline void reset(int destination_thread_id) noexcept {
        aggregators[destination_thread_id]->reset();
    }

    inline void reset_all() noexcept {
        for(int destination_thread_id = 0; destination_thread_id < dsys::number_threads; destination_thread_id++) {
            reset(destination_thread_id);
        }
    }

    inline ChildAggregator *get_child_aggregator(int destination_thread_id) const noexcept {
        return aggregators[destination_thread_id];
    }

    inline Messenger *get_messenger() const noexcept {
        return flush_messenger;
    }

    ///////////////////////////
    // Transparent functions //
    ///////////////////////////

    template<typename F>
    inline bool call(int destination_thread_id, F &&function, Synchronizer *synchronizer = nullptr) noexcept {
        return aggregators[destination_thread_id]->call(destination_thread_id, function, synchronizer);
    }

    template<typename F>
    inline bool call_buffer(int destination_thread_id, F &&function, RDMAMemory *local_memory, uint64_t offset, uint64_t size, Synchronizer *synchronizer = nullptr) noexcept {
        return aggregators[destination_thread_id]->call_buffer(destination_thread_id, function, local_memory, offset, size, synchronizer);
    }

    template<typename F>
    inline bool call_buffer(int destination_thread_id, F &&function, void *data, uint64_t size, Synchronizer *synchronizer = nullptr) noexcept {
        return aggregators[destination_thread_id]->call_buffer(destination_thread_id, function, data, size, synchronizer);
    }
};

template<typename Messenger, typename ChildAggregator = RDMAAggregator<Messenger, typename Messenger::AllocatorType>>
class RDMATimedAggregatorGlobal: public RDMAAggregatorGlobal<Messenger, ChildAggregator> {
public:
    bool sleeper_done = false;
    thread *sleeper = nullptr;

public:
    template<typename... Args>
    RDMATimedAggregatorGlobal(Messenger *messenger, Args &&... args): RDMAAggregatorGlobal<Messenger, ChildAggregator>{messenger, std::forward<Args>(args)...} {
    }

    virtual ~RDMATimedAggregatorGlobal() {
        sleeper_done = true;

        if(sleeper) {
            sleeper->join();

            delete sleeper;
        }
    }

    inline void start_timer(uint64_t microseconds = 1000) noexcept {
        sleeper = new thread([this, microseconds] {
            init_thread(-1);

            while(true) {
                usleep(microseconds);

                for(int i = 0; i < dsys::number_threads; i++) {
                    RDMAAggregatorGlobal<Messenger, ChildAggregator>::aggregators[i].flush();
                }

                if(sleeper_done) {
                    break;
                }
            }

            finalize_thread();
        });
    }
};

} // namespace dsys

#endif /* RDMA_AGGREGATOR_H */
