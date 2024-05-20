/*
 * Copyright (c) 2024, Hammurabi Mendes. All rights reserved.

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

#ifndef MCTS_NODE
#define MCTS_NODE

#include <stdint.h>
#include <cassert>

#include "thread_handler.h"
#include "remote_calls.hpp"
#include "rdma_aggregators.hpp"

#include "utils/Random.hpp"

using dsys::RDMAMessengerGlobal;
using dsys::RDMAAggregatorGlobal;

#ifdef STATS
extern thread_local uint64_t thread_total_visits;
extern thread_local uint64_t thread_total_simulations;
#endif /* STATS */

struct alignas(64) SimulationQueues {
    FastQueuePC<void *> incoming[32];

#ifndef NO_DONATE_SIMULATIONS
    atomic<bool> want_donation{false};
#endif /* !NO_DONATE_SIMULATIONS */
};

template<class S>
extern thread_local NUMAAllocator<S> *node_allocator;
template<class S>
extern thread_local NUMAAllocator<S> *hex_allocator;

#ifndef NO_COMBINING
template<class SimulationReport>
extern thread_local unordered_map<void *, SimulationReport> combiner;
#endif /* NO_COMBINING */

class MCTS {
public:
    static uint64_t LEVELS_REMAINING_MAX;

    static thread_local RDMAMessengerGlobal<> *global_messenger;
    static thread_local RDMAAggregatorGlobal<RDMAMessengerGlobal<>> *global_aggregator;

#ifndef NO_SIMULATION_QUEUES
    static SimulationQueues *simulation_queues;
#endif /* !NO_SIMULATION_QUEUES */

    static void init_thread() {
        global_messenger = new RDMAMessengerGlobal<>();
        global_aggregator = new RDMAAggregatorGlobal<RDMAMessengerGlobal<>>(global_messenger);
    }

    static void finalize_thread() {
        if(global_aggregator) {
            delete global_aggregator;
        }

        if(global_messenger) {
            delete global_messenger;
        }
    }
};

uint64_t MCTS::LEVELS_REMAINING_MAX = 0;
#ifndef NO_SIMULATION_QUEUES
SimulationQueues *MCTS::simulation_queues;
#endif /* !NO_SIMULATION_QUEUES */ 

thread_local RDMAMessengerGlobal<> *MCTS::global_messenger = nullptr;
thread_local RDMAAggregatorGlobal<RDMAMessengerGlobal<>> *MCTS::global_aggregator = nullptr;

template<class S, uint64_t CHILDREN_SIZE>
class MCTS_Node {
public:
    using Node = MCTS_Node<S, CHILDREN_SIZE>;
    using NodeAddress = GlobalAddress<Node>;

    static constexpr float UCB_constant = 1.0;

    static constexpr uint64_t NUMBER_SIMULATIONS = S::NUMBER_SIMULATIONS;

    S *state;

    NodeAddress self;

    atomic<NodeAddress *> parent;
    NodeAddress parent_storage;

    uint64_t parent_move;

    // atomic<uint64_t> levels_remaining;
    const uint64_t levels_remaining;

    atomic<uint64_t> visits{0};
    atomic<uint64_t> wins{0};

    atomic<uint64_t> backpropagation_visits{0};

    alignas(128) struct ChildInformation {
        atomic<NodeAddress *> address{nullptr};
        NodeAddress storage;

        atomic<uint64_t> visits{0};
        atomic<uint64_t> wins{0};

        atomic<uint64_t> deferred_selections{0};

        atomic<int> owning_thread{INT_MAX};
    } children_information[CHILDREN_SIZE];

    atomic<bool> expansion_done{false};

    atomic<uint64_t> current_round_visits = 0;

    template<typename... Args>
    MCTS_Node(const NodeAddress &parent_reference, uint64_t parent_move, uint64_t levels_remaining, Args &&... arguments): self{this}, parent{&parent_storage}, parent_storage{parent_reference}, parent_move{parent_move}, levels_remaining{levels_remaining} {
#ifdef NUMA_ALLOCATOR
        state = new(hex_allocator<S>->allocate()) S(std::forward<Args>(arguments)...);
#else
        state = new S(std::forward<Args>(arguments)...);
#endif /* NUMA_ALLOCATOR */
    }

    template<typename... Args>
    MCTS_Node(uint64_t levels_remaining, Args &&... arguments): self{this}, parent{&parent_storage}, parent_storage{nullptr}, parent_move{0}, levels_remaining{levels_remaining} {
#ifdef NUMA_ALLOCATOR
        state = new(hex_allocator<S>->allocate()) S(std::forward<Args>(arguments)...);
#else
        state = new S(std::forward<Args>(arguments)...);
#endif /* NUMA_ALLOCATOR */
    }

    ~MCTS_Node() {
#ifdef NUMA_ALLOCATOR
        state->~S();
        hex_allocator<S>->deallocate(state, thread_id);
#else
        delete state;
#endif /* NUMA_ALLOCATOR */
    }

    inline void set_root() noexcept {
        parent_storage = nullptr;
        parent.store(&parent_storage, std::memory_order_relaxed);
    }

    inline bool is_root() const noexcept {
        return (parent.load(std::memory_order_relaxed)->get_address() == nullptr);
    }

    inline void set_levels_remaining(uint64_t new_levels_remaining) noexcept {
        if(expansion_done.load(std::memory_order_relaxed)) {
            return;
        }

        levels_remaining = new_levels_remaining;
    }

    inline void set_levels_remaining_descendants(uint64_t new_levels_remaining) noexcept {
        set_levels_remaining(new_levels_remaining);

        uint64_t start_index = Random<uint64_t>::getRandom(0, CHILDREN_SIZE - 1);

        for(uint64_t i = 0; i < CHILDREN_SIZE; i++) {
            uint64_t move = (start_index + i) % CHILDREN_SIZE;

            if(children_information[move].address == nullptr) {
                continue;
            }

            if(dsys::get_process_rank(get_thread(move)) == process_rank) {
                get_child(move)->set_levels_remaining_descendants(new_levels_remaining);
            }
        }
    }

    // inline void setup_reclamation(uint64_t retained_move) {
    //     reclamation_retained_move = retained_move;
    // }

    // inline void attempt_reclamation() {
    //     uint64_t observed = reclamation_retained_move;
    //     uint64_t snapshot_observed = observed;

    //     if(observed != UINT64_MAX) {
    //         if(reclamation_retained_move.compare_exchange_strong(observed, UINT64_MAX)) {
    //             MCTS::global_aggregator->call(self.get_thread_id(), [old_root = self, old_root_best_move = snapshot_observed] {
    //                 old_root->delete_unwanted_paths(old_root_best_move);
    //                 old_root.get_address()->~MCTS_Node();
    //                 node_allocator<Node>->deallocate(old_root.get_address(), thread_id);
    //                 // delete old_root.get_address();
    //             });
    //         }
    //     }
    // }

    inline void delete_unwanted_paths(uint64_t retained_move) noexcept {
        uint64_t start_index = Random<uint64_t>::getRandom(0, CHILDREN_SIZE - 1);

        for(uint64_t i = 0; i < CHILDREN_SIZE; i++) {
            uint64_t move = (start_index + i) % CHILDREN_SIZE;

            if(move == retained_move) {
                continue;
            }

            if(children_information[move].address.load(std::memory_order_relaxed) == nullptr) {
                continue;
            }

            if(get_thread(move) == thread_id) {
                get_child(move)->delete_unwanted_paths(UINT64_MAX);

#ifdef NUMA_ALLOCATOR
                get_child(move).get_address()->~MCTS_Node();
                node_allocator<Node>->deallocate(get_child(move).get_address(), thread_id);
#else
                delete get_child(move).get_address();
#endif /* NUMA_ALLOCATOR */
            }
            else {
                MCTS::global_aggregator->call(get_thread(move), [child = get_child(move)] {
                    child->delete_unwanted_paths(UINT64_MAX);

#ifdef NUMA_ALLOCATOR
                    child.get_address()->~MCTS_Node();
                    node_allocator<Node>->deallocate(child.get_address(), thread_id);
#else
                    delete child.get_address();
#endif /* NUMA_ALLOCATOR */
                });
            }
        }
    }

    NodeAddress &get_parent() const noexcept {
        return *(parent.load(std::memory_order_relaxed));
    }

    NodeAddress &get_child(uint64_t move) const noexcept {
        return *(children_information[move].address.load(std::memory_order_relaxed));
    }

    void set_child(uint64_t move, const NodeAddress &child) noexcept {
        children_information[move].storage = child;
        children_information[move].address.store(&children_information[move].storage, std::memory_order_relaxed);
    }

    const NodeAddress &get_remote_address() const noexcept {
        return self;
    }

    uint64_t get_thread(uint64_t move) {
        int observed = children_information[move].owning_thread.load(std::memory_order_relaxed);

        if(observed != INT_MAX) {
            return observed;
        }

        int proposed = state->get_thread(move) % dsys::number_threads;

        if(children_information[move].owning_thread.compare_exchange_strong(observed, proposed, std::memory_order_relaxed, std::memory_order_relaxed)) {
            return proposed;
        }
        else {
            // "observed" has been updated by the CAS call
            return observed;
        }
    }

    uint64_t get_process(uint64_t move) {
        return dsys::get_process_rank(get_thread(move));
    }

    uint64_t attempt_expansion(uint64_t number_visits) {
        if(expansion_done.load(std::memory_order_relaxed)) {
            return UINT64_MAX;
        }

        uint64_t start_index = Random<uint64_t>::getRandom(0, CHILDREN_SIZE - 1);

        for(uint64_t i = 0; i < CHILDREN_SIZE; i++) {
            uint64_t position = (start_index + i) % CHILDREN_SIZE;

            // TODO HM: possible improvment if we make it per thread
            if(state->can_expand(position) && children_information[position].visits.load(std::memory_order_relaxed) == 0) {
                uint64_t expected = 0;
                uint64_t replacing = number_visits;

                if(children_information[position].visits.compare_exchange_strong(expected, replacing, std::memory_order_relaxed, std::memory_order_relaxed)) {
                    state->notify_expanded(position, number_visits);
                    return position;
                }
            }
        }

        if(!expansion_done.load(std::memory_order_relaxed)) {
            expansion_done.store(true, std::memory_order_relaxed);
        }

        return UINT64_MAX;
    }

    template<typename SimulationReport>
    void update(SimulationReport &&simulation_report) {
        if(is_root()) {
            backpropagation_visits.fetch_add(simulation_report.get_number_simulations(), std::memory_order_relaxed);
        }

        wins.fetch_add(state->get_wins(simulation_report), std::memory_order_relaxed);
        state->notify_update(simulation_report);
    }

    template<typename SimulationReport>
    void update_child(uint64_t move, SimulationReport &&simulation_report) {
        if(is_root()) {
            backpropagation_visits.fetch_add(simulation_report.get_number_simulations(), std::memory_order_relaxed);
        }

        wins.fetch_add(state->get_wins(simulation_report), std::memory_order_relaxed);
        state->notify_update(simulation_report);

        children_information[move].wins.fetch_add(state->get_wins(move, simulation_report), std::memory_order_relaxed);
        state->notify_update_child(move, simulation_report);
    }

    NodeAddress &get_best_child() const noexcept {
        return get_child(get_best_move());
    }

    NodeAddress &get_best_child(uint64_t &best_move) const noexcept {
        best_move = get_best_move();

        return get_child(best_move);
    }

    uint64_t get_best_move() const noexcept {
        uint64_t most_visits = 0;
        uint64_t most_visited[CHILDREN_SIZE];
        int most_visited_size = 0;

        do {
            int starting_point = thread_id * std::min(1UL, CHILDREN_SIZE / number_threads);

            for(int i = 0; i < CHILDREN_SIZE; i++) {
                int position = (starting_point + i) % CHILDREN_SIZE;

                // TODO HM: possible improvment if we make it per thread
                uint64_t visits_snapshot = children_information[position].visits.load(std::memory_order_relaxed);

                if(!state->can_expand(position) || visits_snapshot == 0 || children_information[position].address.load(std::memory_order_relaxed) == nullptr) {
                    continue;
                }

                if(visits_snapshot > most_visits) {
                    most_visited_size = 0;

                    most_visited[most_visited_size++] = position;
                    most_visits = visits_snapshot;
                }
                else if(visits_snapshot == most_visits) {
                    most_visited[most_visited_size++] = position;
                }
            }
        } while(most_visited_size == 0);

        // Then break ties by number of wins

        // TODO HM: possible improvment if we make it per thread
        uint64_t most_wins = children_information[most_visited[0]].wins.load(std::memory_order_relaxed);

        uint64_t biggest_winner[CHILDREN_SIZE];
        int biggest_winner_size = 0;

        biggest_winner[0] = most_visited[0];
        biggest_winner_size++;

        for(uint64_t i = 1; i < most_visited_size; i++) {
            // TODO HM: possible improvment if we make it per thread
            uint64_t wins_snapshot = children_information[most_visited[i]].wins.load(std::memory_order_relaxed);

            if(wins_snapshot > most_wins) {
                biggest_winner_size = 0;

                biggest_winner[biggest_winner_size++] = most_visited[i];
                most_wins = wins_snapshot;
            }
            else if(wins_snapshot == most_wins) {
                biggest_winner[biggest_winner_size++] = most_visited[i];
            }
        }

        uint64_t position = Random<uint64_t>::getRandom(0, biggest_winner_size - 1);
        return biggest_winner[position];
    }

    void visit() {
        // assert(thread_id >= 4);
        visits.fetch_add(NUMBER_SIMULATIONS, std::memory_order_relaxed);
#ifdef STATS
        thread_total_visits += NUMBER_SIMULATIONS;
#endif /* STATS */
    }

    template<typename SimulationReport>
    void perform_backpropagation(SimulationReport &&simulation_report) {
        if(get_parent().get_address() == nullptr) {
            return;
        }

        if(dsys::get_process_rank(get_parent().get_thread_id()) == process_rank) {
        // NOTE: Alternative
        // if(get_parent().get_thread_id() == thread_id) {
            get_parent()->update_child(parent_move, simulation_report);
#ifndef NO_COMBINING
            get_parent()->perform_combined_backpropagation(simulation_report);
#else
            get_parent()->perform_backpropagation(simulation_report);
#endif /* NO_COMBINING */
        }
        else {
            MCTS::global_aggregator->call(get_parent().get_thread_id(), [parent_address = get_parent().get_address(), parent_move = this->parent_move, simulation_report] {
                parent_address->update_child(parent_move, simulation_report);
#ifndef NO_COMBINING
                parent_address->perform_combined_backpropagation(simulation_report);
#else
                parent_address->perform_backpropagation(simulation_report);
#endif /* NO_COMBINING */
            });
        }
    }

    template<typename SimulationReport>
    void perform_combined_backpropagation(const SimulationReport &simulation_report) {
        using HexSimulationReport = typename std::remove_reference<SimulationReport>::type;

        auto &&result = combiner<HexSimulationReport>.emplace(std::make_pair(reinterpret_cast<void *>(this), simulation_report));

        if(result.second == false) {
            auto &&previous = result.first;

            (previous->second).add_number_simulations(simulation_report.get_number_simulations());
            (previous->second).add_p1_wins(simulation_report.get_p1_wins());
        }
    }

    void perform_simulation_backpropagation() {
        auto &&simulation_report = state->simulate(NUMBER_SIMULATIONS);

#ifdef STATS
        thread_total_simulations += NUMBER_SIMULATIONS;
#endif /* STATS */

        update(simulation_report);

#ifndef NO_COMBINING
        perform_combined_backpropagation(simulation_report);
#else
        perform_backpropagation(simulation_report);
#endif /* NO_COMBINING */
    }

    void perform_expansion_simulation_backpropagation(uint64_t move) {
        if(get_thread(move) == thread_id) {
#ifdef NUMA_ALLOCATOR
            Node *child = new(node_allocator<Node>->allocate()) Node(get_remote_address(), move, (levels_remaining > 0 ? levels_remaining - 1 : MCTS::LEVELS_REMAINING_MAX), state, move);
#else
            Node *child = new Node(get_remote_address(), move, (levels_remaining > 0 ? levels_remaining - 1 : MCTS::LEVELS_REMAINING_MAX), state, move);
#endif /* NUMA_ALLOCATOR */

            set_child(move, child->get_remote_address());

            // Check for deferred selections
            move_down(move, 0);

            // child->visit();
#ifndef NO_SIMULATION_QUEUES
            MCTS::simulation_queues[Random<int>::getRandom(0, number_threads_process - 1)].incoming[thread_rank].push((void *) child);
#else
            child->perform_simulation_backpropagation();
#endif /* !NO_SIMULATION_QUEUES */
        }
        else {
            MCTS::global_aggregator->call_buffer(get_thread(move), [parent = get_remote_address(), parent_move = move](void *state, uint64_t state_size) {
#ifdef NUMA_ALLOCATOR
                Node *child = new(node_allocator<Node>->allocate()) Node(parent, parent_move, MCTS::LEVELS_REMAINING_MAX, state, state_size, parent_move);
#else
                Node *child = new Node(parent, parent_move, MCTS::LEVELS_REMAINING_MAX, state, state_size, parent_move);
#endif /* NUMA_ALLOCATOR */

                MCTS::global_aggregator->call(parent.get_thread_id(), [parent_address = parent.get_address(), child_move = parent_move, child = child->get_remote_address()] {
                    parent_address->set_child(child_move, child);

                    // Check for deferred selections
                    parent_address->move_down(child_move, 0);
                });

                // child->visit();
#ifndef NO_SIMULATION_QUEUES
                MCTS::simulation_queues[Random<int>::getRandom(0, number_threads_process - 1)].incoming[thread_rank].push((void *) child);
#else
                child->perform_simulation_backpropagation();
#endif /* !NO_SIMULATION_QUEUES */
            }, state->get_serialized(), 0, state->get_serialized_length());
        }
    }

    inline float UCB(uint64_t move) noexcept {
        // TODO HM: possible improvment if we make it per thread
        uint64_t child_visits_snapshot = children_information[move].visits.load(std::memory_order_relaxed);

        // TODO HM: possible improvment if we make it per thread
        float exploit = ((float) children_information[move].wins.load(std::memory_order_relaxed)) / child_visits_snapshot;
        float explore = std::sqrt(std::log((float) visits.load(std::memory_order_relaxed)) / child_visits_snapshot);

        return exploit + UCB_constant * explore;
    }

    inline float UCB_explore_precalculation(uint64_t move, float explore_precalculation) noexcept {
        // TODO HM: possible improvment if we make it per thread
        uint64_t child_visits_snapshot = children_information[move].visits.load(std::memory_order_relaxed);

        // TODO HM: possible improvment if we make it per thread
        float exploit = ((float) children_information[move].wins.load(std::memory_order_relaxed)) / child_visits_snapshot;
        float explore = std::sqrt(explore_precalculation / child_visits_snapshot);

        return exploit + UCB_constant * explore;
    }

    uint64_t select() {
        uint64_t best_moves[CHILDREN_SIZE];
        int best_moves_size = 0;

        float best_weight;

        int starting_point = thread_id * std::min(1UL, CHILDREN_SIZE / number_threads);

        float explore_precalculation = std::log((float) visits.load(std::memory_order_relaxed));

        for(int i = 0; i < CHILDREN_SIZE; i++) {
            int position = (starting_point + i) % CHILDREN_SIZE;

            if(!state->can_expand(position)) {
                continue;
            }

            float weight = UCB_explore_precalculation(position, explore_precalculation);

            if(best_moves_size == 0) {
                best_moves[best_moves_size++] = position;

                best_weight = weight;
            }
            else if(weight == best_weight) {
                best_moves[best_moves_size++] = position;
            }
            else if(weight > best_weight) {
                best_moves_size = 0;
                best_moves[best_moves_size++] = position;

                best_weight = weight;
            }
        }

        uint64_t position = Random<uint64_t>::getRandom(0, best_moves_size - 1);
        uint64_t move = best_moves[position];

        state->notify_selected(move);

        return move;
    }

    void perform_selection(uint64_t number_rollouts = 1) {
        for(uint64_t i = 0; i < number_rollouts; i++) {
            visit();

            // Already an exhausted state: disconsider expansion
            if(state->is_exhausted() || state->has_winner()) {
#ifndef NO_SIMULATION_QUEUES
                MCTS::simulation_queues[Random<int>::getRandom(0, number_threads_process - 1)].incoming[thread_rank].push((void *) this);
#else
                perform_simulation_backpropagation();
#endif /* NO_SIMULATION_QUEUES */
                continue;
            }

            // Not expanded:
            //     1) Try to expand
            //     2) If the expansion is successful, simulate and backpropagate
            if(!expansion_done.load(std::memory_order_relaxed)) {
                uint64_t move = attempt_expansion(NUMBER_SIMULATIONS);

                if(move != UINT64_MAX) {
                    perform_expansion_simulation_backpropagation(move);
                    continue;
                }
            }

            uint64_t move = select();

            move_down(move);
        }
    }

    uint64_t obtain_deferred_selections(uint64_t move) {
        uint64_t backlog = children_information[move].deferred_selections;

        while(backlog > 0) {
            if(children_information[move].deferred_selections.compare_exchange_strong(backlog, 0, std::memory_order_relaxed, std::memory_order_relaxed)) {
                return backlog;
            }
        }

        return 0;
    }

    void move_down(uint64_t move, uint64_t number_selections = 1) {
        // If we have an unexpanded child, defer the selection
        if(children_information[move].address.load(std::memory_order_relaxed) == nullptr) {
            children_information[move].deferred_selections.fetch_add(number_selections, std::memory_order_relaxed);

            if(children_information[move].address.load(std::memory_order_relaxed) == nullptr) {
                return;
            }

            // Already transferred number_selections to deferred_selections[move]
            number_selections = 0;
        }

        NodeAddress &child = get_child(move);

        // Get the backlog of pending selections

        uint64_t total_number_selections = obtain_deferred_selections(move);

        // Account for the requested number of selections

        total_number_selections += number_selections;

        if(total_number_selections == 0) {
            return;
        }

        // Update child's visit counter

        // TODO HM: possible improvment if we make it per thread
        children_information[move].visits.fetch_add(NUMBER_SIMULATIONS * total_number_selections, std::memory_order_relaxed);

        // Visit child

        if(dsys::get_process_rank(get_thread(move)) == process_rank) {
        // NOTE: Alternative
        // if(get_thread(move) == thread_id) {
            child->perform_selection(total_number_selections);
        }
        else {
            MCTS::global_aggregator->call(child.get_thread_id(), [child_address = child.get_address(), total_number_selections] {
                child_address->perform_selection(total_number_selections);
            });
        }
    }
};

#endif /* MCTS_NODE */