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

#include <iostream>
#include <unistd.h>

#include <vector>
#include <array>
#include <unordered_map>

#include <algorithm>

#include <thread>
#include <future>
#include <mutex>

#include <cstring>
#include <iomanip>

#ifdef DEBUG
#include <cassert>
#endif /* DEBUG */

#include "seriema.h"
#include "Allocator.hpp"

using std::vector;
using std::array;

using std::thread;
using std::async;
using std::recursive_mutex;
using std::lock_guard;

using std::cout;
using std::cerr;
using std::endl;

using seriema::GlobalAddress;

using seriema::number_processes;
using seriema::process_rank;
using seriema::number_threads;
using seriema::number_threads_process;
using seriema::local_number_processes;
using seriema::local_process_rank;
using seriema::thread_rank;
using seriema::thread_id;

using seriema::context;
using seriema::queue_pairs;

using seriema::incoming_message_queues;

using seriema::Configuration;

#ifdef STATS
#include <MPI/MPIHelper.hpp>
#endif /* STATS */
#ifdef PROFILE
#include <gperftools/profiler.h>
#endif /* PROFILE */

#include "MCTS_Node.hpp"

class Hex {
public:
    struct SimulationReport: std::pair<uint32_t, uint32_t> {
        SimulationReport(uint32_t number_simulations, uint32_t p1_wins): std::pair<uint32_t, uint32_t>{number_simulations, p1_wins} {}
        SimulationReport(const SimulationReport &other): SimulationReport{other.get_number_simulations(), other.get_p1_wins()} {}
        SimulationReport(SimulationReport &&other): SimulationReport{other.get_number_simulations(), other.get_p1_wins()} {}

        inline void add_number_simulations(uint32_t number_simulations) noexcept {
            first += number_simulations;
        }

        inline void add_p1_wins(uint32_t p1_wins) noexcept {
            second += p1_wins;
        }

        inline uint32_t get_number_simulations() const noexcept {
            return first;
        }

        inline uint32_t get_p1_wins() const noexcept {
            return second;
        }
    };

    using PlayRepresentation = uint8_t;

    constexpr static uint64_t BOARD_HW = 11;
    constexpr static uint64_t BOARD_SIZE = BOARD_HW * BOARD_HW;

    RDMAMemory *board_memory;
    char *board;

    RDMAMemory *plays_memory;
    PlayRepresentation *plays;
    uint64_t number_plays;

    int winner;
    int turn;

    static constexpr uint64_t NUMBER_SIMULATIONS = 16;

    void initialize_board_root() {
        board_memory = seriema::thread_context->linear_allocator->allocate(BOARD_SIZE);
        board = reinterpret_cast<char *>(board_memory->get_buffer());

        std::memset(board, 0, BOARD_SIZE * sizeof(char));

        turn = 1;

        plays_memory = seriema::thread_context->linear_allocator->allocate(1 * sizeof(PlayRepresentation));
        plays = reinterpret_cast<PlayRepresentation *>(plays_memory->get_buffer());

        // Serialized version
        plays[0] = number_plays;

        plays += 1;
    }

    void initialize_board(PlayRepresentation *previous_plays, uint64_t number_previous_plays) {
        board_memory = seriema::thread_context->linear_allocator->allocate(BOARD_SIZE);
        board = reinterpret_cast<char *>(board_memory->get_buffer());

        std::memset(board, 0, BOARD_SIZE * sizeof(char));

        turn = 1;

        for(int i = 0; i < number_previous_plays; i++) {
            board[previous_plays[i]] = turn;

            turn = -turn;
        }

        // Allocates space for the serialized version
        plays_memory = seriema::thread_context->linear_allocator->allocate((number_previous_plays + 1 + 1) * sizeof(PlayRepresentation));
        plays = reinterpret_cast<PlayRepresentation *>(plays_memory->get_buffer());

        // Serialized version
        plays[0] = number_plays;

        plays += 1;

        std::memcpy(plays, previous_plays, number_previous_plays * sizeof(uint64_t));
    }

    Hex(): number_plays{0} {
        initialize_board_root();

        winner = player_has_won(board, turn) ? turn : 0;
    }

    Hex(Hex *parent, uint64_t parent_move): number_plays{parent->number_plays + 1} {
        initialize_board(parent->plays, parent->number_plays);

        board[parent_move] = turn;
        plays[number_plays - 1] = parent_move;

        winner = player_has_won(board, turn) ? turn : 0;
    }

    Hex(void *serialized, uint64_t serialized_length, uint64_t parent_move) {
        PlayRepresentation *previous_plays = reinterpret_cast<PlayRepresentation *>(serialized) + 1;
        PlayRepresentation number_previous_plays = *(reinterpret_cast<PlayRepresentation *>(serialized));

        number_plays = number_previous_plays + 1;

        initialize_board(previous_plays, number_previous_plays);

        board[parent_move] = turn;
        plays[number_plays - 1] = parent_move;

        winner = player_has_won(board, turn) ? turn : 0;
    }

    static void init_thread() {
    }

    static void finalize_thread() {
    }

    RDMAMemory *get_serialized() {
        return plays_memory;
    }

    uint64_t get_serialized_length() {
        return (number_plays + 1) * sizeof(PlayRepresentation);
    }

    virtual ~Hex() {
        seriema::thread_context->linear_allocator->deallocate(board_memory);
        seriema::thread_context->linear_allocator->deallocate(plays_memory);
    }

    void notify_expanded(uint64_t move, uint64_t number_visits) {
    }

    void notify_selected(uint64_t move) {
    }

    void notify_update(const SimulationReport &simulation_report) {
    }

    void notify_update_child(uint64_t move, const SimulationReport &simulation_report) {
    }

    uint64_t get_wins(const SimulationReport &simulation_report) {
        if(simulation_report.get_p1_wins() == UINT8_MAX) {
            return 0;
        }

        if(turn == 1) {
            return simulation_report.get_p1_wins();
        }
        else {
            return simulation_report.get_number_simulations() - simulation_report.get_p1_wins();
        }
    }

    uint64_t get_wins(uint64_t move, const SimulationReport &simulation_report) {
        if(simulation_report.get_p1_wins() == UINT8_MAX) {
            return 0;
        }

        // Reverse logic from the current node
        if(turn == -1) {
            return simulation_report.get_p1_wins();
        }
        else {
            return simulation_report.get_number_simulations() - simulation_report.get_p1_wins();
        }
    }

    SimulationReport simulate(uint64_t number_simulations) {
        static thread_local std::mt19937_64 engine(thread_id);

        if(has_winner()) {
            return SimulationReport(number_simulations, turn == 1 ? number_simulations : 0);
        }

        if(is_exhausted()) {
            return SimulationReport(number_simulations, UINT8_MAX);
        }

        char working_board[BOARD_SIZE];
        std::memset(working_board, 0, BOARD_SIZE * sizeof(char));

        array<uint64_t, 128> unexpanded_moves;
        int unexpanded_moves_size = 0;

        for(uint64_t position = 0; position < BOARD_SIZE; position++) {
            working_board[position] = board[position];

            if(can_expand(position)) {
                unexpanded_moves[unexpanded_moves_size++] = position;
            }
        }

        uint8_t p1_wins = 0;

        for(uint64_t simulation = 0; simulation < number_simulations; simulation++) {
            std::shuffle(unexpanded_moves.begin(), unexpanded_moves.begin() + unexpanded_moves_size, engine);

            for(uint64_t i = 0; i < unexpanded_moves_size / 2; i++) {
                working_board[unexpanded_moves[i]] = 1;
            }
            for(uint64_t i = unexpanded_moves_size / 2; i < unexpanded_moves_size; i++) {
                working_board[unexpanded_moves[i]] = -1;
            }

            if(player_has_won(working_board, 1)) {
                p1_wins++;
            }
        }

        return SimulationReport(number_simulations, p1_wins);
    }

    bool player_has_won(const char *board, int player) {
        // Life is easier if we always work from player +1's perspective, so we
        // transpose the array and multiply by -1 to switch player perspectives.

        char working_board[BOARD_SIZE];

        if(player == -1) {
            for(uint64_t row = 0; row < BOARD_HW; row++) {
                for(uint64_t col = 0; col < BOARD_HW; col++) {
                    working_board[col * BOARD_HW + row] = -board[row * BOARD_HW + col];
                }
            }
        }
        // Perspective already correct; just copy the board.
        else {
            for(uint64_t pos = 0; pos < BOARD_SIZE; pos++) {
                working_board[pos] = board[pos];
            }
        }

        // Flood fill, replacing 1s with 2s for each 1 in the first column.
        for(int row = 0; row < BOARD_HW; row++) {
            fill_ones(working_board, row, 0);
        }

        bool has_won = false;

        // Check for 2s in the last column to see if player 1 has won.
        for(uint64_t row = 0; row < BOARD_HW; row++) {
            if(working_board[row * BOARD_HW + BOARD_HW - 1] == 2) {
                has_won = true;
                break;
            }
        }

        return has_won;
    }

    void fill_ones(char *board, int row, int col) {
        // Stop the flood if we don't see a 1.
        if(board[row * BOARD_HW + col] != 1) {
            return;
        }

        board[row * BOARD_HW + col] = 2;

        // Flood-fill in the hexagonally-adjacent directions.
        if(row > 0) { // can check up-left
            fill_ones(board, row - 1, col);
        }
        if(row > 0 && col < BOARD_HW - 1) { // can check up-right
            fill_ones(board, row - 1, col + 1);
        }
        if(col > 0) { // can check left
            fill_ones(board, row, col - 1);
        }
        if(col < BOARD_HW - 1) { // can check right
            fill_ones(board, row, col + 1);
        }
        if(row < BOARD_HW - 1 && col > 0) { // can check down-left
            fill_ones(board, row + 1, col - 1);
        }
        if(row < BOARD_HW - 1) { // can check down-right
            fill_ones(board, row + 1, col);
        }
    }

    void print() {
        for(uint64_t row = 0; row < BOARD_HW; row++) {
            for(uint64_t i = 0; i < row; i++) {
                cout << " ";
            }
            for(uint64_t col = 0; col < BOARD_HW; col++) {
                int piece = board[BOARD_HW * row + col];

                if(piece == 1) {
                    cout << "X ";
                }
                else if(piece == -1) {
                    cout << "O ";
                }
                else {
                    cout << ". ";
                }
            }
            cout << endl;
        }
    }

    uint64_t get_thread(uint64_t move) {
        return Random<int>::getRandom(0, number_threads - 1);
    }

    bool can_expand(uint64_t position) {
        return (board[position] == 0);
    }

    bool has_winner() {
        return (winner == turn);
    }

    uint64_t get_winner() {
        return winner;
    }

    bool is_exhausted() {
        return (number_plays == BOARD_SIZE);
    }

#ifdef DEBUG
    bool check(Hex *parent) {
        if(memcmp(parent->plays, plays, parent->number_plays) != 0) {
            return false;
        }

        if(parent->number_plays + 1 != number_plays) {
            return false;
        }

        uint64_t total_difference = 0;

        for(uint64_t position = 0; position < BOARD_SIZE; position++) {
            if(parent->board[position] != board[position]) {
                total_difference++;
            }
        }

        if(total_difference != 1) {
            return false;
        }

        return true;
    }
#endif /* DEBUG */
};

using Node = MCTS_Node<Hex, Hex::BOARD_SIZE>;
using NodeAddress = GlobalAddress<Node>;

#ifdef NUMA_ALLOCATOR
template<>
thread_local NUMAAllocator<Node> *node_allocator<Node>;
template<>
thread_local NUMAAllocator<Hex> *hex_allocator<Hex>;
#endif /* NUMA_ALLOCATOR */

#ifndef NO_COMBINING
template<>
thread_local unordered_map<void *, Hex::SimulationReport> combiner<Hex::SimulationReport>;
#endif /* NO_COMBINING */

static thread_local int start_counter;
static thread_local int finish_counter;

atomic<bool> finish{false};

#ifdef STATS
atomic<uint64_t> total_time_0{0};
atomic<uint64_t> total_time_1{0};
atomic<uint64_t> total_time_2{0};

thread_local uint64_t thread_total_visits = 0;
uint64_t *local_thread_total_visits;

thread_local uint64_t thread_total_simulations = 0;
uint64_t *local_thread_total_simulations;

Barrier barrierA;
Barrier barrierB;
Barrier barrierC;
Barrier barrierD;
#endif /* STATS */

constexpr uint64_t RUN_TIME = 100000;
constexpr uint64_t RUN_VISITS = 65536;
constexpr uint64_t NUMBER_ROLLOUTS = 128;

thread_local NodeAddress root{nullptr};

void move_forward() {
    root->state->print();
#ifdef DEBUG
    cout<< "Completed: " << root->backpropagation_visits.load(std::memory_order_relaxed) << endl;
#endif /* DEBUG */

    if(root->state->is_exhausted() || root->state->has_winner()) {
        root->delete_unwanted_paths(UINT64_MAX);
#ifdef NUMA_ALLOCATOR
        root.get_address()->~MCTS_Node();
        node_allocator<Node>->deallocate(root.get_address(), thread_id);
#else
        delete root.get_address();
#endif /* NUMA_ALLOCATOR */

        for(int i = 0; i < number_threads; i++){
            MCTS::global_aggregator->call(i, [] {
                finish.store(true, std::memory_order_relaxed);
            });
        }
    }
    else {
        NodeAddress old_root = root;

        uint64_t old_root_best_move;
        NodeAddress &best_child = old_root->get_best_child(old_root_best_move);

#ifdef DEBUG
        if(number_processes == 1) {
            assert(best_child->get_parent().get_address() == old_root->self.get_address());
            assert(best_child->state->check(best_child->get_parent()->state));
        }
#endif /* DEBUG */

        root = nullptr;

        if(best_child.get_process() == process_rank) {
            best_child->set_root();
            // best_child->set_levels_remaining_descendants(0);

            for(int i: seriema::get_local_thread_ids()) {
                MCTS::global_aggregator->call(i, [best_child] {
                    root = best_child;
                });
                MCTS::global_aggregator->get_child_aggregator(i)->flush();
            }
        }
        else {
            for(int i: seriema::get_local_thread_ids()) {
                MCTS::global_aggregator->call(i, [best_child] {
                    root = nullptr;
                });
                MCTS::global_aggregator->get_child_aggregator(i)->flush();
            }

            int random_thread_id = seriema::get_random_thread_id(best_child.get_process());

            MCTS::global_aggregator->call(random_thread_id, [best_child] {
                best_child->set_root();
                // best_child->set_levels_remaining_descendants(0);

                for(int i: seriema::get_local_thread_ids()) {
                    MCTS::global_aggregator->call(i, [best_child] {
                        root = best_child;
                    });
                    MCTS::global_aggregator->get_child_aggregator(i)->flush();
                }
            });
        }

        MCTS::global_aggregator->call(old_root.get_thread_id(), [old_root, old_root_best_move] {
            old_root->delete_unwanted_paths(old_root_best_move);
#ifdef NUMA_ALLOCATOR
            old_root.get_address()->~MCTS_Node();
            node_allocator<Node>->deallocate(old_root.get_address(), thread_id);
#else
            delete old_root.get_address();
#endif /* NUMA_ALLOCATOR */
        });

        MCTS::global_aggregator->flush_all();
    }
}

void worker_thread(int offset) {
    seriema::init_thread(offset);
    MCTS::init_thread();
    Hex::init_thread();

#ifdef NUMA_ALLOCATOR
    node_allocator<Node> = new NUMAAllocator<Node>();
    hex_allocator<Hex> = new NUMAAllocator<Hex>();
#endif /* NUMA_ALLOCATOR */

#ifdef STATS
    start_counter = number_threads;

    for(int i = 0; i < number_threads; i++) {
        bool result = MCTS::global_aggregator->call(i, [] {
            start_counter--;
        });
    }

    Synchronizer start_synchronizer{(uint64_t) number_threads};

    MCTS::global_aggregator->flush_all(&start_synchronizer);

    while(start_synchronizer.get_number_operations_left() > 0) {
        MCTS::global_aggregator->flush_all();

        seriema::flush_send_completion_queues();
    #ifdef DEBUG
        cout << "waiting for initial flush to complete" << endl;
    #endif /* DEBUG */
    }

    while(start_counter > 0) {
        MCTS::global_messenger->process_calls_all();
        MCTS::global_aggregator->flush_all();
    #ifdef DEBUG
        cout << "waiting for initial message to arrive" << endl;
    #endif /* DEBUG */
    }

    barrierA.wait();
    if(thread_rank == 0) {
        MPIHelper::barrier();
    }
    barrierB.wait();
#endif /* STATS */

    if(thread_id == 0) {
        // Create the root
#ifdef NUMA_ALLOCATOR
        root = NodeAddress(new(node_allocator<Node>->allocate()) Node(0));
#else
        root = NodeAddress(new Node(0));
#endif /* NUMA_ALLOCATOR */

        for(int i: seriema::get_local_thread_ids()) {
            MCTS::global_aggregator->call(i, [root_copy = root] {
                root = root_copy; 
            });
        }

        MCTS::global_aggregator->flush_all();
    }

    uint64_t outstanding_requests = 0;

    const int RUN_FACTOR = number_threads;
    // const int RUN_FACTOR = number_processes;
    // constexpr int RUN_FACTOR = 1;

    while(!finish.load(std::memory_order_relaxed)) {
        if(root.get_address() != nullptr) {
            uint64_t snapshot_current_round_visits = root->current_round_visits.load(std::memory_order_relaxed);

            if(snapshot_current_round_visits < (RUN_VISITS * RUN_FACTOR)) {
                snapshot_current_round_visits = root->current_round_visits.fetch_add(NUMBER_ROLLOUTS * Hex::NUMBER_SIMULATIONS * RUN_FACTOR, std::memory_order_relaxed);
            }

            if(snapshot_current_round_visits < (RUN_VISITS * RUN_FACTOR)) {
                root->perform_selection(NUMBER_ROLLOUTS * RUN_FACTOR);
            }
            else {
                if(root.get_thread_id() == thread_id) {
                    if(root->backpropagation_visits.load(std::memory_order_relaxed) == (RUN_VISITS * RUN_FACTOR)) {
                        move_forward();
                    }
                }
                else {
                    root = nullptr;
                }
            }
        }

        do {
            outstanding_requests = MCTS::global_aggregator->get_messenger()->process_calls_all();
        } while(outstanding_requests > 0);

#ifndef NO_COMBINING
        auto original_combiner = std::move(combiner<Hex::SimulationReport>);

        for(auto &&combined: original_combiner) {
            reinterpret_cast<Node *>(combined.first)->perform_backpropagation(combined.second);
        }
#endif /* NO_COMBINING */

        int start = Random<int>::getRandom(0, number_threads_process - 1);

        int simulations_performed = 0;

#ifndef NO_SIMULATION_QUEUES
        for(int i = start; i < start + number_threads_process; i++) {
            int process = i % number_threads_process;

            uint64_t number_nodes;
            Node **nodes = reinterpret_cast<Node **>(MCTS::simulation_queues[thread_rank].incoming[process].receive(number_nodes));

#ifndef NO_DONATE_SIMULATIONS
            uint64_t returned_nodes = 0;

            if(process != thread_rank && number_nodes > 0) {
                if(MCTS::simulation_queues[process].want_donation) {
                    Node **buffer = reinterpret_cast<Node **>(MCTS::simulation_queues[process].incoming[thread_rank].lease(returned_nodes, number_nodes / 2));

                    if(returned_nodes > 0) {
                        std::memcpy(buffer, nodes, returned_nodes * sizeof(Node *));
                        MCTS::simulation_queues[process].incoming[thread_rank].push_lease(returned_nodes);
                        MCTS::simulation_queues[process].want_donation = false;
                    }
                }
            }
            for(int j = returned_nodes; j < number_nodes; j++) {
#else
            for(int j = 0; j < number_nodes; j++) {
#endif /* !NO_DONATE_SIMULATIONS */
                nodes[j]->perform_simulation_backpropagation();
            }

            if(number_nodes > 0) {
                MCTS::simulation_queues[thread_rank].incoming[process].pop_receive(number_nodes);
                simulations_performed++;
            }
        }
#endif /* !NO_SIMULATION_QUEUES */ 

        MCTS::global_aggregator->flush_all();
    }

#ifdef STATS
    barrierC.wait();
    if(thread_rank == 0) {
        MPIHelper::barrier();
    }
    barrierD.wait();
#endif /* STATS */

    // _move to before barrier finish_counter = number_threads;
    // Synchronizer shutdown_synchronizer{(uint64_t) number_threads};

    // MCTS::global_aggregator->shutdown_all(&shutdown_synchronizer);

    // while(shutdown_synchronizer.get_number_operations_left() > 0) {
    //     MCTS::global_messenger->process_calls_all();
    //     MCTS::global_aggregator->flush_all();

    //     seriema::flush_send_completion_queues();

    // #ifdef DEBUG
    //     cout << "waiting for incoming shutdown (1)" << endl;
    // #endif /* DEBUG */
    // }

    // while(!MCTS::global_messenger->get_incoming_shutdown_all()) {
    //     MCTS::global_messenger->process_calls_all();
    //     MCTS::global_aggregator->flush_all();
    // #ifdef DEBUG
    //     cout << "waiting for incoming shutdown (2)" << endl;
    // #endif /* DEBUG */
    // }

    // for(int i = 0; i < number_threads; i++) {
    //     bool result = MCTS::global_aggregator->call(i, [] {
    //         finish_counter--;
    //     });
    // }

    // Synchronizer finish_synchronizer{(uint64_t) number_threads};

    // MCTS::global_aggregator->flush_all(&finish_synchronizer);

    // while(finish_synchronizer.get_number_operations_left() > 0 || !MCTS::global_aggregator->flush_all()) {
    //     seriema::flush_send_completion_queues();
    // // #ifdef DEBUG
    //     cout << "waiting for final flush to complete" << endl;
    //     sleep(1);
    // // #endif /* DEBUG */
    // }

    // while(finish_counter > 0) {
    //     MCTS::global_messenger->process_calls_all();

    //     MCTS::global_aggregator->flush_all();
    //     seriema::flush_send_completion_queues();
    // // #ifdef DEBUG
    //     cout << "waiting for final message to arrive" << endl;
    //     sleep(1);
    // // #endif /* DEBUG */
    // }

#ifdef STATS
    local_thread_total_visits[thread_rank] += thread_total_visits;
    local_thread_total_simulations[thread_rank] += thread_total_simulations;
#endif /* STATS */

    seriema::print_mutex.lock();
    cout << "done" << endl;
    seriema::print_mutex.unlock();

#ifdef NUMA_ALLOCATOR
    delete node_allocator<Node>;
    delete hex_allocator<Hex>;
#endif /* NUMA_ALLOCATOR */

    Hex::finalize_thread();
    MCTS::finalize_thread();
    seriema::finalize_thread();
}

int main(int argc, char **argv) {
    vector<thread> thread_list;

    if(argc != 2) {
        cerr << "Run with format " << argv[0] << " <number_threads_process>" << endl;
        exit(EXIT_FAILURE);
    }

    number_threads_process = atoi(argv[1]);

    Configuration configuration{number_threads_process, true, false};
    configuration.number_service_threads = 1;
    configuration.create_completion_channel_shared = true;

    seriema::init_thread_handler(argc, argv, configuration);

    MCTS::LEVELS_REMAINING_MAX = 0;//std::max(4UL, (uint64_t) std::ceil(std::log2(seriema::number_threads)));

#ifdef STATS
    local_thread_total_visits = new uint64_t[number_threads_process];
    local_thread_total_simulations = new uint64_t[number_threads_process];

    for(int i = 0; i < number_threads_process; i++) {
        local_thread_total_visits[i] = 0;
        local_thread_total_simulations[i] = 0;
    }

    barrierA.setup(number_threads_process);
    barrierB.setup(number_threads_process + 1);
    barrierC.setup(number_threads_process + 1);
    barrierD.setup(number_threads_process);
#endif /* STATS */

#ifndef NO_SIMULATION_QUEUES
    MCTS::simulation_queues = new SimulationQueues[number_threads_process];
#endif /* !NO_SIMULATION_QUEUES */ 

    const unsigned int share_hardware_concurrency = std::thread::hardware_concurrency() / local_number_processes;

    for(int i = 0; i < number_threads_process; i++) {
        thread_list.emplace_back(thread(worker_thread, i));

        if(configuration.number_threads_process <= share_hardware_concurrency / 2) {
            seriema::affinity_handler.set_thread_affinity(&thread_list[i], i, (local_process_rank * share_hardware_concurrency) + (2 * i));
        }
        else {
            seriema::affinity_handler.set_thread_affinity(&thread_list[i], i, (local_process_rank * share_hardware_concurrency) + i);
        }

        cout << "Numa zone for prank " << i << ": " << seriema::affinity_handler.get_numa_zone(i) << endl;
    }

    // NOTE: In case we want to pin the service thread

    // uint64_t last_service_offset = (share_hardware_concurrency % 2 == 0) ? 1 : 2;

    // for(int i = 0; i < configuration.number_service_threads; i++) {
    //     if(configuration.number_threads_process <= share_hardware_concurrency / 2) {
    //         seriema::affinity_handler.set_thread_affinity(seriema::service_threads[i], (local_process_rank * share_hardware_concurrency) + share_hardware_concurrency - last_service_offset - (2 * i));
    //     }
    //     else if(configuration.number_threads_process <= share_hardware_concurrency - configuration.number_service_threads) {
    //         seriema::affinity_handler.set_thread_affinity(seriema::service_threads[i], (local_process_rank * share_hardware_concurrency) + share_hardware_concurrency - 1 - i);
    //     }
    // }

#ifdef STATS
    barrierB.wait();

    TimeHolder timer_real{CLOCK_REALTIME};
#endif /* STATS */

#ifdef PROFILE
    ProfilerStart("mcts-profile.txt");
#endif /* PROFILE */

#ifdef STATS
    barrierC.wait();
#endif /* STATS */

#ifdef PROFILE
    ProfilerStop();
#endif /* PROFILE */

#ifdef STATS
    total_time_0 += timer_real.tick();
#endif /* STATS */

    for(int i = 0; i < number_threads_process; i++) {
        thread_list[i].join();
    }

#ifdef STATS
    // Total time

    uint64_t my_total_time = total_time_0;
    uint64_t all_total_time[number_processes];

    MPIHelper::allGather(&my_total_time, all_total_time, MPI_UINT64_T, 1);

    uint64_t max_total_time = 0;

    for(int i = 0; i < number_processes; i++) {
        if(all_total_time[i] > max_total_time) {
            max_total_time = all_total_time[i];
        }
    }

    double avg_total_time = 0;

    for(int i = 0; i < number_processes; i++) {
        avg_total_time += all_total_time[i];
    }

    avg_total_time /= number_processes;

    // Total visits and simulations

    uint64_t global_thread_total_visits[number_threads];
    MPIHelper::allGather(local_thread_total_visits, global_thread_total_visits, MPI_UINT64_T, number_threads_process);

    uint64_t global_thread_total_simulations[number_threads];
    MPIHelper::allGather(local_thread_total_simulations, global_thread_total_simulations, MPI_UINT64_T, number_threads_process);

    uint64_t total_thread_total_visits = 0;
    uint64_t total_thread_total_simulations = 0;

    for(int i = 0; i < number_threads; i++) {
        total_thread_total_visits += global_thread_total_visits[i];
        total_thread_total_simulations += global_thread_total_simulations[i];
    }

    // Local visits and simulations

    uint64_t local_visits = 0;
    uint64_t local_simulations = 0;

    for(int i = 0; i < number_threads_process; i++) {
        local_visits += global_thread_total_visits[(process_rank * number_threads_process) + i];
        local_simulations += global_thread_total_simulations[(process_rank * number_threads_process) + i];
    }

    for(int process = 0; process < number_processes; process++) {
        if(process_rank == process) {
            cout << "Total visits: " << process_rank << ": " << local_visits << endl;
            cout << "Total simulations: " << process_rank << ": " << local_simulations << endl;
            cout << "Total time (whole_process) ms: " << process_rank << ": " << (total_time_0 / 1000000.0) << endl;
            cout << "Visits/msec on process (whole_process): " << process_rank << ": " << local_visits / (total_time_0 / 1000000.0) << endl;
            cout << "Simulations/msec on process (whole_process): " << process_rank << ": " << local_simulations / (total_time_0 / 1000000.0) << endl;
            cout << endl;

            fflush(stdout);
        }

        MPIHelper::barrier();
    }

    if(process_rank == 0) {
        cout << "AVG Global visits/msec: " << (((double) total_thread_total_visits) / (max_total_time / 1000000.0)) << endl;
        cout << "AVG Global simulations/msec: " << (((double) total_thread_total_simulations) / (max_total_time / 1000000.0)) << endl;
        cout << endl;
        cout << "Work distribution (visits): " << endl;
        for(int i = 0; i < number_processes; i++) {
            cout << "Process " << i << endl;
            for(int j = 0; j < number_threads_process; j++) {
                cout << std::setprecision(2) << 100 * ((double) global_thread_total_visits[(i * number_threads_process) + j]) / total_thread_total_visits << endl;
            }
        }
        cout << endl;
        cout << "Work distribution (simulations): " << endl;
        for(int i = 0; i < number_processes; i++) {
            cout << "Process " << i << endl;
            for(int j = 0; j < number_threads_process; j++) {
                cout << std::setprecision(2) << 100 * ((double) global_thread_total_simulations[(i * number_threads_process) + j]) / total_thread_total_simulations << endl;
            }
        }
    }
        
    delete[] local_thread_total_visits;
    delete[] local_thread_total_simulations;
#endif /* STATS */

#ifndef NO_SIMULATION_QUEUES
    delete[] MCTS::simulation_queues;
#endif /* !NO_SIMULATION_QUEUES */ 

    seriema::finalize_thread_handler();

    return 0;
}