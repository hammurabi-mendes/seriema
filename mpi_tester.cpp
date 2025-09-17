#include <chrono>
#include <random>

#include "thread_handler.h"
#include "MPI/MPIThreadHelper.hpp"
#include "utils/Synchronizer.hpp"

#define SUPERCHUNK_SIZE 4096 * 512 * 512

using namespace seriema;

void thread_function(int thread_num) {
    MPIThreadHelper::init_thread(thread_num, false);

    RDMAMemory *recv_buffer = seriema::global_allocators[affinity_handler.get_numa_zone(thread_rank)]->allocate_arbitrary(SUPERCHUNK_SIZE);
    char *recv_array = reinterpret_cast<char *>(recv_buffer->get_buffer());

    RDMAMemory *send_buffer = seriema::global_allocators[affinity_handler.get_numa_zone(thread_rank)]->allocate_arbitrary(SUPERCHUNK_SIZE);
    char *send_array = reinterpret_cast<char *>(send_buffer->get_buffer());

    RDMAMemory *time_buffer_r = seriema::global_allocators[affinity_handler.get_numa_zone(thread_rank)]->allocate_arbitrary(16*sizeof(uint64_t));
    uint64_t *recv_time = reinterpret_cast<uint64_t *>(time_buffer_r->get_buffer());

    RDMAMemory *time_buffer_s = seriema::global_allocators[affinity_handler.get_numa_zone(thread_rank)]->allocate_arbitrary(sizeof(uint64_t));
    uint64_t *send_time = reinterpret_cast<uint64_t *>(time_buffer_s->get_buffer());

    uint64_t size = 1;

    while (size <= SUPERCHUNK_SIZE / number_threads /*don't need to divide for broadcast*/) {

        /* use for all to all V and all to all W*/

        // uint64_t send_size[number_threads];
        // uint64_t send_offsets[number_threads];
        // uint64_t recv_size[number_threads];
        // uint64_t recv_offsets[number_threads];
        // for (int i = 0; i < number_threads; i++){
        //     send_size[i] = size;
        //     recv_size[i] = size;
        //     send_offsets[i] = size * i;
        //     recv_offsets[i] = size * i;
        // }

        MPIThreadHelper::barrier();

        auto start = std::chrono::system_clock::now();
        //this is the tested function. switch out the function with the desired
        //use MPI_CHAR as the data type.
        MPIThreadHelper::allToAllV(send_buffer, send_size, send_offsets, recv_buffer, recv_size, recv_offsets, MPI_CHAR, nullptr);

        auto end = std::chrono::system_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
        uint64_t time = duration.count();

        *send_time = time;

        MPIThreadHelper::gather(time_buffer_s, time_buffer_r, 0, MPI_UINT64_T, 1, nullptr);

        if(seriema::thread_id == 0){
            uint64_t average = 0;

            for(int i = 0; i < number_threads; i++){
                average += recv_time[i];
            }

            average = average / number_threads;

            std::cout << "The average time was " << average << " for a size of " << size << "\n";
        }

        size = size * 4;
    }

    finalize_thread();
}

int main(int argc, char** argv) {
    Configuration config(1);  // number of threads per process for test
    init_thread_handler(argc, argv, config);

    std::vector<std::thread> threads;
    for (int i = 0; i < number_threads_process; ++i) {
        threads.emplace_back(thread_function, i);
    }

    for (auto& t : threads) {
        t.join();
    }

    finalize_thread_handler();
    return 0;
}
