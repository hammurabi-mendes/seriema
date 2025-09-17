#ifndef MPI_THREAD_HELPER_HPP
#define MPI_THREAD_HELPER_HPP

#include <numeric>

#include <algorithm>
#include <functional>
#include <limits.h>
#include <thread>
#include <chrono>

#include <mpi.h>

#include "seriema.h"

#include "MPIHelper.hpp"

#include "utils/Barrier.hpp"
#include "utils/Synchronizer.hpp"

using std::accumulate;
using std::thread;

using seriema::number_threads;
using seriema::number_threads_process;
using seriema::number_processes;
using seriema::process_rank;

#define MAX_THREADS_SUPPORTED 128
    
thread_local RDMAMemory *my_flags_rdma;
thread_local void *my_flags;
thread_local RDMAMemoryLocator *others_flags;
/**
 * Class that provides facilities to use MPI collective operations at thread level.
 */
struct MPIThreadHelper {
        
    static MPI_Datatype get_rdma_memory_locator_type() {
        static MPI_Datatype type = [] {
            MPI_Datatype t;
            int block_lengths[2] = {1, 1};
            MPI_Aint displacements[2];
            MPI_Datatype types[2] = {MPI_AINT, MPI_UINT32_T};

            displacements[0] = offsetof(RDMAMemoryLocator, buffer);
            displacements[1] = offsetof(RDMAMemoryLocator, remote_key);

            MPI_Type_create_struct(2, block_lengths, displacements, types, &t);
            MPI_Type_commit(&t);

            return t;
        }();

        return type;
    }

    static void flagsAllGather(RDMAMemoryLocator *my_flag) {
        static Barrier thread_check_in(number_threads_process);
        static vector<RDMAMemoryLocator> send_locators(number_threads_process);
        static vector<RDMAMemoryLocator> recv_locators(number_threads);
        send_locators[seriema::thread_rank] = *my_flag;

        thread_check_in.wait();
        if (seriema::thread_rank == 0){
            MPIHelper::allGather(send_locators.data(), recv_locators.data(), get_rdma_memory_locator_type(), number_threads_process);
        }
        thread_check_in.wait();

        memcpy(others_flags, recv_locators.data(), sizeof(RDMAMemoryLocator)*number_threads);
    }

    static void init_thread(int offset, bool create_allocators) {
        seriema::thread_rank = offset;
        seriema::thread_id = seriema::thread_rank + (process_rank * number_threads_process);

        RDMAAllocator<seriema::GLOBAL_ALLOCATOR_CHUNK_SIZE, seriema::GLOBAL_ALLOCATOR_SUPERCHUNK_SIZE> *global_allocator = seriema::global_allocators[seriema::affinity_handler.get_numa_zone(seriema::thread_rank)];

        seriema::thread_context = new seriema::ThreadContext(create_allocators, global_allocator);

        my_flags_rdma = global_allocator->allocate_chunk();

        my_flags = reinterpret_cast<uint64_t *>(my_flags_rdma->get_buffer());

        RDMAMemoryLocator *my_flags_locator = new RDMAMemoryLocator(my_flags_rdma);

        RDMAMemory *other_flags_rdma = global_allocator->allocate_chunk();

        others_flags = reinterpret_cast<RDMAMemoryLocator *>(other_flags_rdma->get_buffer());

        flagsAllGather(my_flags_locator);

        for(int i = 0; i < 4*number_threads; i++){
            *reinterpret_cast<uint64_t*>(static_cast<char*>(my_flags) + i * sizeof(uint64_t)) = 0;
        }

        for(int i = 0; i < number_threads; i++){
            others_flags[i].buffer = others_flags[i].get_buffer(seriema::thread_id*4*sizeof(uint64_t));
        }

        #if defined(LIBNUMA) && defined(__linux__)
            seriema::affinity_handler.set_numa_location(seriema::thread_rank);
        #endif /* LIBNUMA and __linux__ */
    }

    template<typename T>
    static inline void MPI_CHECK(T function) {
        int return_value;

        if((return_value = (function)) != 0) {
            char error_string[MPI_MAX_ERROR_STRING];
            int length;

            MPI_Error_string(return_value, error_string, &length);

            PRINT_ERROR("MPI call failed: " << function << ": " << error_string);

            exit(-2);
        }
    }

    /**
     * Performs an MPI Scatter operation on a per thread basis.  
     *
     * @param send_buffer Location of the sent data (\p number_records for each process).
     * @param recv_buffer Location of the received data (\p number_records).
     * @param root_thread_id The thread for which \p send_buffer is valid.
     * @param datatype MPI datatype of each element in the send and receive buffers.
     * @param addr Synchronizer to decrement at termination, defaults to null pointer.
     * @param request MPI request that defaults to the thread_local global_request from dsys
     * @param number_records Number of records to be sent to each process by process \p root,
     *                      and number of records to be received by each process.
     */
    static inline void scatter(RDMAMemory *&send_buffer, RDMAMemory *&recv_buffer, int root_thread_id, MPI_Datatype datatype, uint64_t number_records = 1, Synchronizer *addr = nullptr) {
        int type_size;
        MPI_Type_size(datatype, &type_size);

        if(seriema::thread_id != root_thread_id){
            new (reinterpret_cast<RDMAMemoryLocator*>(static_cast<char*>(my_flags) + root_thread_id * 4 * sizeof(uint64_t))) RDMAMemoryLocator(recv_buffer);
            seriema::get_transmitter(root_thread_id)->rdma_write(my_flags_rdma, root_thread_id * 4 * sizeof(uint64_t), sizeof(RDMAMemoryLocator), (&others_flags[root_thread_id]));
            while(*reinterpret_cast<uint64_t*>(static_cast<char*>(my_flags) + 4*root_thread_id*sizeof(uint64_t)) != 0){
            std::this_thread::yield();
            }
        }

        else{
            new (reinterpret_cast<RDMAMemoryLocator*>(static_cast<char*>(my_flags) + root_thread_id * 4 * sizeof(uint64_t))) RDMAMemoryLocator(recv_buffer);
            Synchronizer sync_1{(uint64_t) number_threads};
            Synchronizer sync_2{(uint64_t) number_threads};
            int i = 0;
            while(sync_1.get_number_operations_left() > 0){
                if(i == number_threads){
                    i = 0;
                }
                if(*reinterpret_cast<uint64_t*>(static_cast<char*>(my_flags) + 4*i*sizeof(uint64_t)) != 0){
                    RDMAMemoryLocator write_to = *reinterpret_cast<RDMAMemoryLocator*>(static_cast<char*>(my_flags) + i*4*sizeof(uint64_t));
                    seriema::get_transmitter(i)->rdma_write_batches(send_buffer, i*number_records*type_size, number_records*type_size, &write_to, 0, 0, &sync_2);
                    *reinterpret_cast<uint64_t*>(static_cast<char*>(my_flags) + 4*i*sizeof(uint64_t)) = 0;
                    *reinterpret_cast<uint64_t*>(static_cast<char*>(my_flags) + (4*i+1)*sizeof(uint64_t)) = 0;
                    seriema::get_transmitter(i)->rdma_write(my_flags_rdma, 4*i*sizeof(uint64_t), 2*sizeof(uint64_t), &others_flags[i], 0, 0, nullptr);
                    sync_1.decrease();
                }
                i++;          
            }
            while(sync_2.get_number_operations_left() > 0){
                seriema::context->completion_queues->flush_send_completion_queue();
            }
        }


        if(addr != nullptr){
            addr->decrease();
        }
    }

    /**
     * Performs an MPI Gather operation on a per thread basis.
     *
     * @param send_buffer Location of the sent data (\p number_records).
     * @param recv_buffer Location of the received data (\p number_records for each process).
     * @param root_thread_id The process for which \p recv_buffer is valid.
     * @param datatype MPI datatype of each element in the send and receive buffers.
     * @param addr Synchronizer to decrement at termination, defaults to null pointer.
     * @param request MPI request that defaults to the thread_local global_request from dsys.
     * @param number_records Number of records to be received from each thread, gathered locally and then gathered across process by process \p root,
     *                      and number of records to be sent by each thread.
     */
    static inline void gather(RDMAMemory *&send_buffer, RDMAMemory *&recv_buffer, int root_thread_id, MPI_Datatype datatype, uint64_t number_records = 1, Synchronizer *addr = nullptr){
        int type_size;
        MPI_Type_size(datatype, &type_size);

        if(seriema::thread_id == root_thread_id){
            Synchronizer self_sync{(uint64_t) 1};
            RDMAMemory *per_thread_recv;
            for(int i = 0; i < number_threads; i++){
                per_thread_recv = new RDMAMemory(recv_buffer, number_records*type_size, i*number_records*type_size);
                if(i == root_thread_id){
                    seriema::get_transmitter(i)->rdma_write_batches(send_buffer, 0, type_size*number_records, new RDMAMemoryLocator(per_thread_recv), 0, 0, &self_sync);
                }
                else{
                    new (reinterpret_cast<RDMAMemoryLocator*>(static_cast<char*>(my_flags) + i * 4 * sizeof(uint64_t))) RDMAMemoryLocator(per_thread_recv);
                    seriema::get_transmitter(i)->rdma_write(my_flags_rdma, i * 4 * sizeof(uint64_t), sizeof(RDMAMemoryLocator), (&others_flags[i]));
                }
            }
            for(int i = 0; i < number_threads; i++){
                if(i == root_thread_id){
                    *reinterpret_cast<uint64_t*>(static_cast<char*>(my_flags) + 4*root_thread_id*sizeof(uint64_t)) = 0;
                    *reinterpret_cast<uint64_t*>(static_cast<char*>(my_flags) + (4*root_thread_id+1)*sizeof(uint64_t)) = 0;
                }
                while(*reinterpret_cast<uint64_t*>(static_cast<char*>(my_flags) + 4*i*sizeof(uint64_t)) != 0){
                    std::this_thread::yield();
                }
            }
            while(self_sync.get_number_operations_left() > 0){
                std::this_thread::yield();
            }
        }

        else{
            Synchronizer sync{(uint64_t) 1};
            while(*reinterpret_cast<uint64_t*>(static_cast<char*>(my_flags) + 4*root_thread_id*sizeof(uint64_t)) == 0){
                std::this_thread::yield();
            }
            RDMAMemoryLocator write_to = *reinterpret_cast<RDMAMemoryLocator*>(static_cast<char*>(my_flags) + 4*root_thread_id*sizeof(uint64_t));
            seriema::get_transmitter(root_thread_id)->rdma_write_batches(send_buffer, 0, number_records*type_size, &write_to, 0, 0, &sync);
            *reinterpret_cast<uint64_t*>(static_cast<char*>(my_flags) + 4*root_thread_id*sizeof(uint64_t)) = 0;
            *reinterpret_cast<uint64_t*>(static_cast<char*>(my_flags) + (4*root_thread_id+1)*sizeof(uint64_t)) = 0;
            seriema::get_transmitter(root_thread_id)->rdma_write(my_flags_rdma, 4*root_thread_id * sizeof(uint64_t), 2*sizeof(uint64_t), &others_flags[root_thread_id], 0, 0, nullptr);
            while(sync.get_number_operations_left() > 0){
                seriema::context->completion_queues->flush_send_completion_queue();
            }
        }

        if(addr != nullptr){
            addr->decrease();
        }
    }


    /**
     * Performs an MPI AllGather operation on a per thread basis.
     *
     * @param send_buffer Location of the sent data (\p number_records).
     * @param recv_buffer Location of the received data (\p number_records for each process).
     * @param datatype MPI datatype of each element in the send and receive buffers.
     * @param addr Synchronizer to decrement at termination, defaults to null pointer.
     * @param request MPI request that defaults to the thread_local global_request from dsys
     * @param number_records Number of records to be received from each process,
     *                      and number of records to be sent by each process.
     */
    static inline void allGather(RDMAMemory *&send_buffer, RDMAMemory *&recv_buffer, MPI_Datatype datatype, uint64_t number_records = 1, Synchronizer *addr = nullptr) {
        int type_size;
        MPI_Type_size(datatype, &type_size);

        new (reinterpret_cast<RDMAMemoryLocator*>(static_cast<char*>(my_flags) + seriema::thread_id * 4 * sizeof(uint64_t))) RDMAMemoryLocator(recv_buffer);
        for(int i = 0; i < number_threads; i++){
            if(i != seriema::thread_id){
                seriema::get_transmitter(i)->rdma_write(my_flags_rdma, seriema::thread_id * 4 * sizeof(uint64_t), sizeof(RDMAMemoryLocator), (&others_flags[i]));
                *reinterpret_cast<uint64_t*>(static_cast<char*>(my_flags) + (4*i+2)*sizeof(uint64_t)) = 1;
            }
        }

        Synchronizer sync_1{(uint64_t) number_threads-1};
        Synchronizer sync_2{(uint64_t) number_threads};

        RDMAMemoryLocator write_to = *reinterpret_cast<RDMAMemoryLocator*>(static_cast<char*>(my_flags) + seriema::thread_id*4*sizeof(uint64_t));
        write_to.buffer = write_to.get_buffer(seriema::thread_id*number_records*type_size);
        seriema::get_transmitter(seriema::thread_id)->rdma_write_batches(send_buffer, 0, number_records*type_size, &write_to, 0, 0, &sync_2);
        
        int i = 0;
        while(sync_1.get_number_operations_left() > 0){
            if(i == number_threads){
                i = 0;
            }
            if(i != seriema::thread_id){
                if(*reinterpret_cast<uint64_t*>(static_cast<char*>(my_flags) + 4*i*sizeof(uint64_t)) != 0){
                    write_to = *reinterpret_cast<RDMAMemoryLocator*>(static_cast<char*>(my_flags) + i*4*sizeof(uint64_t));
                    write_to.buffer = write_to.get_buffer(seriema::thread_id*number_records*type_size);
                    seriema::get_transmitter(i)->rdma_write_batches(send_buffer, 0, number_records*type_size, &write_to, 0, 0, &sync_2);
                    *reinterpret_cast<uint64_t*>(static_cast<char*>(my_flags) + 4*i*sizeof(uint64_t)) = 0;
                    *reinterpret_cast<uint64_t*>(static_cast<char*>(my_flags) + (4*i+1)*sizeof(uint64_t)) = 0;
                    write_to = others_flags[i];
                    write_to.buffer = write_to.get_buffer(2*sizeof(uint64_t));
                    seriema::get_transmitter(i)->rdma_write(my_flags_rdma, (4*seriema::thread_id+2)*sizeof(uint64_t), sizeof(uint64_t), &write_to, 0, 0, nullptr);
                    sync_1.decrease();
                }
            }
            i++;          
        }
        while(sync_2.get_number_operations_left() > 0){
            seriema::context->completion_queues->flush_send_completion_queue();
        }

        for(int i = 0; i < number_threads; i++){
            while(*reinterpret_cast<uint64_t*>(static_cast<char*>(my_flags) + (4*i+2)*sizeof(uint64_t)) !=0){
                std::this_thread::yield();
            }
        }
        *reinterpret_cast<uint64_t*>(static_cast<char*>(my_flags) + 4*seriema::thread_id*sizeof(uint64_t)) = 0;
        *reinterpret_cast<uint64_t*>(static_cast<char*>(my_flags) + (4*seriema::thread_id+1)*sizeof(uint64_t)) = 0;

        if(addr != nullptr){
            addr->decrease();
        }
    }

    /**
	 * Performs an MPI All-to-All operation on a per thread basis.
	 *
	 * @param send_buffer Location of the sent data (\p sendSizes[i] records at \p sendOffsets[i] for each process).
	 * @param recv_buffer Location of the received data (\p recvSizes[i] records at \p recvOffsets[i] for each process).
	 * @param datatype MPI datatype of each element in the send and receive buffers.
     * @param addr Synchronizer to decrement at termination, defaults to null pointer.
     * @param request MPI request that defaults to the thread_local global_request from dsys.
     * @param number_records Number of records to be received from each process,
     *                      and number of records to be sent by each process.
	 */
    static inline void allToAll(RDMAMemory *&send_buffer, RDMAMemory *&recv_buffer, MPI_Datatype datatype, uint64_t number_records = 1, Synchronizer *addr = nullptr) {
        int type_size;
        MPI_Type_size(datatype, &type_size);

        new (reinterpret_cast<RDMAMemoryLocator*>(static_cast<char*>(my_flags) + seriema::thread_id * 4 * sizeof(uint64_t))) RDMAMemoryLocator(recv_buffer);
        for(int i = 0; i < number_threads; i++){
            if(i != seriema::thread_id){
                seriema::get_transmitter(i)->rdma_write(my_flags_rdma, seriema::thread_id * 4 * sizeof(uint64_t), sizeof(RDMAMemoryLocator), (&others_flags[i]));
                *reinterpret_cast<uint64_t*>(static_cast<char*>(my_flags) + (4*i+2)*sizeof(uint64_t)) = 1;
            }
        }

        Synchronizer sync_1{(uint64_t) number_threads-1};
        Synchronizer sync_2{(uint64_t) number_threads};

        RDMAMemoryLocator write_to = *reinterpret_cast<RDMAMemoryLocator*>(static_cast<char*>(my_flags) + seriema::thread_id*4*sizeof(uint64_t));
        write_to.buffer = write_to.get_buffer(seriema::thread_id*number_records*type_size);
        seriema::get_transmitter(seriema::thread_id)->rdma_write_batches(send_buffer, seriema::thread_id*number_records*type_size, number_records*type_size, &write_to, 0, 0, &sync_2);
        
        int i = 0;
        while(sync_1.get_number_operations_left() > 0){
            if(i == number_threads){
                i = 0;
            }
            if(i != seriema::thread_id){
                if(*reinterpret_cast<uint64_t*>(static_cast<char*>(my_flags) + 4*i*sizeof(uint64_t)) != 0){
                    write_to = *reinterpret_cast<RDMAMemoryLocator*>(static_cast<char*>(my_flags) + i*4*sizeof(uint64_t));
                    write_to.buffer = write_to.get_buffer(seriema::thread_id*number_records*type_size);
                    seriema::get_transmitter(i)->rdma_write_batches(send_buffer, i*number_records*type_size, number_records*type_size, &write_to, 0, 0, &sync_2);
                    *reinterpret_cast<uint64_t*>(static_cast<char*>(my_flags) + 4*i*sizeof(uint64_t)) = 0;
                    *reinterpret_cast<uint64_t*>(static_cast<char*>(my_flags) + (4*i+1)*sizeof(uint64_t)) = 0;
                    write_to = others_flags[i];
                    write_to.buffer = write_to.get_buffer(2*sizeof(uint64_t));
                    seriema::get_transmitter(i)->rdma_write(my_flags_rdma, (4*seriema::thread_id+2)*sizeof(uint64_t), sizeof(uint64_t), &write_to, 0, 0, nullptr);
                    sync_1.decrease();
                }
            }
            i++;          
        }
        while(sync_2.get_number_operations_left() > 0){
            seriema::context->completion_queues->flush_send_completion_queue();
        }

        for(int i = 0; i < number_threads; i++){
            while(*reinterpret_cast<uint64_t*>(static_cast<char*>(my_flags) + (4*i+2)*sizeof(uint64_t)) !=0){
                std::this_thread::yield();
            }
        }
        *reinterpret_cast<uint64_t*>(static_cast<char*>(my_flags) + 4*seriema::thread_id*sizeof(uint64_t)) = 0;
        *reinterpret_cast<uint64_t*>(static_cast<char*>(my_flags) + (4*seriema::thread_id+1)*sizeof(uint64_t)) = 0;

        if(addr != nullptr){
            addr->decrease();
        }
    }

    /**
	 * Performs an MPI All-to-All-V operation on a per thread basis.
	 *
	 * @param send_buffer Location of the sent data (\p sendSizes[i] records at \p sendOffsets[i] for each process).
	 * @param send_sizes Vector containing the number of objects sent to each process.
	 * @param send_offsets Vector containing the number of objects, offset from the base, to skip and send to each process.
	 * @param recv_buffer Location of the received data (\p recvSizes[i] records at \p recvOffsets[i] for each process).
	 * @param recv_sizes Vector containing the number of objects received from each process.
	 * @param recv_offsets Vector containing the number of objects, offset from the base, to skip and receive from each process.
	 * @param datatype MPI datatype of each element in the send and receive buffers.
     * @param addr Synchronizer to decrement at termination, defaults to null pointer.
     * @param request MPI request that defaults to the thread_local global_request from dsys
	 */
    static inline void allToAllV(RDMAMemory *&send_buffer, uint64_t *send_sizes, uint64_t *send_offsets, RDMAMemory *&recv_buffer, uint64_t *recv_sizes, uint64_t *recv_offsets, MPI_Datatype datatype, Synchronizer *addr = nullptr){
        int type_size;
        MPI_Type_size(datatype, &type_size);
        
        RDMAMemoryLocator write_to;
        new (reinterpret_cast<RDMAMemoryLocator*>(static_cast<char*>(my_flags) + seriema::thread_id * 4 * sizeof(uint64_t))) RDMAMemoryLocator(recv_buffer);
        for(int i = 0; i < number_threads; i++){
            if(i != seriema::thread_id){
                *reinterpret_cast<uint64_t*>(static_cast<char*>(my_flags) + (4*i+3)*sizeof(uint64_t)) = recv_offsets[i];
                write_to = others_flags[i];
                write_to.buffer = write_to.get_buffer(2*sizeof(uint64_t));
                seriema::get_transmitter(i)->rdma_write(my_flags_rdma, (4*i+3)*sizeof(uint64_t), sizeof(uint64_t), &write_to, 0, 0, nullptr);
                seriema::get_transmitter(i)->rdma_write(my_flags_rdma, seriema::thread_id * 4 * sizeof(uint64_t), sizeof(RDMAMemoryLocator), (&others_flags[i]));
            }
        } 

        Synchronizer sync_1{(uint64_t) number_threads-1};
        Synchronizer sync_2{(uint64_t) number_threads};

        write_to = *reinterpret_cast<RDMAMemoryLocator*>(static_cast<char*>(my_flags) + seriema::thread_id*4*sizeof(uint64_t));
        uint64_t self_offset = recv_offsets[seriema::thread_id];
        write_to.buffer = write_to.get_buffer(self_offset);
        seriema::get_transmitter(seriema::thread_id)->rdma_write_batches(send_buffer, send_offsets[seriema::thread_id]*type_size, send_sizes[seriema::thread_id]*type_size, &write_to, 0, 0, &sync_2);

        int i = 0;
        while(sync_1.get_number_operations_left() > 0){
            if(i == number_threads){
                i = 0;
            }
            if(i != seriema::thread_id){
                if(*reinterpret_cast<uint64_t*>(static_cast<char*>(my_flags) + 4*i*sizeof(uint64_t)) != 0){
                    write_to = *reinterpret_cast<RDMAMemoryLocator*>(static_cast<char*>(my_flags) + i*4*sizeof(uint64_t));
                    uint64_t offset = *reinterpret_cast<uint64_t*>(static_cast<char*>(my_flags) + (i*4+2)*sizeof(uint64_t));
                    offset = offset * type_size;
                    write_to.buffer = write_to.get_buffer(offset);
                    seriema::get_transmitter(i)->rdma_write_batches(send_buffer, send_offsets[i]*type_size, send_sizes[i]*type_size, &write_to, 0, 0, &sync_2);
                    *reinterpret_cast<uint64_t*>(static_cast<char*>(my_flags) + 4*i*sizeof(uint64_t)) = 0;
                    *reinterpret_cast<uint64_t*>(static_cast<char*>(my_flags) + (4*i+1)*sizeof(uint64_t)) = 0;
                    *reinterpret_cast<uint64_t*>(static_cast<char*>(my_flags) + (4*i+2)*sizeof(uint64_t)) = 0;
                    write_to = others_flags[i];
                    write_to.buffer = write_to.get_buffer(3*sizeof(uint64_t));
                    seriema::get_transmitter(i)->rdma_write(my_flags_rdma, (4*seriema::thread_id+3)*sizeof(uint64_t), sizeof(uint64_t), &write_to, 0, 0, nullptr);
                    sync_1.decrease();
                }
            }
            i++;          
        }
        while(sync_2.get_number_operations_left() > 0){
            seriema::context->completion_queues->flush_send_completion_queue();
        }

        for(int i = 0; i < number_threads; i++){
            while(*reinterpret_cast<uint64_t*>(static_cast<char*>(my_flags) + (4*i+3)*sizeof(uint64_t)) !=0){
                std::this_thread::yield();
            }
        }
        *reinterpret_cast<uint64_t*>(static_cast<char*>(my_flags) + 4*seriema::thread_id*sizeof(uint64_t)) = 0;
        *reinterpret_cast<uint64_t*>(static_cast<char*>(my_flags) + (4*seriema::thread_id+1)*sizeof(uint64_t)) = 0;

        if(addr != nullptr){
            addr->decrease();
        }
    }

    /**
	 * Performs an MPI All-to-All-W operation on a per thread basis.
	 *
	 * @param send_buffer Location of the sent data (\p sendSizes[i] records of type \p sendTypes[i] at \p sendOffsets[i] for each process).
	 * @param send_sizes Vector containing the number of objects sent to each process.
	 * @param send_offsets Vector containing the byte offset from the base, to skip and send to each process.
     * @param send_types Vector containing the datatype send to each process.
	 * @param recv_buffer Location of the received data (\p recvSizes[i] records of type \p recvTypes[i] at \p recvOffsets[i] for each process).
	 * @param recv_sizes Vector containing the number of objects received from each process.
	 * @param recv_offsets Vector containing the byte offset from the base, to skip and receive from each process.
	 * @param recv_types Vector containing the datatype received from each process.
     * @param addr Synchronizer to decrement at termination, defaults to null pointer.
     * @param request MPI request that defaults to the thread_local global_request from dsys
	 */
    static inline void allToAllW(RDMAMemory *&send_buffer, uint64_t *send_sizes, uint64_t *send_offsets, MPI_Datatype *send_types, RDMAMemory *&recv_buffer, uint64_t *recv_sizes, uint64_t *recv_offsets, MPI_Datatype *recv_types, Synchronizer *addr = nullptr) {
        int type_size;

        uint64_t send_size_byte[number_threads];
        uint64_t recv_size_byte[number_threads];
        //for loop is taking about 20 microseconds
        //way to do with out MPI_Type_size?
        for (int i  = 0; i < number_threads; i++){
            MPI_Type_size(send_types[i], &type_size);
            send_size_byte[i] = send_sizes[i] * type_size;
            MPI_Type_size(send_types[i], &type_size);
            send_size_byte[i] = send_sizes[i] * type_size;
        }

        MPIThreadHelper::allToAllV(send_buffer, send_size_byte, send_offsets, recv_buffer, recv_size_byte, recv_offsets, MPI_CHAR);

        if(addr != nullptr){
            addr->decrease();
        }
    }

    /**
	 * Performs an MPI All-to-All-W operation on a per thread basis. Faster but with non-standard parameters.
	 *
	 * @param send_buffer Location of the sent data (\p sendSizes[i] records of type \p sendTypes[i] at \p sendOffsets[i] for each process).
	 * @param send_sizes Vector containing the number of bytes sent to each process.
	 * @param send_offsets Vector containing the byte offset from the base, to skip and send to each process.
	 * @param recv_buffer Location of the received data (\p recvSizes[i] records of type \p recvTypes[i] at \p recvOffsets[i] for each process).
	 * @param recv_sizes Vector containing the number of bytes received from each process.
	 * @param recv_offsets Vector containing the byte offset from the base, to skip and receive from each process.
     * @param addr Synchronizer to decrement at termination, defaults to null pointer.
     * @param request MPI request that defaults to the thread_local global_request from dsys
	 */
    static inline void allToAllW(RDMAMemory *&send_buffer, uint64_t *send_sizes, uint64_t *send_offsets, RDMAMemory *&recv_buffer, uint64_t *recv_sizes, uint64_t *recv_offsets, Synchronizer *addr = nullptr) {
        int type_size;

        MPIThreadHelper::allToAllV(send_buffer, send_sizes, send_offsets, recv_buffer, recv_sizes, recv_offsets, MPI_CHAR);

        if(addr != nullptr){
            addr->decrease();
        }
    }

    /**
     * Executes barrier for all threads in all processes
     */
    static inline void barrier() {
        static Barrier threadCheckIn(number_threads_process);
        threadCheckIn.wait();
        if(seriema::thread_rank == 0) {
            MPI_Barrier(MPI_COMM_WORLD);
        }
        threadCheckIn.wait();
    }
    
    /**
     * Performs an MPI Broadcast operation on a per thread basis.  
     *
     * @param thread_buffer Location where data is sent from or received to.
     * @param root_thread_id The thread for which \p send_buffer is valid.
     * @param datatype MPI datatype of each element in the send and receive buffers.
     * @param addr Synchronizer to decrement at termination, defaults to null pointer.
     * @param request MPI request that defaults to the thread_local global_request from dsys
     * @param number_records Number of records to be sent to each process by process \p root,
     *                      and number of records to be received by each process.
     */
    static inline void broadcast(RDMAMemory *&thread_buffer, int root_thread_id, MPI_Datatype datatype, uint64_t number_records = 1, Synchronizer *addr = nullptr) {
        int type_size;
        MPI_Type_size(datatype, &type_size);

        if(seriema::thread_id != root_thread_id){
            new (reinterpret_cast<RDMAMemoryLocator*>(static_cast<char*>(my_flags) + root_thread_id * 4 * sizeof(uint64_t))) RDMAMemoryLocator(thread_buffer);
            seriema::get_transmitter(root_thread_id)->rdma_write(my_flags_rdma, root_thread_id * 4 * sizeof(uint64_t), sizeof(RDMAMemoryLocator), (&others_flags[root_thread_id]));
            while(*reinterpret_cast<uint64_t*>(static_cast<char*>(my_flags) + 4*root_thread_id*sizeof(uint64_t)) != 0){
                std::this_thread::yield();
            }
        }

        else{
            Synchronizer sync_1{(uint64_t) number_threads-1};
            Synchronizer sync_2{(uint64_t) number_threads-1};
            int i = 0;
            while(sync_1.get_number_operations_left() > 0){
                if(i == number_threads){
                    i = 0;
                }
                if(i != root_thread_id){
                    if(*reinterpret_cast<uint64_t*>(static_cast<char*>(my_flags) + 4*i*sizeof(uint64_t)) != 0){
                        RDMAMemoryLocator write_to = *reinterpret_cast<RDMAMemoryLocator*>(static_cast<char*>(my_flags) + i* 4* sizeof(uint64_t));
                        seriema::get_transmitter(i)->rdma_write_batches(thread_buffer, 0, number_records*type_size, &write_to, 0, 0, &sync_2);
                        *reinterpret_cast<uint64_t*>(static_cast<char*>(my_flags) + 4*i*sizeof(uint64_t)) = 0;
                        *reinterpret_cast<uint64_t*>(static_cast<char*>(my_flags) + (4*i+1)*sizeof(uint64_t)) = 0;
                        seriema::get_transmitter(i)->rdma_write(my_flags_rdma, i * 4 * sizeof(uint64_t), 2*sizeof(uint64_t), &others_flags[i], 0, 0, nullptr);
                        sync_1.decrease();
                    }
                }
                i++;          
            }
            while(sync_2.get_number_operations_left() > 0){
                seriema::context->completion_queues->flush_send_completion_queue();
            }
        }

        if(addr != nullptr){
            addr->decrease();
        }
    }
};

#endif /* MPI_THREAD_HELPER_HPP */