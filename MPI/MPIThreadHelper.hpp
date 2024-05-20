#ifndef MPI_THREAD_HELPER_HPP
#define MPI_THREAD_HELPER_HPP

#include <numeric>

#include <algorithm>
#include <functional>
#include <limits.h>
#include <thread>

#include <mpi.h>

#include "definitions.h"

#include "utils/SerializationBuffer.hpp"
#include "MPIHelper.hpp"

#include "Barrier.hpp"
#include "utils/Synchronizer.hpp"

using std::accumulate;
using std::thread;

using seriema::Barrier;
using seriema::Synchronizer;

using seriema::number_threads;
using seriema::number_threads_process;
using seriema::number_processes;
using seriema::process_rank;

#define MAX_THREADS_SUPPORTED 128
/**
 * Class that provides facilities to use MPI collective operations.
 */
struct MPIThreadHelper {
    /*
     * Helper function that waits on the given request and status and then signals on the semaphore when the wait is over.  
     * @param request The MPI_Request of the MPI call on which to wait
     * @param status the MPI_Status of the MPI call on which to wait
     * @param work the Synchronizer used to indicate when the wait is over
     */
    // static void waitOnNonBlock(MPI_Request *request, MPI_Status *status, Barrier *work, Synchronizer *wasParsed, bool signal = true) {
    //     MPI_Wait(request, status);
    //     work->wait();
    //     if(signal) {
    //         wasParsed->decrease();
    //         cout << "signalled?" << endl;
    //     }
    // }

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
	 * Returns a new datatype on \p newType that consists of \p count elements of an old datatype \oldType.
	 */
    static inline void getNewDatatype(MPI_Count count, MPI_Datatype oldType, MPI_Datatype *newType) {
        MPI_Count numberChunks = count / INT_MAX;
        MPI_Count numberRemainder = count % INT_MAX;

        MPI_Datatype chunks;
        MPI_Type_vector(numberChunks, INT_MAX, INT_MAX, oldType, &chunks);

        MPI_Datatype remainder;
        MPI_Type_contiguous(numberRemainder, oldType, &remainder);

        MPI_Aint lowerBound, oldTypeExtent;
        MPI_Type_get_extent(oldType, &lowerBound, &oldTypeExtent);

        MPI_Aint remainderDisplacement = ((MPI_Aint) numberChunks) * INT_MAX * oldTypeExtent;

        int lengths[2] = {1, 1};
        MPI_Aint displacements[2] = {0, remainderDisplacement};
        MPI_Datatype types[2] = {chunks, remainder};
        MPI_Type_create_struct(2, lengths, displacements, types, newType);

        MPI_Type_commit(newType);

        MPI_Type_free(&chunks);
        MPI_Type_free(&remainder);
    }

    /**
	 * Returns the size associated with datatype \p type on \p size.
	 */
    static inline void getDatatypeExtent(MPI_Datatype type, MPI_Aint *size) {
        MPI_Aint lbSize;

        MPI_Type_get_extent(type, &lbSize, size);
    }

    /**
	 * Creates a new MPI communicator in \p newCommunicator that represents a complete communication graph.
	 * The new MPI communicator is created over \p oldCommunicator.
	 */
    static inline int createCompleteGraph(MPI_Comm oldCommunicator, MPI_Comm *newCommunicator) {
        vector<int> sources(number_threads);
        vector<int> destinations(number_threads);

        for(int i = 0; i < number_threads; i++) {
            sources[i] = i;
            destinations[i] = i;
        }

        int returnValue = MPI_Dist_graph_create_adjacent(oldCommunicator, number_threads, &sources[0], MPI_UNWEIGHTED, number_threads, &destinations[0], MPI_UNWEIGHTED, MPI_INFO_NULL, 0, newCommunicator);

        return returnValue;
    }

    /**
	 * Performs an MPI Scatter operation on a per thread basis.  
	 *
	 * @param threadLocalSendBuffer Location of the sent data (\p numberRecords for each thread).
	 * @param threadLocalRecvBuffer Location of the received data (\p numberRecords).
     * @param rootThread The thread_rank for which \p threadLocalSendBuffer is valid
	 * @param rootProcess The process for which \p threadLocalSendBuffer is valid.
	 * @param datatype MPI datatype of each element in the send and receive buffers.
	 * @param numberRecords Number of records to be sent to each process by process \p root,
	 *                      and number of records to be received by each process.
     * @param request MPI request that defaults to the thread_local global_request from dsys
	 */

    template<typename T>
    static inline void doScatter(T *threadLocalSendBuffer, T *&threadLocalRecvBuffer, int rootThreadID, MPI_Datatype datatype, Synchronizer *&routineReturned, int numberRecords = 1, bool copy = true, bool nonblock = false) {
        static T *processSendBuffer;
        static T *processRecvBuffer;
        static T *accumulatedRecvBuffers[MAX_THREADS_SUPPORTED];

        static Barrier threadCheckIn(number_threads_process);
        static Synchronizer bufferIsReady(1);

        int rootThread = rootThreadID % number_threads_process;
        int rootProcess = rootThreadID / number_threads_process;

        if(nonblock) {
            routineReturned = &bufferIsReady;
        }
        accumulatedRecvBuffers[seriema::thread_rank] = threadLocalRecvBuffer;
        if(seriema::thread_rank == rootThread && process_rank == rootProcess) {
            processSendBuffer = threadLocalSendBuffer;
        }

        threadCheckIn.wait();

        if(seriema::thread_rank == rootThread) {
            if(!nonblock) {
                processRecvBuffer = new T[numberRecords / number_processes];
                MPIHelper::scatter(processSendBuffer, processRecvBuffer, rootProcess, datatype, numberRecords / number_processes);
            }
            else {
                Synchronizer *pointerToBufferIsReady = &bufferIsReady;
                thread waiter([datatype, numberRecords, pointerToBufferIsReady, rootProcess, rootThread, copy]() {
                    processRecvBuffer = new T[numberRecords / number_processes];

                    MPIHelper::scatter(processSendBuffer, processRecvBuffer, rootProcess, datatype, numberRecords / number_processes);
                    int numRecordsPerThread = numberRecords / number_threads;
                    for(auto t = 0; t < number_threads_process; t++) {
                        if(true) { //TODO: Fix when we do black box reference counting
                            for(auto i = 0; i < numRecordsPerThread; ++i) {
                                accumulatedRecvBuffers[t][i] = processRecvBuffer[t * numRecordsPerThread + i];
                            }
                        }
                        else {
                            accumulatedRecvBuffers[t] = &processRecvBuffer[t * numRecordsPerThread];
                        }
                    }
                    pointerToBufferIsReady->decrease();
                });
                waiter.detach();
            }
        }
        if(!nonblock) {
            if(copy) {
                int numRecordsPerThread = numberRecords / number_threads;
                for(auto i = 0; i < numRecordsPerThread; i++) {
                    threadLocalRecvBuffer[i] = processRecvBuffer[seriema::thread_rank * numRecordsPerThread + i];
                }
            }
            else {
                threadLocalRecvBuffer = &processRecvBuffer[seriema::thread_rank * numberRecords / number_threads];
            }
        }
    }

    /**
	 * Performs an MPI Scatter operation.
	 * If numberRecordsExceeds INT_MAX, an alternative datatype is created to avoid overflow.
	 *
	 * @param sendBuffer Location of the sent data (\p numberRecords for each process).
	 * @param recvBuffer Location of the received data (\p numberRecords).
	 * @param root The process for which \p sendBuffer is valid.
	 * @param datatype MPI datatype of each element in the send and receive buffers.
	 * @param numberRecords Number of records to be sent to each process by process \p root,
	 *                      and number of records to be received by each process.
	 */
    template<typename T>
    static inline void scatter(T *threadLocalSendBuffer, T *&threadLocalRecvBuffer, int rootThreadID, MPI_Datatype datatype, Synchronizer *&addr, uint64_t numberRecords = 1, bool copy = true, bool nonblock = false) {
        if(numberRecords <= INT_MAX) {
            return doScatter(threadLocalSendBuffer, threadLocalRecvBuffer, rootThreadID, datatype, addr, static_cast<int>(numberRecords), copy, nonblock);
        }

        MPI_Datatype newDatatype;

        getNewDatatype(numberRecords, datatype, &newDatatype);

        doScatter(threadLocalSendBuffer, threadLocalRecvBuffer, rootThreadID, newDatatype, addr, 1, copy, nonblock);
    }

    /**
	 * Performs an MPI Gather operation.
	 *
	 * @param sendBuffer Location of the sent data (\p numberRecords).
	 * @param recvBuffer Location of the received data (\p numberRecords for each process).
	 * @param root The process for which \p recvBuffer is valid.
	 * @param datatype MPI datatype of each element in the send and receive buffers.
     * @param request MPI request that defaults to the thread_local global_request from dsys.
	 * @param numberRecords Number of records to be received from each thread, gathered locally and then gathered across process by process \p root,
	 *                      and number of records to be sent by each thread.
	 */
    template<typename T>
    static inline void doGather(T *threadLocalBuffer, T *threadLocalRecvBuffer, int rootThreadID, MPI_Datatype datatype, Synchronizer *&routineReturned, int numberRecords = 1, bool nonblock = false) {
        static T *processLocalBuffer[MAX_THREADS_SUPPORTED];
        static T *recvBuffer;

        static Barrier threadCheckIn(number_threads_process);
        static Synchronizer bufferIsReady(1);

        processLocalBuffer[seriema::thread_rank] = threadLocalBuffer;
        if(nonblock) {
            routineReturned = &bufferIsReady;
        }

        threadCheckIn.wait();

        int rootThread = rootThreadID % number_threads_process;
        int rootProcess = rootThreadID / number_threads_process;

        if(seriema::thread_rank == rootThread) {
            if(!nonblock) {
                int numberRecordsInProcess = numberRecords * number_threads_process;
                T *sendBuffer = new T[numberRecordsInProcess];

                for(auto i = 0; i < numberRecordsInProcess; ++i) {
                    sendBuffer[i] = processLocalBuffer[i / numberRecords][i % numberRecords];
                }

                MPIHelper::gather(sendBuffer, threadLocalRecvBuffer, rootProcess, datatype, numberRecordsInProcess);
            }
            else {
                recvBuffer = threadLocalRecvBuffer;
                Synchronizer *pointerToBufferIsReady = &bufferIsReady;
                thread waiter([datatype, numberRecords, pointerToBufferIsReady, rootProcess, rootThread]() {
                    int numberRecordsInProcess = numberRecords * number_threads_process;
                    T *sendBuffer = new T[numberRecordsInProcess];

                    for(auto i = 0; i < numberRecordsInProcess; ++i) {
                        sendBuffer[i] = processLocalBuffer[i / numberRecords][i % numberRecords];
                    }

                    MPIHelper::gather(sendBuffer, recvBuffer, rootProcess, datatype, numberRecordsInProcess);

                    pointerToBufferIsReady->decrease();
                });
                waiter.detach();
            }
        }
    }

    /**
	 * Performs an MPI Gather operation.
	 * If numberRecordsExceeds INT_MAX, an alternative datatype is created to avoid overflow.
	 *
	 * @param sendBuffer Location of the sent data (\p numberRecords).
	 * @param recvBuffer Location of the received data (\p numberRecords for each process).
	 * @param root The process for which \p recvBuffer is valid.
	 * @param datatype MPI datatype of each element in the send and receive buffers.
	 * @param numberRecords Number of records to be received from each process by process \p root,
	 *                      and number of records to be sent by each process.
	 */
    template<typename T>
    static inline void gather(T *threadLocalBuffer, T *recvBuffer, int rootThreadID, MPI_Datatype datatype, Synchronizer *&addr, uint64_t numberRecords = 1, bool nonblock = false) {
        if(numberRecords <= INT_MAX) {
            return doGather(threadLocalBuffer, recvBuffer, rootThreadID, datatype, addr, static_cast<int>(numberRecords), nonblock);
        }

        MPI_Datatype newDatatype;

        getNewDatatype(numberRecords, datatype, &newDatatype);

        doGather(threadLocalBuffer, recvBuffer, rootThreadID, newDatatype, addr, 1, nonblock);
    }

    /**
	 * Performs an MPI AllGather operation.
	 *
	 * @param sendBuffer Location of the sent data (\p numberRecords).
	 * @param recvBuffer Location of the received data (\p numberRecords for each process).
	 * @param datatype MPI datatype of each element in the send and receive buffers.
     * @param request MPI request that defaults to the thread_local global_request from dsys
	 * @param numberRecords Number of records to be received from each process,
	 *                      and number of records to be sent by each process.
	 */
    template<typename T>
    static inline void doAllGather(T *threadLocalSendBuffer, T *&threadLocalRecvBuffer, MPI_Datatype datatype, Synchronizer *&routineReturned, int numberRecords = 1, bool copy = true, bool nonblock = false) {
        static T *processSendBuffer[MAX_THREADS_SUPPORTED];
        static T *processRecvBuffers[MAX_THREADS_SUPPORTED];
        static T *processRecvBuffer;

        static Barrier threadCheckIn(number_threads_process);
        static Synchronizer bufferIsReady(1);

        if(nonblock) {
            routineReturned = &bufferIsReady;
        }

        processSendBuffer[seriema::thread_rank] = threadLocalSendBuffer;
        processRecvBuffers[seriema::thread_rank] = threadLocalRecvBuffer;

        threadCheckIn.wait();

        if(seriema::thread_rank == 0) {
            if(!nonblock) {
                T *accumulatedData = new T[numberRecords * number_threads_process];
                processRecvBuffer = new T[numberRecords * number_threads];

                for(auto i = 0; i < numberRecords * number_threads_process; ++i) {
                    accumulatedData[i] = processSendBuffer[i / numberRecords][i % numberRecords];
                }

                MPIHelper::allGather(accumulatedData, processRecvBuffer, datatype, numberRecords * number_threads_process);
            }
            else {
                Synchronizer *pointerToBufferIsReady = &bufferIsReady;
                thread waiter([numberRecords, datatype, pointerToBufferIsReady, copy]() {
                    T *accumulatedData = new T[numberRecords * number_threads_process];
                    processRecvBuffer = new T[numberRecords * number_threads];

                    for(auto i = 0; i < numberRecords * number_threads_process; ++i) {
                        accumulatedData[i] = processSendBuffer[i / numberRecords][i % numberRecords];
                    }

                    MPIHelper::allGather(accumulatedData, processRecvBuffer, datatype, numberRecords * number_threads_process);

                    for(auto t = 0; t < seriema::number_threads_process; ++t) {
                        T *curr = processRecvBuffers[t];

                        if(true) { //TODO: Fix when we do black box reference counting
                            for(auto i = 0; i < numberRecords * number_threads; ++i) {
                                curr[i] = processRecvBuffer[i];
                            }
                        }
                        else {
                            processRecvBuffers[t] = processRecvBuffer;
                        }
                    }
                    pointerToBufferIsReady->decrease();
                });
                waiter.detach();
            }
        }
        if(!nonblock) {
            threadCheckIn.wait();

            if(copy) {
                for(auto i = 0; i < numberRecords * number_threads; ++i) {
                    threadLocalRecvBuffer[i] = processRecvBuffer[i];
                }
            }
            else {
                threadLocalRecvBuffer = processRecvBuffer;
            }
        }
    }

    /**
	 * Performs an MPI AllGather operation.
	 * If numberRecordsExceeds INT_MAX, an alternative datatype is created to avoid overflow.
	 *
	 * @param sendBuffer Location of the sent data (\p numberRecords).
	 * @param recvBuffer Location of the received data (\p numberRecords for each process).
	 * @param datatype MPI datatype of each element in the send and receive buffers.
	 * @param numberRecords Number of records to be received from each process,
	 *                      and number of records to be sent by each process.
	 */
    template<typename T>
    static inline void allGather(T *threadLocalSendBuffer, T *&threadLocalRecvBuffer, MPI_Datatype datatype, Synchronizer *&addr, uint64_t numberRecords = 1, bool copy = true, bool nonblock = false) {
        if(numberRecords <= INT_MAX) {
            return doAllGather(threadLocalSendBuffer, threadLocalRecvBuffer, datatype, addr, static_cast<int>(numberRecords), copy, nonblock);
        }

        MPI_Datatype newDatatype;

        getNewDatatype(numberRecords, datatype, &newDatatype);

        doAllGather(threadLocalSendBuffer, threadLocalRecvBuffer, newDatatype, addr, 1, copy, nonblock);
    }

    /**
	 * Performs an MPI All-to-All operation.  Does a gather on the threads and then a process All - to - All
	 *
	 * @param sendBuffer Location of the sent data (\p numberRecords for each process).
	 * @param recvBuffer Location of the received data (\p numberRecords for each process).
	 * @param datatype MPI datatype of each element in the send and receive buffers.
     * @param request MPI request that defaults to the thread_local global_request from dsys
	 * @param numberRecords Number of records to be received from each process,
	 *                      and number of records to be sent by each process.
	 */
    template<typename T>
    static inline void doAllToAll(T *threadLocalSendBuffer, T *&threadLocalRecvBuffer, MPI_Datatype datatype, Synchronizer *&bufferIsReady, int numberRecords = 1, bool copy = true, bool nonblock = false) {
        // threadCheckIn.wait();
        static T *processLocalSendBuffers[MAX_THREADS_SUPPORTED];
        static T *accumulatedRecvBuffers[MAX_THREADS_SUPPORTED];
        static T *processLocalRecvBuffer;

        static Barrier threadCheckIn(number_threads_process);
        static Synchronizer threadsWereParsed(1);

        processLocalSendBuffers[seriema::thread_rank] = threadLocalSendBuffer;
        accumulatedRecvBuffers[seriema::thread_rank] = threadLocalRecvBuffer;

        if(nonblock) {
            bufferIsReady = &threadsWereParsed;
        }

        threadCheckIn.wait();

        if(seriema::thread_rank == 0) {
            processLocalRecvBuffer = new T[numberRecords * number_threads_process];
            if(!nonblock) {
                T *accumulatedData = new T[numberRecords * number_threads_process];

                for(auto i = 0; i < numberRecords * number_threads_process; ++i) {
                    accumulatedData[i] = processLocalSendBuffers[i % number_threads_process][i / number_threads_process];
                }

                MPIHelper::doAllToAll(accumulatedData, processLocalRecvBuffer, datatype, numberRecords * number_threads_process / number_processes);
            }
            else {
                Synchronizer *pointerThreadsWereParsed = &threadsWereParsed;
                T *accumulatedData = new T[numberRecords * number_threads_process];
                thread waiter([&, numberRecords, datatype, pointerThreadsWereParsed, copy]() {
                    // T *recvBuffer = new T[numberRecords * number_threads_process];

                    for(auto i = 0; i < numberRecords * number_threads_process; ++i) {
                        accumulatedData[i] = processLocalSendBuffers[i % number_threads_process][i / number_threads_process];
                    }

                    MPIHelper::doAllToAll(accumulatedData, processLocalRecvBuffer, datatype, numberRecords * number_threads_process / number_processes);

                    for(auto t = 0; t < seriema::number_threads_process; ++t) {
                        T *curr = accumulatedRecvBuffers[t];
                        if(true) { //TODO: Fix when we do black box reference counting
                            for(auto i = 0; i < numberRecords; ++i) {
                                curr[i] = processLocalRecvBuffer[t * numberRecords + i];
                            }
                        }
                        else {
                            curr = &processLocalRecvBuffer[t * numberRecords];
                        }
                    }
                    pointerThreadsWereParsed->decrease();
                });
                waiter.detach();
            }
        }
        if(!nonblock) {
            threadCheckIn.wait();
            if(copy) {
                for(auto i = 0; i < numberRecords; i++) {
                    threadLocalRecvBuffer[i] = processLocalRecvBuffer[seriema::thread_rank * numberRecords + i];
                }
            }
            else {
                threadLocalRecvBuffer = &processLocalRecvBuffer[seriema::thread_rank * numberRecords];
            }
        }
    }

    /**
	 * Performs an MPI All-to-All operation.
	 * If numberRecordsExceeds INT_MAX, an alternative datatype is created to avoid overflow.
	 *
	 * @param sendBuffer Location of the sent data (\p numberRecords for each process).
	 * @param recvBuffer Location of the received data (\p numberRecords for each process).
	 * @param datatype MPI datatype of each element in the send and receive buffers.
	 * @param numberRecords Number of records to be received from each process,
	 *                      and number of records to be sent by each process.
	 */
    template<typename T>
    static inline void allToAll(T *threadLocalSendBuffer, T *&threadLocalRecvBuffer, MPI_Datatype datatype, Synchronizer *&bufferIsReady, uint64_t numberRecords = 1, bool copy = true, bool nonblock = false) {
        if(numberRecords <= INT_MAX) {
            return doAllToAll(threadLocalSendBuffer, threadLocalRecvBuffer, datatype, bufferIsReady, static_cast<int>(numberRecords), copy, nonblock);
        }

        MPI_Datatype newDatatype;

        getNewDatatype(numberRecords, datatype, &newDatatype);

        doAllToAll(threadLocalSendBuffer, threadLocalRecvBuffer, datatype, bufferIsReady, copy, nonblock);
    }

    /**
	k * Performs an MPI All-to-All-V operation.
	 *
	 * @param sendBuffer Location of the sent data (\p sendSizes[i] records at \p sendOffsets[i] for each process).
	 * @param sendSizes Vector containing the number of objects sent to each process.
	 * @param sendOffsets Vector containing the number of objects, offset from the base, to skip and send to each process.
	 * @param recvBuffer Location of the received data (\p recvSizes[i] records at \p recvOffsets[i] for each process).
	 * @param recvSizes Vector containing the number of objects received from each process.
	 * @param recvOffsets Vector containing the number of objects, offset from the base, to skip and receive from each process.
	 * @param datatype MPI datatype of each element in the send and receive buffers.
     * @param request MPI request that defaults to the thread_local global_request from dsys
	 */
    template<typename T>
    static inline void doAllToAllV(T *threadLocalSendBuffer, int *threadLocalSendSizes, int *threadLocalSendOffsets, T *&threadLocalRecvBuffer, int *threadLocalRecvSizes, int *threadLocalRecvOffsets, MPI_Datatype datatype, Synchronizer *&routineReturned,
        bool copy = true, bool nonblock = false) {
        //Process local accumulators
        static T *processSendBuffer[MAX_THREADS_SUPPORTED];
        static int *processSendSizes[MAX_THREADS_SUPPORTED];
        static int *processRecvSizes[MAX_THREADS_SUPPORTED];
        static int *processSendOffsets[MAX_THREADS_SUPPORTED];
        static int *processRecvOffsets[MAX_THREADS_SUPPORTED];
        static T *processRecvBuffer[MAX_THREADS_SUPPORTED];

        static Barrier threadCheckIn(number_threads_process);
        static Synchronizer bufferIsReady(1);

        static T *postAllToAllVRecvBuffer;
        static int *postAllToAllVRecvSizes;
        if(nonblock) {
            routineReturned = &bufferIsReady;
        }

        processSendBuffer[seriema::thread_rank] = threadLocalSendBuffer;
        processSendSizes[seriema::thread_rank] = threadLocalSendSizes;
        processRecvSizes[seriema::thread_rank] = threadLocalRecvSizes;
        processRecvBuffer[seriema::thread_rank] = threadLocalRecvBuffer;
        processRecvOffsets[seriema::thread_rank] = threadLocalRecvOffsets;
        processSendOffsets[seriema::thread_rank] = threadLocalSendOffsets;

        threadCheckIn.wait();

        if(seriema::thread_rank == 0) {
            if(!nonblock) {
                int *threadSpecificSendSizes = new int[number_threads_process * number_threads];
                int processSpecificSendSizes[MAX_PROCESSES_SUPPORTED] = {0};
                int *recvSendSizes = new int[number_threads * number_threads_process];

                int totalSend = 0;
                for(auto i = 0; i < number_threads; i++) {
                    for(auto j = 0; j < number_threads_process; j++) {
                        threadSpecificSendSizes[i * number_threads_process + j] = processSendSizes[j][i];
                        totalSend += processSendSizes[j][i];
                        processSpecificSendSizes[seriema::get_process_rank(i)] += processSendSizes[j][i];
                    }
                }

                MPIHelper::allToAll(threadSpecificSendSizes, recvSendSizes, MPI_INT, number_threads * number_threads_process / number_processes);

                int flattenedSendOffsets[MAX_PROCESSES_SUPPORTED] = {0};
                int offsetSoFar = 0;

                for(auto i = 0; i < number_processes; ++i) {
                    flattenedSendOffsets[i] = offsetSoFar;
                    offsetSoFar += processSpecificSendSizes[i];
                }

                int flattenedRecvSizes[MAX_PROCESSES_SUPPORTED] = {0};
                int recvBufferSize = 0;
                for(auto i = 0; i < number_threads * number_threads_process; i++) {
                    recvBufferSize += recvSendSizes[i];
                    flattenedRecvSizes[i / (number_threads_process * number_threads_process)] += recvSendSizes[i];
                }

                T *recvBuffer = new T[recvBufferSize];

                int flattenedRecvOffsets[MAX_PROCESSES_SUPPORTED] = {0};
                int flattenedRecvOffset = 0;
                for(auto i = 0; i < number_processes; ++i) {
                    flattenedRecvOffsets[i] = flattenedRecvOffset;
                    flattenedRecvOffset += flattenedRecvSizes[i];
                }

                T *flattenedSendBuffer = new T[totalSend];
                int flattenedSendBufferIndex = 0;

                for(auto i = 0; i < number_threads; ++i) {
                    for(auto j = 0; j < number_threads_process; ++j) {
                        int sendCount = processSendSizes[j][i];
                        int offset = processSendOffsets[j][i];
                        for(auto k = 0; k < sendCount; ++k) {
                            flattenedSendBuffer[flattenedSendBufferIndex] = processSendBuffer[j][offset + k];
                            flattenedSendBufferIndex++;
                        }
                    }
                }

                MPIHelper::doAllToAllV(flattenedSendBuffer, processSpecificSendSizes, flattenedSendOffsets, recvBuffer, flattenedRecvSizes, flattenedRecvOffsets, datatype);

                postAllToAllVRecvBuffer = recvBuffer;
                postAllToAllVRecvSizes = recvSendSizes;
            }
            else {
                Synchronizer *pointerToBufferIsReady = &bufferIsReady;
                thread waiter([pointerToBufferIsReady, datatype]() {
                    int *threadSpecificSendSizes = new int[number_threads_process * number_threads];
                    int processSpecificSendSizes[MAX_PROCESSES_SUPPORTED] = {0};
                    int *recvSendSizes = new int[number_threads * number_threads_process];

                    int totalSend = 0;
                    for(auto i = 0; i < number_threads; i++) {
                        for(auto j = 0; j < number_threads_process; j++) {
                            threadSpecificSendSizes[i * number_threads_process + j] = processSendSizes[j][i];
                            totalSend += processSendSizes[j][i];
                            processSpecificSendSizes[seriema::get_process_rank(i)] += processSendSizes[j][i];
                        }
                    }

                    MPIHelper::allToAll(threadSpecificSendSizes, recvSendSizes, MPI_INT, number_threads * number_threads_process / number_processes);

                    int flattenedSendOffsets[MAX_PROCESSES_SUPPORTED] = {0};
                    int offsetSoFar = 0;

                    for(auto i = 0; i < number_processes; ++i) {
                        flattenedSendOffsets[i] = offsetSoFar;
                        offsetSoFar += processSpecificSendSizes[i];
                    }

                    int flattenedRecvSizes[MAX_PROCESSES_SUPPORTED] = {0};
                    int recvBufferSize = 0;
                    for(auto i = 0; i < number_threads * number_threads_process; i++) {
                        recvBufferSize += recvSendSizes[i];
                        flattenedRecvSizes[i / (number_threads_process * number_threads_process)] += recvSendSizes[i];
                    }

                    T *recvBuffer = new T[recvBufferSize];

                    int flattenedRecvOffsets[MAX_PROCESSES_SUPPORTED] = {0};
                    int flattenedRecvOffset = 0;
                    for(auto i = 0; i < number_processes; ++i) {
                        flattenedRecvOffsets[i] = flattenedRecvOffset;
                        flattenedRecvOffset += flattenedRecvSizes[i];
                    }

                    T *flattenedSendBuffer = new T[totalSend];
                    int flattenedSendBufferIndex = 0;

                    for(auto i = 0; i < number_threads; ++i) {
                        for(auto j = 0; j < number_threads_process; ++j) {
                            int sendCount = processSendSizes[j][i];
                            int offset = processSendOffsets[j][i];
                            for(auto k = 0; k < sendCount; ++k) {
                                flattenedSendBuffer[flattenedSendBufferIndex] = processSendBuffer[j][offset + k];
                                flattenedSendBufferIndex++;
                            }
                        }
                    }

                    MPIHelper::doAllToAllV(flattenedSendBuffer, processSpecificSendSizes, flattenedSendOffsets, recvBuffer, flattenedRecvSizes, flattenedRecvOffsets, datatype);
                    int recvBufferPos = 0;
                    if(true) { //TODO: fix when we do black box reference counting
                        for(auto t = 0; t < number_threads_process; t++) {
                            for(auto i = 0; i < number_processes; ++i) {
                                for(auto j = 0; j < number_threads_process; ++j) {
                                    for(auto k = 0; k < number_threads_process; ++k) {
                                        int recvSizeOffset = i * number_threads_process * number_threads_process + j * number_threads_process + k;
                                        if(j == t) {
                                            int senderThreadID = i * number_threads_process + k;

                                            int myStart = processRecvOffsets[t][senderThreadID];
                                            int myMaxSize = processRecvSizes[t][senderThreadID];
                                            int sentSize = recvSendSizes[recvSizeOffset];
                                            int toRecv = std::min(myMaxSize, sentSize);
                                            if(true) { //TODO: fix when we do black box reference counting
                                                for(auto l = 0; l < toRecv; ++l) {
                                                    processRecvBuffer[t][myStart + l] = recvBuffer[recvBufferPos + l];
                                                }
                                            }
                                            else {
                                                processRecvOffsets[t][senderThreadID] = recvBufferPos;
                                                processRecvSizes[t][senderThreadID] = sentSize;
                                            }
                                        }
                                        recvBufferPos += recvSendSizes[recvSizeOffset];
                                    }
                                }
                            }
                        }
                    }
                    else {
                        //TODO
                    }
                    pointerToBufferIsReady->decrease();
                });
                waiter.detach();
            }
        }
        if(!nonblock) {
            threadCheckIn.wait();
            int recvBufferPos = 0;
            if(!copy) {
                threadLocalRecvBuffer = postAllToAllVRecvBuffer;
            }
            else {
                for(auto i = 0; i < number_processes; ++i) {
                    for(auto j = 0; j < number_threads_process; ++j) {
                        for(auto k = 0; k < number_threads_process; ++k) {
                            int recvSizeOffset = i * number_threads_process * number_threads_process + j * number_threads_process + k;
                            if(j == seriema::thread_rank) {
                                int senderThreadID = i * number_threads_process + k;

                                int myStart = threadLocalRecvOffsets[senderThreadID];
                                int myMaxSize = threadLocalRecvSizes[senderThreadID];
                                int sentSize = postAllToAllVRecvSizes[recvSizeOffset];
                                int toRecv = std::min(myMaxSize, sentSize);
                                if(copy) {
                                    for(auto l = 0; l < toRecv; ++l) {
                                        threadLocalRecvBuffer[myStart + l] = postAllToAllVRecvBuffer[recvBufferPos + l];
                                    }
                                }
                                else {
                                    threadLocalRecvOffsets[senderThreadID] = recvBufferPos;
                                    threadLocalRecvSizes[senderThreadID] = sentSize;
                                }
                            }
                            recvBufferPos += postAllToAllVRecvSizes[recvSizeOffset];
                        }
                    }
                }
            }
        }
    }

    /**
	 * Performs an MPI All-to-All-V operation.
	 * If numberRecordsExceeds INT_MAX, an alternative datatype is created to avoid overflow.
	 *
	 * @param sendBuffer Location of the sent data (\p sendSizes[i] records at \p sendOffsets[i] for each process).
	 * @param sendSizes Vector containing the number of objects sent to each process.
	 * @param sendOffsets Vector containing the number of objects, offset from the base, to skip and send to each process.
	 * @param recvBuffer Location of the received data (\p recvSizes[i] records at \p recvOffsets[i] for each process).
	 * @param recvSizes Vector containing the number of objects received from each process.
	 * @param recvOffsets Vector containing the number of objects, offset from the base, to skip and receive from each process.
	 * @param datatype MPI datatype of each element in the send and receive buffers.
	 */
    static inline void allToAllV(SerializationBuffer &sendBuffer, uint64_t *sendSizes, uint64_t *sendOffsets, SerializationBuffer &recvBuffer, uint64_t *recvSizes, uint64_t *recvOffsets, Synchronizer *&addr) {
        if(sendBuffer.size <= INT_MAX && recvBuffer.size <= INT_MAX) {
            return smallAllToAllV(sendBuffer.data, sendSizes, sendOffsets, recvBuffer.data, recvSizes, recvOffsets, MPI_PACKED, addr);
        }

        largeAllToAllV(sendBuffer.data, sendSizes, sendOffsets, recvBuffer.data, recvSizes, recvOffsets, MPI_PACKED);
    }

    /**
	 * Performs an MPI All-to-All-V operation.
	 *
	 * @param sendBuffer Location of the sent data (\p sendSizes[i] records at \p sendOffsets[i] for each process).
	 * @param sendSizes Vector containing the number of objects sent to each process.
	 * @param sendOffsets Vector containing the number of objects, offset from the base, to skip and send to each process.
	 * @param recvBuffer Location of the received data (\p recvSizes[i] records at \p recvOffsets[i] for each process).
	 * @param recvSizes Vector containing the number of objects received from each process.
	 * @param recvOffsets Vector containing the number of objects, offset from the base, to skip and receive from each process.
	 * @param datatype MPI datatype of each element in the send and receive buffers.
	 */
    template<typename T>
    static inline void smallAllToAllV(T *sendBuffer, uint64_t *sendSizes, uint64_t *sendOffsets, T *recvBuffer, uint64_t *recvSizes, uint64_t *recvOffsets, MPI_Datatype datatype, Synchronizer *&addr) {
        vector<int> intSendSizes(number_threads);
        vector<int> intRecvSizes(number_threads);

        vector<int> intSendOffsets(number_threads);
        vector<int> intRecvOffsets(number_threads);

        for(int i = 0; i < number_threads; i++) {
            intSendSizes[i] = static_cast<int>(sendSizes[i]);
            intRecvSizes[i] = static_cast<int>(recvSizes[i]);

            intSendOffsets[i] = static_cast<int>(sendOffsets[i]);
            intRecvOffsets[i] = static_cast<int>(recvOffsets[i]);
        }

        doAllToAllV(sendBuffer, &intSendSizes[0], &intSendOffsets[0], recvBuffer, &intRecvSizes[0], &intRecvOffsets[0], datatype, addr);
    }

    /**
	 * Performs an MPI All-to-All-V operation.
	 * An alternative datatype is created to avoid overflow.
	 *
	 * @param sendBuffer Location of the sent data (\p sendSizes[i] records at \p sendOffsets[i] for each process).
	 * @param sendSizes Vector containing the number of objects sent to each process.
	 * @param sendOffsets Vector containing the number of objects, offset from the base, to skip and send to each process.
	 * @param recvBuffer Location of the received data (\p recvSizes[i] records at \p recvOffsets[i] for each process).
	 * @param recvSizes Vector containing the number of objects received from each process.
	 * @param recvOffsets Vector containing the number of objects, offset from the base, to skip and receive from each process.
	 * @param datatype MPI datatype of each element in the send and receive buffers.
     * @param request MPI request that defaults to the thread_local global_request from dsys.
	 */
    template<typename T>
    static void largeAllToAllV(T *sendBuffer, uint64_t *sendSizes, uint64_t *sendOffsets, T *recvBuffer, uint64_t *recvSizes, uint64_t *recvOffsets, MPI_Datatype datatype, MPI_Request &request = seriema::global_request) {
        vector<int> newSendSizes(number_threads);
        vector<MPI_Datatype> newSendTypes(number_threads);
        vector<MPI_Aint> newSendOffsets(number_threads);

        vector<int> newRecvSizes(number_threads);
        vector<MPI_Datatype> newRecvTypes(number_threads);
        vector<MPI_Aint> newRecvOffsets(number_threads);

        MPI_Aint typeSize;
        getDatatypeExtent(datatype, &typeSize);

        for(int i = 0; i < number_threads; i++) {
            newSendSizes[i] = 1;
            newRecvSizes[i] = 1;

            getNewDatatype(sendSizes[i], datatype, &newSendTypes[i]);
            getNewDatatype(recvSizes[i], datatype, &newRecvTypes[i]);

            newSendOffsets[i] = sendOffsets[i] * typeSize;
            newRecvOffsets[i] = recvOffsets[i] * typeSize;
        }

        MPI_Comm completeGraph;

        createCompleteGraph(MPI_COMM_WORLD, &completeGraph);

        MPI_CHECK(MPI_Ineighbor_alltoallw(sendBuffer, &newSendSizes[0], &newSendOffsets[0], &newSendTypes[0], recvBuffer, &newRecvSizes[0], &newRecvOffsets[0], &newRecvTypes[0], completeGraph, &request));

        MPI_Comm_free(&completeGraph);

        for(int i = 0; i < number_threads; i++) {
            MPI_Type_free(&newSendTypes[i]);
            MPI_Type_free(&newRecvTypes[i]);
        }
    }

    /**
	 * Performs an MPI All-to-All-W operation.
	 *
	 * @param sendBuffer Location of the sent data (\p sendSizes[i] records of type \p sendTypes[i] at \p sendOffsets[i] for each process).
	 * @param sendSizes Vector containing the number of objects sent to each process.
	 * @param sendOffsets Vector containing the byte offset from the base, to skip and send to each process.
	 * @param recvBuffer Location of the received data (\p recvSizes[i] records of type \p recvTypes[i] at \p recvOffsets[i] for each process).
	 * @param recvSizes Vector containing the number of objects received from each process.
	 * @param recvOffsets Vector containing the byte offset from the base, to skip and receive from each process.
	 * @param datatype MPI datatype of each element in the send and receive buffers.
     * @param request MPI request that defaults to the thread_local global_request from dsys
	 */
    // template<typename T>
    // static inline void doAllToAllW(T *sendBuffer, int *sendSizes, int *sendOffsets, MPI_Datatype *sendTypes, T *recvBuffer, int *recvSizes, int *recvOffsets, MPI_Datatype *recvTypes, MPI_Request &request = seriema::global_request) {
    //     //HM: TODO
    // }

    /**
	 * Performs an MPI All-to-All-W operation.
	 * If numberRecordsExceeds INT_MAX, an alternative datatype is created to avoid overflow.
	 *
	 * @param sendBuffer Location of the sent data (\p sendSizes[i] records of type \p sendTypes[i] at \p sendOffsets[i] for each process).
	 * @param sendSizes Vector containing the number of objects sent to each process.
	 * @param sendOffsets Vector containing the byte offset from the base, to skip and send to each process.
	 * @param recvBuffer Location of the received data (\p recvSizes[i] records of type \p recvTypes[i] at \p recvOffsets[i] for each process).
	 * @param recvSizes Vector containing the number of objects received from each process.
	 * @param recvOffsets Vector containing the byte offset from the base, to skip and receive from each process.
	 * @param datatype MPI datatype of each element in the send and receive buffers.
	 */
    // static inline void allToAllW(SerializationBuffer &sendBuffer, uint64_t *sendSizes, uint64_t *sendOffsets, MPI_Datatype *sendTypes, SerializationBuffer &recvBuffer, uint64_t *recvSizes, uint64_t *recvOffsets, MPI_Datatype *recvTypes) {
    //     if(sendBuffer.size <= INT_MAX && recvBuffer.size <= INT_MAX) {
    //         return smallAllToAllW<char>(sendBuffer.data, sendSizes, sendOffsets, sendTypes, recvBuffer.data, recvSizes, recvOffsets, recvTypes);
    //     }

    //     largeAllToAllW<char>(sendBuffer.data, sendSizes, sendOffsets, sendTypes, recvBuffer.data, recvSizes, recvOffsets, recvTypes);
    // }

    /**
	 * Performs an MPI All-to-All-W operation.
	 *
	 * @param sendBuffer Location of the sent data (\p sendSizes[i] records of type \p sendTypes[i] at \p sendOffsets[i] for each process).
	 * @param sendSizes Vector containing the number of objects sent to each process.
	 * @param sendOffsets Vector containing the byte offset from the base, to skip and send to each process.
	 * @param recvBuffer Location of the received data (\p recvSizes[i] records of type \p recvTypes[i] at \p recvOffsets[i] for each process).
	 * @param recvSizes Vector containing the number of objects received from each process.
	 * @param recvOffsets Vector containing the byte offset from the base, to skip and receive from each process.
	 * @param datatype MPI datatype of each element in the send and receive buffers.
	 */
    // template<typename T>
    // static inline void smallAllToAllW(T *sendBuffer, uint64_t *sendSizes, uint64_t *sendOffsets, MPI_Datatype *sendTypes, T *recvBuffer, uint64_t *recvSizes, uint64_t *recvOffsets, MPI_Datatype *recvTypes) {
    //     vector<int> intSendSizes(number_threads);
    //     vector<int> intRecvSizes(number_threads);

    //     vector<int> intSendOffsets(number_threads);
    //     vector<int> intRecvOffsets(number_threads);

    //     for(int i = 0; i < number_threads; i++) {
    //         intSendSizes[i] = static_cast<int>(sendSizes[i]);
    //         intRecvSizes[i] = static_cast<int>(recvSizes[i]);

    //         intSendOffsets[i] = static_cast<int>(sendOffsets[i]);
    //         intRecvOffsets[i] = static_cast<int>(recvOffsets[i]);
    //     }

    //     doAllToAllW<T>(sendBuffer, &intSendSizes[0], &intSendOffsets[0], sendTypes, recvBuffer, &intRecvSizes[0], &intRecvOffsets[0], recvTypes);
    // }

    /**
	 * Performs an MPI All-to-All-W operation.
	 * An alternative datatype is created to avoid overflow.
	 *
	 * @param sendBuffer Location of the sent data (\p sendSizes[i] records of type \p sendTypes[i] at \p sendOffsets[i] for each process).
	 * @param sendSizes Vector containing the number of objects sent to each process.
	 * @param sendOffsets Vector containing the byte offset from the base, to skip and send to each process.
	 * @param recvBuffer Location of the received data (\p recvSizes[i] records of type \p recvTypes[i] at \p recvOffsets[i] for each process).
	 * @param recvSizes Vector containing the number of objects received from each process.
	 * @param recvOffsets Vector containing the byte offset from the base, to skip and receive from each process.
	 * @param datatype MPI datatype of each element in the send and receive buffers.
     * @param request MPI request that defaults to the thread_local global_request from dsys
	 */
    // template<typename T>
    // static inline void largeAllToAllW(T *sendBuffer, uint64_t *sendSizes, uint64_t *sendOffsets, MPI_Datatype *sendTypes, T *recvBuffer, uint64_t *recvSizes, uint64_t *recvOffsets, MPI_Datatype *recvTypes, MPI_Request &request = seriema::global_request) {
    //     vector<int> newSendSizes(number_threads);
    //     vector<MPI_Datatype> newSendTypes(number_threads);
    //     vector<MPI_Aint> newSendOffsets(number_threads);

    //     vector<int> newRecvSizes(number_threads);
    //     vector<MPI_Datatype> newRecvTypes(number_threads);
    //     vector<MPI_Aint> newRecvOffsets(number_threads);

    //     for(int i = 0; i < number_threads; i++) {
    //         newSendSizes[i] = 1;
    //         newRecvSizes[i] = 1;

    //         getNewDatatype(sendSizes[i], sendTypes[i], &newSendTypes[i]);
    //         getNewDatatype(recvSizes[i], recvTypes[i], &newRecvTypes[i]);

    //         newSendOffsets[i] = sendOffsets[i];
    //         newRecvOffsets[i] = recvOffsets[i];
    //     }

    //     MPI_Comm completeGraph;

    //     createCompleteGraph(MPI_COMM_WORLD, &completeGraph);

    //     MPI_CHECK(MPI_Ineighbor_alltoallw(sendBuffer, &newSendSizes[0], &newSendOffsets[0], &newSendTypes[0], recvBuffer, &newRecvSizes[0], &newRecvOffsets[0], &newRecvTypes[0], completeGraph, &request));

    //     MPI_Comm_free(&completeGraph);

    //     for(int i = 0; i < number_threads; i++) {
    //         MPI_Type_free(&newSendTypes[i]);
    //         MPI_Type_free(&newRecvTypes[i]);
    //     }
    // }

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

    template<typename T>
    static inline void broadcast(T *&threadBuffer, int rootThreadID, MPI_Datatype datatype, Synchronizer *&bufferIsReady, int numRecords = 1, bool copy = true, bool nonblock = false) {
        static Barrier threadCheckIn(number_threads_process);
        static Synchronizer routineReturned(1);
        if(nonblock) {
            bufferIsReady = &routineReturned;
        }

        static T *allThreadsBuffer[MAX_THREADS_SUPPORTED];
        allThreadsBuffer[seriema::thread_rank] = threadBuffer;

        static T *processBuffer;

        threadCheckIn.wait();

        int rootThread = rootThreadID % number_threads_process;
        int rootProcess = rootThreadID / number_threads_process;

        if(seriema::thread_rank == rootThread % number_threads_process) {
            processBuffer = threadBuffer;
            if(!nonblock) {
                MPIHelper::broadcast(processBuffer, datatype, rootProcess, numRecords);
            }
            else {
                Synchronizer *pointerRoutineReturned = &routineReturned;
                thread waiter([&, numRecords, rootProcess, datatype, pointerRoutineReturned, copy]() {
                    MPIHelper::broadcast(processBuffer, datatype, rootProcess, numRecords);
                    for(auto t = 0; t < seriema::number_threads_process; ++t) {
                        T *curr = allThreadsBuffer[t];
                        if(copy) {
                            for(auto i = 0; i < numRecords; ++i) {
                                curr[i] = processBuffer[i];
                            }
                        }
                        else {
                            curr = processBuffer;
                        }
                    }
                    pointerRoutineReturned->decrease();
                });
                waiter.detach();
            }
        }
        if(seriema::thread_rank != rootThread % seriema::number_threads_process && !nonblock) {
            threadCheckIn.wait();
            if(copy) {
                for(int i = 0; i < numRecords; i++) {
                    threadBuffer[i] = processBuffer[i];
                }
            }
            else {
                threadBuffer = processBuffer;
            }
        }
    }

    /**
	 * Reduces #cores elements using the reduce function \t G.
	 *
	 * @return The value globally reduced by \t G.
	 */
    template<typename T, typename G>
    static T reduce(T *elements, int count, T &initialValue, G reduceGlobal) {
        vector<T> locals(number_threads);

        doAllToAll(&initialValue, &locals[0]);

        T result = accumulate(locals.begin(), locals.end(), initialValue, reduceGlobal);

        return result;
    }

    /**
	 * Reduces \p count elements from vector \p elements using the reduce function \t F.
	 * Then, reduces #cores elements using the reduce function \t G.
	 *
	 * @return The value globally reduced by \t G.
	 */
    template<typename T, typename F, typename G>
    static T reduce(T *elements, int count, T &initialValue, F reduceLocal, G reduceGlobal) {
        T local = accumulate(elements, elements + count, initialValue, reduceLocal);

        vector<T> locals(number_threads);

        doAllToAll(&local, &locals[0]);

        T result = accumulate(locals.begin(), locals.end(), initialValue, reduceGlobal);

        return result;
    }

    /**
	 * Helper struct used to make all-to-all communication easier.
	 */
    struct AllToAllContext {
        /// Size of the data sent to each core
        vector<uint64_t> sendSizes;
        /// Size of the data received from each core
        vector<uint64_t> recvSizes;

        /// Offsets in the send buffer for the data associated with each core
        vector<uint64_t> sendOffsets;

        /// Offsets in the receive buffer for the data associated with each core
        vector<uint64_t> recvOffsets;

        /// Total number of bytes sent in this core as part of this all-to-all setting
        uint64_t sendSizeTotal;

        /// Total number of bytes received in this core as part of this all-to-all setting
        uint64_t recvSizeTotal;

        /// Master send buffer used in the all-to-all setting
        SerializationBuffer sendBuffer;

        /// Master receive buffer used in the all-to-all setting
        SerializationBuffer recvBuffer;

        /// Slave send buffers, one per core, used in this all-to-all setting
        vector<SerializationBuffer> sendBuffers;

        /// Slave receive buffers, one per core, used in this all-to-all setting
        vector<SerializationBuffer> recvBuffers;

        /// If true, the total send size is initialized by the sum of each core's send size,
        bool dynamic;

        /**
		 * Constructor for the dynamic setting, which means that the user
		 * allocates and manipulates send data in the per-core send buffers.
		 */
        AllToAllContext() : sendSizes(number_threads), recvSizes(number_threads), sendOffsets(number_threads), recvOffsets(number_threads), sendSizeTotal{0UL}, recvSizeTotal{0UL}, dynamic{true} {
            sendBuffers.resize(number_threads);
            recvBuffers.resize(number_threads);
        }

        /**
		 * Constructor for the non-dynamic setting, which means that the user
		 * pre-allocates a single master buffer, while manipulates slave buffers.
		 */
        AllToAllContext(vector<uint64_t> &sendSizes) : sendSizes{sendSizes}, recvSizes(number_threads), sendOffsets(number_threads), recvOffsets(number_threads), sendSizeTotal{0UL}, recvSizeTotal{0UL}, dynamic{false} {
            // Initialize sizes and prepare per-core buffers
            prepare();
        }

        /**
		 * Performs the all-to-all communication.
		 */
        inline void perform() {
            if(dynamic) {
                // Initialize sizes and prepare per-core buffers
                for(int core = 0; core < number_threads; core++) {
                    sendSizes[core] = sendBuffers[core].used;
                }

                sendBuffer.append(sendBuffers);

                prepare();
            }
            Synchronizer *addr;
            allToAllV(sendBuffer, &sendSizes[0], &sendOffsets[0], recvBuffer, &recvSizes[0], &recvOffsets[0], addr);
        }

        /**
		 * Reset buffers, freeing their memory.
		 */
        inline void reset() {
            sendBuffer.reset();
            recvBuffer.reset();
        }

    private:
        /**
		 * Exchanges transfer sizes before the all-to-all communication.
		 */
        void prepare() {
            // Shuffle receive size information
            //TODO: Figure out, uncomment
            //MPIHelper::allToAll(&sendSizes[0], &recvSizes[0], MPI_UINT64_T);

            // Calculate offsets

            sendOffsets[0] = 0;
            partial_sum(sendSizes.begin(), sendSizes.end() - 1, sendOffsets.begin() + 1);

            recvOffsets[0] = 0;
            partial_sum(recvSizes.begin(), recvSizes.end() - 1, recvOffsets.begin() + 1);

            // Calculate total sizes

            sendSizeTotal = accumulate(sendSizes.begin(), sendSizes.end(), 0UL);
            recvSizeTotal = accumulate(recvSizes.begin(), recvSizes.end(), 0UL);

            // Reserve space and get out-of-order nodes

            sendBuffers = sendBuffer.setupAuxiliaryBuffers(sendSizes, sendOffsets);
            recvBuffers = recvBuffer.setupAuxiliaryBuffers(recvSizes, recvOffsets);
        }
    };
};

#endif /* MPI_THREAD_HELPER_HPP */
