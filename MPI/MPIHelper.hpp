#ifndef MPI_HELPER_HPP
#define MPI_HELPER_HPP

#include <numeric>

#include <algorithm>
#include <functional>
#include <limits.h>

#include <mpi.h>

#include "seriema.h"

#include "utils/SerializationBuffer.hpp"

using std::accumulate;

/**
 * Class that provides facilities to use MPI collective operations.
 */
struct MPIHelper {
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
        vector<int> sources(seriema::number_threads);
        vector<int> destinations(seriema::number_threads);

        for(int i = 0; i < seriema::number_threads; i++) {
            sources[i] = i;
            destinations[i] = i;
        }

        int returnValue = MPI_Dist_graph_create_adjacent(oldCommunicator, seriema::number_threads, &sources[0], MPI_UNWEIGHTED, seriema::number_threads, &destinations[0], MPI_UNWEIGHTED, MPI_INFO_NULL, 0, newCommunicator);

        return returnValue;
    }

    /**
	 * Performs an MPI Scatter operation.
	 *
	 * @param sendBuffer Location of the sent data (\p numberRecords for each process).
	 * @param recvBuffer Location of the received data (\p numberRecords).
	 * @param root The process for which \p sendBuffer is valid.
	 * @param datatype MPI datatype of each element in the send and receive buffers.
     * @param request MPI request that defaults to the thread_local global_request from dsys
	 * @param numberRecords Number of records to be sent to each process by process \p root,
	 *                      and number of records to be received by each process.
	 */
    template<typename T>
    static inline void doScatter(T *sendBuffer, T *recvBuffer, int root, MPI_Datatype datatype, int numberRecords = 1, MPI_Request &request = seriema::global_request) {
        MPI_CHECK(MPI_Scatter(sendBuffer, numberRecords, datatype, recvBuffer, numberRecords, datatype, root, MPI_COMM_WORLD));
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
    static inline void scatter(T *sendBuffer, T *recvBuffer, int root, MPI_Datatype datatype, uint64_t numberRecords = 1) {
        if(numberRecords <= INT_MAX) {
            return doScatter(sendBuffer, recvBuffer, root, datatype, static_cast<int>(numberRecords));
        }

        MPI_Datatype newDatatype;

        getNewDatatype(numberRecords, datatype, &newDatatype);

        doScatter(sendBuffer, recvBuffer, root, newDatatype, 1);
    }

    /**
	 * Performs an MPI Gather operation.
	 *
	 * @param sendBuffer Location of the sent data (\p numberRecords).
	 * @param recvBuffer Location of the received data (\p numberRecords for each process).
	 * @param root The process for which \p recvBuffer is valid.
	 * @param datatype MPI datatype of each element in the send and receive buffers.
     * @param request MPI request that defaults to the thread_local global_request from dsys.
	 * @param numberRecords Number of records to be received from each process by process \p root,
	 *                      and number of records to be sent by each process.
	 */
    template<typename T>
    static inline void doGather(T *sendBuffer, T *recvBuffer, int root, MPI_Datatype datatype, int numberRecords = 1, MPI_Request &request = seriema::global_request) {
        MPI_CHECK(MPI_Gather(sendBuffer, numberRecords, datatype, recvBuffer, numberRecords, datatype, root, MPI_COMM_WORLD));
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
    static inline void gather(T *sendBuffer, T *recvBuffer, int root, MPI_Datatype datatype, uint64_t numberRecords = 1) {
        if(numberRecords <= INT_MAX) {
            return doGather(sendBuffer, recvBuffer, root, datatype, static_cast<int>(numberRecords));
        }

        MPI_Datatype newDatatype;

        getNewDatatype(numberRecords, datatype, &newDatatype);

        doGather(sendBuffer, recvBuffer, root, newDatatype, 1);
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
    static inline void doAllGather(T *sendBuffer, T *recvBuffer, MPI_Datatype datatype, int numberRecords = 1, MPI_Request &request = seriema::global_request) {
        MPI_CHECK(MPI_Allgather(sendBuffer, numberRecords, datatype, recvBuffer, numberRecords, datatype, MPI_COMM_WORLD));
    }

    /**
	 * Performs an MPI broadcast operation.
	 *
	 * @param buffer Location of the data (\p numberRecords).
	 * @param datatype MPI datatype of each element in the send and receive buffers.
     * @param request MPI request that defaults to the thread_local global_request from dsys
	 * @param numberRecords Number of records to be received from each process,
	 *                      and number of records to be sent by each process.
	 */
    template<typename T>
    static inline void doBroadcast(T *buffer, MPI_Datatype datatype, int root, int numberRecords = 1) {
        MPI_CHECK(MPI_Bcast(buffer, numberRecords, datatype, root, MPI_COMM_WORLD));
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
    static inline void broadcast(T *buffer, MPI_Datatype datatype, int root, uint64_t numberRecords = 1) {
        if(numberRecords <= INT_MAX) {
            return doBroadcast(buffer, datatype, root, static_cast<int>(numberRecords));
        }

        MPI_Datatype newDatatype;

        getNewDatatype(numberRecords, datatype, &newDatatype);

        doBroadcast(buffer, newDatatype, root, 1);
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
    static inline void allGather(T *sendBuffer, T *recvBuffer, MPI_Datatype datatype, uint64_t numberRecords = 1) {
        if(numberRecords <= INT_MAX) {
            return doAllGather(sendBuffer, recvBuffer, datatype, static_cast<int>(numberRecords));
        }

        MPI_Datatype newDatatype;

        getNewDatatype(numberRecords, datatype, &newDatatype);

        doAllGather(sendBuffer, recvBuffer, newDatatype, 1);
    }

    /**
	 * Performs an MPI All-to-All operation.
	 *
	 * @param sendBuffer Location of the sent data (\p numberRecords for each process).
	 * @param recvBuffer Location of the received data (\p numberRecords for each process).
	 * @param datatype MPI datatype of each element in the send and receive buffers.
     * @param request MPI request that defaults to the thread_local global_request from dsys
	 * @param numberRecords Number of records to be received from each process,
	 *                      and number of records to be sent by each process.
	 */
    template<typename T>
    static inline void doAllToAll(T *sendBuffer, T *recvBuffer, MPI_Datatype datatype, int numberRecords = 1) {
        MPI_CHECK(MPI_Alltoall(sendBuffer, numberRecords, datatype, recvBuffer, numberRecords, datatype, MPI_COMM_WORLD));
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
    static inline void allToAll(T *sendBuffer, T *recvBuffer, MPI_Datatype datatype, uint64_t numberRecords = 1) {
        if(numberRecords <= INT_MAX) {
            return doAllToAll(sendBuffer, recvBuffer, datatype, static_cast<int>(numberRecords));
        }

        MPI_Datatype newDatatype;

        getNewDatatype(numberRecords, datatype, &newDatatype);

        doAllToAll(sendBuffer, recvBuffer, datatype, 1);
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
     * @param request MPI request that defaults to the thread_local global_request from dsys
	 */
    template<typename T>
    static inline void doAllToAllV(T *sendBuffer, int *sendSizes, int *sendOffsets, T *recvBuffer, int *recvSizes, int *recvOffsets, MPI_Datatype datatype) {
        MPI_CHECK(MPI_Alltoallv(sendBuffer, sendSizes, sendOffsets, datatype, recvBuffer, recvSizes, recvOffsets, datatype, MPI_COMM_WORLD));
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
    static inline void allToAllV(SerializationBuffer &sendBuffer, uint64_t *sendSizes, uint64_t *sendOffsets, SerializationBuffer &recvBuffer, uint64_t *recvSizes, uint64_t *recvOffsets) {
        if(sendBuffer.size <= INT_MAX && recvBuffer.size <= INT_MAX) {
            return smallAllToAllV(sendBuffer.data, sendSizes, sendOffsets, recvBuffer.data, recvSizes, recvOffsets, MPI_PACKED);
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
    static inline void smallAllToAllV(T *sendBuffer, uint64_t *sendSizes, uint64_t *sendOffsets, T *recvBuffer, uint64_t *recvSizes, uint64_t *recvOffsets, MPI_Datatype datatype) {
        vector<int> intSendSizes(seriema::number_threads);
        vector<int> intRecvSizes(seriema::number_threads);

        vector<int> intSendOffsets(seriema::number_threads);
        vector<int> intRecvOffsets(seriema::number_threads);

        for(int i = 0; i < seriema::number_threads; i++) {
            intSendSizes[i] = static_cast<int>(sendSizes[i]);
            intRecvSizes[i] = static_cast<int>(recvSizes[i]);

            intSendOffsets[i] = static_cast<int>(sendOffsets[i]);
            intRecvOffsets[i] = static_cast<int>(recvOffsets[i]);
        }

        doAllToAllV(sendBuffer, &intSendSizes[0], &intSendOffsets[0], recvBuffer, &intRecvSizes[0], &intRecvOffsets[0], datatype);
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
        vector<int> newSendSizes(seriema::number_threads);
        vector<MPI_Datatype> newSendTypes(seriema::number_threads);
        vector<MPI_Aint> newSendOffsets(seriema::number_threads);

        vector<int> newRecvSizes(seriema::number_threads);
        vector<MPI_Datatype> newRecvTypes(seriema::number_threads);
        vector<MPI_Aint> newRecvOffsets(seriema::number_threads);

        MPI_Aint typeSize;
        getDatatypeExtent(datatype, &typeSize);

        for(int i = 0; i < seriema::number_threads; i++) {
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

        for(int i = 0; i < seriema::number_threads; i++) {
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
    template<typename T>
    static inline void doAllToAllW(T *sendBuffer, int *sendSizes, int *sendOffsets, MPI_Datatype *sendTypes, T *recvBuffer, int *recvSizes, int *recvOffsets, MPI_Datatype *recvTypes, MPI_Request &request = seriema::global_request) {
        MPI_CHECK(MPI_Alltoallw(sendBuffer, sendSizes, sendOffsets, sendTypes, recvBuffer, recvSizes, recvOffsets, recvTypes, MPI_COMM_WORLD));
    }

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
    static inline void allToAllW(SerializationBuffer &sendBuffer, uint64_t *sendSizes, uint64_t *sendOffsets, MPI_Datatype *sendTypes, SerializationBuffer &recvBuffer, uint64_t *recvSizes, uint64_t *recvOffsets, MPI_Datatype *recvTypes) {
        if(sendBuffer.size <= INT_MAX && recvBuffer.size <= INT_MAX) {
            return smallAllToAllW<char>(sendBuffer.data, sendSizes, sendOffsets, sendTypes, recvBuffer.data, recvSizes, recvOffsets, recvTypes);
        }

        largeAllToAllW<char>(sendBuffer.data, sendSizes, sendOffsets, sendTypes, recvBuffer.data, recvSizes, recvOffsets, recvTypes);
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
	 */
    template<typename T>
    static inline void smallAllToAllW(T *sendBuffer, uint64_t *sendSizes, uint64_t *sendOffsets, MPI_Datatype *sendTypes, T *recvBuffer, uint64_t *recvSizes, uint64_t *recvOffsets, MPI_Datatype *recvTypes) {
        vector<int> intSendSizes(seriema::number_threads);
        vector<int> intRecvSizes(seriema::number_threads);

        vector<int> intSendOffsets(seriema::number_threads);
        vector<int> intRecvOffsets(seriema::number_threads);

        for(int i = 0; i < seriema::number_threads; i++) {
            intSendSizes[i] = static_cast<int>(sendSizes[i]);
            intRecvSizes[i] = static_cast<int>(recvSizes[i]);

            intSendOffsets[i] = static_cast<int>(sendOffsets[i]);
            intRecvOffsets[i] = static_cast<int>(recvOffsets[i]);
        }

        doAllToAllW<T>(sendBuffer, &intSendSizes[0], &intSendOffsets[0], sendTypes, recvBuffer, &intRecvSizes[0], &intRecvOffsets[0], recvTypes);
    }

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
    template<typename T>
    static inline void largeAllToAllW(T *sendBuffer, uint64_t *sendSizes, uint64_t *sendOffsets, MPI_Datatype *sendTypes, T *recvBuffer, uint64_t *recvSizes, uint64_t *recvOffsets, MPI_Datatype *recvTypes, MPI_Request &request = seriema::global_request) {
        vector<int> newSendSizes(seriema::number_threads);
        vector<MPI_Datatype> newSendTypes(seriema::number_threads);
        vector<MPI_Aint> newSendOffsets(seriema::number_threads);

        vector<int> newRecvSizes(seriema::number_threads);
        vector<MPI_Datatype> newRecvTypes(seriema::number_threads);
        vector<MPI_Aint> newRecvOffsets(seriema::number_threads);

        for(int i = 0; i < seriema::number_threads; i++) {
            newSendSizes[i] = 1;
            newRecvSizes[i] = 1;

            getNewDatatype(sendSizes[i], sendTypes[i], &newSendTypes[i]);
            getNewDatatype(recvSizes[i], recvTypes[i], &newRecvTypes[i]);

            newSendOffsets[i] = sendOffsets[i];
            newRecvOffsets[i] = recvOffsets[i];
        }

        MPI_Comm completeGraph;

        createCompleteGraph(MPI_COMM_WORLD, &completeGraph);

        MPI_CHECK(MPI_Ineighbor_alltoallw(sendBuffer, &newSendSizes[0], &newSendOffsets[0], &newSendTypes[0], recvBuffer, &newRecvSizes[0], &newRecvOffsets[0], &newRecvTypes[0], completeGraph, &request));

        MPI_Comm_free(&completeGraph);

        for(int i = 0; i < seriema::number_threads; i++) {
            MPI_Type_free(&newSendTypes[i]);
            MPI_Type_free(&newRecvTypes[i]);
        }
    }

    /**
     * Executes barrier
     */
    static inline void barrier() {
        MPI_Barrier(MPI_COMM_WORLD);
    }

    /**
	 * Packs \p numberElements elements of type \t T serialized as an MPI Datatype \t MPI_T into \p buffer.
	 */
    template<typename T>
    static inline void pack(T *elements, SerializationBuffer &buffer, MPI_Datatype datatype, int numberElements = 1) {
        int size = (buffer.size > INT_MAX) ? INT_MAX : static_cast<int>(buffer.size);
        int used = 0;

        MPI_Pack(elements, numberElements, datatype, buffer.data + buffer.used, size, &used, MPI_COMM_WORLD);
        buffer.used += used;
    }

    /**
	 * Packs \p numberElements elements of type \t T serialized as an MPI Datatype \t MPI_T into \p buffer.
	 *
	 * The size of the serialization buffer is checked before packing.
	 */
    template<typename T>
    static inline void packChecked(T *elements, SerializationBuffer &buffer, MPI_Datatype datatype, int numberElements = 1) {
        int size = (buffer.size > INT_MAX) ? INT_MAX : static_cast<int>(buffer.size);
        int used = 0;

        // Allocate data in the serialization buffer if necessary
        buffer.ensure(size);

        MPI_Pack(elements, numberElements, datatype, buffer.data + buffer.used, size, &used, MPI_COMM_WORLD);
        buffer.used += used;
    }

    /**
	 * Unpacks \p numberElements elements of type \t T serialized as an MPI Datatype \t MPI_T from \p buffer.
	 */
    template<typename T>
    static inline void unpack(SerializationBuffer &buffer, T *elements, MPI_Datatype datatype, int numberElements = 1) {
        int size = (buffer.size > INT_MAX) ? INT_MAX : static_cast<int>(buffer.size);
        int used = 0;

        MPI_Unpack(buffer.data + buffer.used, size, &used, elements, numberElements, datatype, MPI_COMM_WORLD);
        buffer.used += used;
    }

    /**
	 * Reduces #cores elements using the reduce function \t G.
	 *
	 * @return The value globally reduced by \t G.
	 */
    template<typename T, typename G>
    static T reduce(T *elements, int count, T &initialValue, G reduceGlobal) {
        vector<T> locals(seriema::number_threads);

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

        vector<T> locals(seriema::number_threads);

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
        AllToAllContext() : sendSizes(seriema::number_threads), recvSizes(seriema::number_threads), sendOffsets(seriema::number_threads), recvOffsets(seriema::number_threads), sendSizeTotal{0UL}, recvSizeTotal{0UL}, dynamic{true} {
            sendBuffers.resize(seriema::number_threads);
            recvBuffers.resize(seriema::number_threads);
        }

        /**
		 * Constructor for the non-dynamic setting, which means that the user
		 * pre-allocates a single master buffer, while manipulates slave buffers.
		 */
        AllToAllContext(vector<uint64_t> &sendSizes) : sendSizes{sendSizes}, recvSizes(seriema::number_threads), sendOffsets(seriema::number_threads), recvOffsets(seriema::number_threads), sendSizeTotal{0UL}, recvSizeTotal{0UL}, dynamic{false} {
            // Initialize sizes and prepare per-core buffers
            prepare();
        }

        /**
		 * Performs the all-to-all communication.
		 */
        inline void perform() {
            if(dynamic) {
                // Initialize sizes and prepare per-core buffers
                for(int core = 0; core < seriema::number_threads; core++) {
                    sendSizes[core] = sendBuffers[core].used;
                }

                sendBuffer.append(sendBuffers);

                prepare();
            }

            allToAllV(sendBuffer, &sendSizes[0], &sendOffsets[0], recvBuffer, &recvSizes[0], &recvOffsets[0]);
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
            MPIHelper::allToAll(&sendSizes[0], &recvSizes[0], MPI_UINT64_T);

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

#endif /* MPI_HELPER_HPP */
