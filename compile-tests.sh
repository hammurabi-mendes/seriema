#!/bin/sh

FLAGS="-DWITHOUT_INLINE_CALLS -DNUMA_ALLOCATOR -DNO_DONATE_SIMULATIONS -DNO_FAIL_AGGREGATOR -DLIBNUMA -DAFFINITY -DUSE_MPI -DSTATS"

LIBS="-lnuma"

mpicxx -O3 -std=c++17 -m64 -fno-strict-aliasing -no-pie -I. -I../Concurrency $FLAGS -o mcts_main -I. -I/opt/include MCTS/main.cpp thread_handler.cpp remote_objects.cpp ibutils.cpp networking.c -libverbs -lpthread $LIBS