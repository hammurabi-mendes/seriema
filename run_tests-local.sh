#!/bin/sh

EXEC="./mcts_main"

run_test() {
	for i in 1 2 3 4 5; do
		timeout 5m $EXEC $1
	done
}

run_mpi_test() {
	for i in 1 2 3 4 5; do
		mpirun -np timeout 5m $1 $EXEC $2
	done
}

run_test 1
run_test 2
run_test 4
run_test 8
run_mpi_test 2 4
run_test 16
run_mpi_test 2 8
run_test 32
run_mpi_test 2 16
