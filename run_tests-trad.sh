#!/bin/sh

EXEC="./mcts_main-trad"

run_test() {
	for i in 1 2 3 4 5; do
		timeout 5m $EXEC $1
	done
}

run_mpi_test() {
	for i in 1 2 3 4 5; do
		mpirun -np $1 timeout 5m $EXEC $2
	done
}

run_mpi_test_hostfile() {
	for i in 1 2 3 4 5; do
		mpirun -hostfile $3 -np $1 timeout 5m $EXEC $2
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

run_mpi_test_hostfile 2 16 "hf-a"
run_mpi_test_hostfile 3 16 "hf-a"
run_mpi_test_hostfile 4 16 "hf-b"

run_mpi_test_hostfile 2 32 "hf-a"
run_mpi_test_hostfile 3 32 "hf-a"
run_mpi_test_hostfile 4 32 "hf-b"

run_mpi_test_hostfile 4 8 "hf-d"
run_mpi_test_hostfile 6 8 "hf-e"
run_mpi_test_hostfile 8 8 "hf-f"

run_mpi_test_hostfile 4 16 "hf-d"
run_mpi_test_hostfile 6 16 "hf-e"
run_mpi_test_hostfile 8 16 "hf-f"
