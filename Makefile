CXX=clang++
CC=clang

LDFLAGS=-lpthread -libverbs

ifeq ($(DEBUG), 1)
	CFLAGS=-I. -g
	CPPFLAGS=-std=c++17 $(CFLAGS)
else
	CFLAGS=-I. -m64 -O3 -fno-strict-aliasing
	CPPFLAGS=-std=c++17 $(CFLAGS)
endif

PROGRAMS=timer_send timer_rdma timer_transmitter_send timer_transmitter_rdma

all: $(PROGRAMS)

timer_rdma: timer_rdma.cpp ibutils.cpp networking.o
	$(CXX) $(CPPFLAGS) -o $@ $^ $(LDFLAGS)

timer_transmitter_rdma: timer_transmitter_rdma.cpp ibutils.cpp networking.o
	$(CXX) $(CPPFLAGS) -o $@ $^ $(LDFLAGS)

timer_send: timer_send.cpp ibutils.cpp networking.o
	$(CXX) $(CPPFLAGS) -o $@ $^ $(LDFLAGS)

timer_transmitter_send: timer_transmitter_send.cpp ibutils.cpp networking.o
	$(CXX) $(CPPFLAGS) -o $@ $^ $(LDFLAGS)

%.o: %.c
	$(CC) -c $(CFLAGS) $< -o $@

clean:
	rm -f *.o $(PROGRAMS)
