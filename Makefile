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

PROGRAMS=client_server

all: $(PROGRAMS)

client_server: client_server.cpp ibutils.cpp networking.o
	$(CXX) $(CPPFLAGS) -o $@ $^ $(LDFLAGS)

%.o: %.c
	$(CC) -c $(CFLAGS) $< -o $@

clean:
	rm -f *.o $(PROGRAMS)
