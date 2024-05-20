/*
Copyright (c) 2024, Hammurabi Mendes.
All rights reserved.

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

#ifndef NETWORKING_H
#define NETWORKING_H

#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <unistd.h>
#include <fcntl.h>

/**
 * Create an accept socket in the specified \p port.
 *
 * @param port Port where the server should run.
 * @return The server socket, or -1 if an error is found -- errno indicate the error.
 */
int create_server(int port);

/**
 * Accept a client in the specified \p accept_socket.
 *
 * @param accept_socket Accept socket used to listen to clients.
 * @return The client socket, or -1 if an error is found -- errno indicate the error.
 */
int accept_client(int accept_socket);

/**
 * Get information about the peer connected to the socket \p socket.
 *
 * @param socket Socket that is connected to the peer we are getting information from.
 * @param host_string Pointer to the character buffer that will hold the hostname.
 * @param host_length Length of the character buffer that will hold the hostname.
 * @param port Pointer to an integer that will contain the port upon return.
 */
void get_peer_information(int socket, char *host_string, int host_length, int *port);

/**
 * Turns socket non-blocking on/off.
 * When non-blocking is on, read and write operations will return error codes instead of blocking.
 * When non-blocking is off... well, it blocks! :)
 *
 * @param socket Socket that will have its non-blocking flag set on/off.
 * @param flag 1 to turn non-blockingness ON; 0 to turn it OFF.
 */
void make_nonblocking(int socket, int flag);

int connect_server(const char* destination_address, uint16_t port);

#endif /* NETWORKING_H */
