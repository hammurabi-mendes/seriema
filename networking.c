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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "networking.h"

void reuse_address(int socket);

int create_server(int port) {
	int result;

	struct addrinfo socket_hints;
	struct addrinfo *socket_results;

	memset(&socket_hints, 0, sizeof(struct addrinfo));

	socket_hints.ai_family = AF_UNSPEC;
	socket_hints.ai_socktype = SOCK_STREAM;
	socket_hints.ai_flags = AI_PASSIVE;

	char port_string[16];

	snprintf(port_string, 16, "%d", port);

	result = getaddrinfo(NULL, port_string, &socket_hints, &socket_results);

	if(result != 0) {
		perror("getaddrinfo");

		return -1;
	}

	if(socket_results == NULL) {
		fprintf(stderr, "Cannot find address to bind.");

		return -1;
	}

	int accept_socket = socket(socket_results->ai_family, socket_results->ai_socktype, socket_results->ai_protocol);

	if(accept_socket == -1) {
		perror("socket");

		return -1;
	}

	// Allows a socket in TIME_WAIT to be reused for binding
	// Treats X:Y and Z:Y bindings as different even if Z is 0.0.0.0 (or ::)
	reuse_address(accept_socket);

	result = bind(accept_socket, socket_results->ai_addr, socket_results->ai_addrlen);

	if(result == -1) {
		perror("bind");

		return -1;
	}

	result = listen(accept_socket, 5);

	if(result == -1) {
		perror("listen");

		return -1;
	}

	// Makes the accept socket non-blocking
	// make_nonblocking(accept_socket);

	return accept_socket;
}

int accept_client(int accept_socket) {
	struct sockaddr_storage client_address;
	socklen_t client_length = sizeof(struct sockaddr_storage);

	int client_socket = accept(accept_socket, (struct sockaddr *) &client_address, &client_length);

	// Makes the client socket non-blocking
	// make_nonblocking(client_socket);

	return client_socket;
}

void get_peer_information(int socket, char *host_string, int host_length, int *port) {
	int result;

	struct sockaddr_storage address;
	socklen_t length;

	result = getpeername(socket, (struct sockaddr *) &address, &length);

	if(result == -1) {
		perror("getpeername");

		return;
	}

	char port_string[16];

	result = getnameinfo((struct sockaddr *) &address, length, host_string, host_length, port_string, 16, NI_NUMERICHOST | NI_NUMERICSERV);

	*port = atoi(port_string);

	if(result != 0) {
		perror("getnameinfo");

		return;
	}
}

// Allows a socket in TIME_WAIT to be reused for binding
// Treats X:Y and Z:Y bindings as differents even if Z is 0.0.0.0 (or ::)
void reuse_address(int socket) {
	int option = 1;

	setsockopt(socket, SOL_SOCKET, SO_REUSEADDR, &option, sizeof(int));
}

// Makes socket non-blocking for reading or writing
void make_nonblocking(int socket, int flag) {
	int options;

	options = fcntl(socket, F_GETFL);

	if(options < 0) {
		perror("fcntl(F_GETFL)");
		exit(EXIT_FAILURE);
	}

	fcntl(socket, F_SETFL, flag ? (options | O_NONBLOCK) : (options & (~O_NONBLOCK)));

	if(options < 0) {
		perror("fcntl(F_SETFL)");
		exit(EXIT_FAILURE);
	}
}

int connect_server(const char* destination_address, uint16_t port) {
	int result;

	// Using getaddrinfo to obtain the first address to connect to

	struct addrinfo result_hints;
	struct addrinfo *result_list;

	memset(&result_hints, 0, sizeof(struct addrinfo));

	result_hints.ai_family = AF_UNSPEC;
	result_hints.ai_socktype = SOCK_STREAM;

	char port_string[16];

	snprintf(port_string, 16, "%d", port);

	result = getaddrinfo(destination_address, port_string, &result_hints, &result_list);

	if(result != 0) {
		perror("Cannot obtain address");

		return -1;
	}

	if(result_list == NULL) {
		fprintf(stderr, "No address found");

		return -1;
	}

	// Socket creation

	int remote_socket;

	remote_socket = socket(result_list->ai_family, result_list->ai_socktype, result_list->ai_protocol);

	if(remote_socket == -1) {
		perror("It wasn't possible to create the socket");

		return -1;
	}

	// Connecting to the server

	result = connect(remote_socket, result_list->ai_addr, result_list->ai_addrlen);

	if(result == -1) {
		perror("Cannot connect to the server");

		return -1;
	}

	return remote_socket;
}

