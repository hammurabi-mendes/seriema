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

#ifndef AFFINITY_HANDLER_HPP
#define AFFINITY_HANDLER_HPP

#include <iostream>
#include <thread>

using std::thread;

#if defined(LIBNUMA) && defined(__linux__)
#include <numa.h>
#endif /* LIBNUMA and __linux__ */

#include <pthread.h>

using std::cout;
using std::cerr;

class AffinityHandler {
public:
    static constexpr int MAX_THREADS = 65536;

    unsigned int hardware_concurrency;

    int cpus[MAX_THREADS];
    int cpus_size = 0;

    int numa_zones[MAX_THREADS];
    int numa_zones_size = 0;

    int number_zones = 0;

    atomic<int> indexes[MAX_THREADS];

    AffinityHandler(): hardware_concurrency{thread::hardware_concurrency()} {
        initialize_affinity("2,16,16$0|16|1|17|2|18|3|19|4|20|5|21|6|22|7|23|8|24|9|25|10|26|11|27|12|28|13|29|14|30|15|31|");
#if defined(LIBNUMA) && defined(__linux__)
        numa_set_localalloc();
#endif /* LIBNUMA and __linux__ */
    }

    AffinityHandler(const char *machine_locality): hardware_concurrency{thread::hardware_concurrency()} {
        initialize_affinity(machine_locality);
#if defined(LIBNUMA) && defined(__linux__)
        numa_set_localalloc();
#endif /* LIBNUMA and __linux__ */
    }

    virtual ~AffinityHandler() {
    }

#if defined(LIBNUMA) && defined(__linux__)
    void set_numa_location(int thread_rank) {
        int index = indexes[thread_rank];

        numa_set_preferred(numa_zones[index % cpus_size]);
        // Not necessary because we use pthread_setaffinity_np
        // numa_run_on_node(numa_zones[index % cpus_size]);
    }
#endif /* LIBNUMA and __linux__ */

    inline int get_cpu(int thread_rank) const noexcept {
        int index = indexes[thread_rank];

        return cpus[index % cpus_size];
    }

    inline int get_numa_zone(int thread_rank) const noexcept {
        int index = indexes[thread_rank];

        return numa_zones[index % cpus_size];
    }

    inline int get_number_zones() const noexcept {
        return number_zones;
    }

    void get_thread_affinity(int thread_rank, int &cpu, int &numa_zone) {
        int index = indexes[thread_rank];

        cpu = cpus[index % cpus_size];
        numa_zone = numa_zones[index % cpus_size];
    }

    void set_thread_affinity(thread *target_thread, int thread_rank, int index) {
        indexes[thread_rank] = index;

        cpu_set_t cpuset;

        CPU_ZERO(&cpuset);

        int cpu_for_i = cpus[index % cpus_size];

        if(cpu_for_i != -1) {
            CPU_SET(cpu_for_i, &cpuset);

            int result = pthread_setaffinity_np(target_thread->native_handle(), sizeof(cpu_set_t), &cpuset);

            cerr << "Affinity: software thread " << thread_rank << " on cpu " << cpus[index % cpus_size] << " numa zone " << numa_zones[index % cpus_size] << std::endl;

            if(result != 0) {
                cerr << "Unable to set thread affinity" << std::endl;
                exit(EXIT_FAILURE);
            }
        }
    }

private:
    void initialize_affinity(const char *encoded_locality) {
        std::string encoded = encoded_locality;
        std::string delimiter = "|";

        size_t pos = 0;

        if((pos = encoded.find("$")) == std::string::npos) {
            cerr << "Error getting the CPU affinity information (1)" << std::endl;
            exit(EXIT_FAILURE);
        }

        std::string numa_zones_string = encoded.substr(0, pos);
        encoded.erase(0, pos + 1);

        if((pos = numa_zones_string.find(",")) == std::string::npos) {
            cerr << "Error getting the CPU affinity information (2)" << std::endl;
            exit(EXIT_FAILURE);
        }

        std::string number_zones_string = numa_zones_string.substr(0, pos);
        numa_zones_string.erase(0, pos + 1);

        number_zones = atoi(number_zones_string.c_str());

        int current_zone = 0;

        for(int i = 0; i < number_zones - 1; i++) {
            if((pos = numa_zones_string.find(",")) == std::string::npos) {
                cerr << "Error getting the CPU affinity information (3)" << std::endl;
                exit(EXIT_FAILURE);
            }

            std::string current_zone_size_string = numa_zones_string.substr(0, pos);
            numa_zones_string.erase(0, pos + 1);

            int current_zone_size = atoi(current_zone_size_string.c_str());

            for(int cpu = 0; cpu < current_zone_size; cpu++) {
                numa_zones[numa_zones_size++] = current_zone;
            }
            current_zone++;
        }

        std::string last_zone_size_string = numa_zones_string;

        int last_zone_size = atoi(last_zone_size_string.c_str());

        for(int cpu = 0; cpu < last_zone_size; cpu++) {
            numa_zones[numa_zones_size++] = current_zone;
        }
        current_zone++;

        while(encoded.size() > 0 && (pos = encoded.find(delimiter)) != std::string::npos) {
            std::string token = encoded.substr(0, pos);

            cpus[cpus_size++] = atoi(token.c_str());

            encoded.erase(0, pos + 1);
        }

        if(cpus_size != hardware_concurrency) {
            cerr << "Error getting the CPU affinity information (4)" << std::endl;
            exit(EXIT_FAILURE);
        }
    }
};

#endif /* AFFINITY_HANDLER_HPP */