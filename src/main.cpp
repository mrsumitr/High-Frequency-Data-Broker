#include <cstdint>
#include <cstdio>
#include <unistd.h>
#include "memory_pool.hpp"
#include "worker_pool.hpp"
#include "tcp_listener.hpp"
#include "packet_reader.hpp"
#include "ring_buffer.hpp"
#include <thread>

constexpr std::size_t POOL_CAPACITY = 10000;
constexpr std::size_t RING_CAPACITY = 1024;

int main() {
    std::printf("High-Frequency Data Broker starting up...\n");

    // the only and only allocation for whole program's life time.
    MemoryPool pool(POOL_CAPACITY);
    const double pool_mb=(pool.capacity()*sizeof(SensorReading))/(1024.0*1024.0);
    std::printf("Memory pool ready: %zu slots (%.2f MB)\n", pool.capacity(), pool_mb);

    RingBuffer<RING_CAPACITY> ring;

    unsigned int hw_threads = std::thread::hardware_concurrency();
    if(hw_threads==0) hw_threads = 4;
    std::printf("Spawning %u worker threads.\n", hw_threads);
    WorkerPool<RING_CAPACITY> workers(hw_threads, pool, ring);

    TcpListener listener(9000);
    uint64_t next_slot = 0;

    while (true) {  // outer loop: accept a new connection if one drops
        int client_fd = listener.accept_connectivity();

        while (true) {  // inner loop: read packets continuously from this connection
            std::size_t slot_index = next_slot % POOL_CAPACITY;

            // Backpressure: don't overwrite a slot a worker hasn't
            // finished with yet.
            while (pool.state(slot_index).load(std::memory_order_acquire) != SlotState::Free) {
                // busy-spin
            }

            if (!receive_sensor_reading(client_fd, pool, slot_index)) {
                break;  // connection closed or malformed packet
            }

            pool.state(slot_index).store(SlotState::Ready, std::memory_order_release);
            ring.publish(slot_index);
            ++next_slot;
        }

        close(client_fd);
        std::printf("Connection closed. Waiting for a new one...\n");
    }

    return 0;
    // workers' destructor runs here, stops the spin loops, and joins all threads
}
