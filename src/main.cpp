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
    int client_fd = listener.accept_connectivity();

    if (receive_sensor_reading(client_fd, pool, 0)) {
        pool.state(0).store(SlotState::Ready, std::memory_order_release);
        ring.publish(0);
        std::printf("Published slot 0 to the ring buffer.\n");
    } else {
        std::printf("Failed to receive a valid sensor packet.\n");
    }

    close(client_fd);

    // Give a worker a moment to claim and process before shutdown.
    // (A real system loops forever ingesting instead of sleeping here --
    // that's the next step.)
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    return 0;
    // workers' destructor runs here, stops the spin loops, and joins all threads
}
