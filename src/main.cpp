#include <cstdio>
#include <unistd.h>
#include "memory_pool.hpp"
#include "worker_pool.hpp"
#include "tcp_listener.hpp"
#include "packet_reader.hpp"
#include "ring_buffer.hpp"
#include <thread>
constexpr std::size_t POOL_CAPACITY = 10000;
int main() {
    std::printf("High-Frequency Data Broker starting up...\n");
    // the only and only allocation for whole program's life time.
    MemoryPool pool(POOL_CAPACITY);
    const double pool_mb=(pool.capacity()*sizeof(SensorReading))/(1024.0*1024.0);
    std::printf("Memory pool ready: %zu slots (%.2f MB)\n", pool.capacity(), pool_mb);

    // quick sanity test of the ring buffer alone, before wiring it to threads
    RingBuffer<1024> ring;
    ring.publish(42);
    ring.publish(7);

    std::size_t claimed;
    while (ring.try_claim(claimed)) {
        std::printf("Claimed pool slot index: %zu\n", claimed);
    }

    unsigned int hw_threads = std::thread::hardware_concurrency();
    if(hw_threads==0) hw_threads = 4;
    std::printf("Spawning %u worker threads.\n", hw_threads);
    {
        WorkerPool workers(hw_threads);
        std::this_thread::sleep_for(std::chrono::seconds(1));

    }// destructor joins all threads here
    std::printf("All workers joined cleanly. \n");

    TcpListener listener(9000);
    int client_fd = listener.accept_connectivity();

    if (receive_sensor_reading(client_fd, pool, 0)) {
        pool.state(0).store(SlotState::Ready, std::memory_order_release);

        SensorReading& r = pool.slot(0);
        std::printf("Parsed packet: timestamp=%llu num_samples=%u voltages=[",
                    static_cast<unsigned long long>(r.timestamp_ns), r.num_samples);
        for (uint32_t i = 0; i < r.num_samples; ++i) {
            std::printf("%.3f%s", r.voltages[i], i + 1 < r.num_samples ? ", " : "");
        }
        std::printf("]\n");

        pool.state(0).store(SlotState::Free, std::memory_order_release);
        std::printf("Slot 0 recycled.\n");
    } else {
        std::printf("Failed to receive a valid sensor packet.\n");
    }

    close(client_fd);
    return 0;
}
