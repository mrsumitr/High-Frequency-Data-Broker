#include <cstdio>
#include "memory_pool.hpp"
#include "worker_pool.hpp"
#include <thread>
constexpr std::size_t POOL_CAPACITY = 10000;
int main() {
    std::printf("High-Frequency Data Broker starting up...\n");
    // the only and only allocation for whole program's life time.
    MemoryPool pool(POOL_CAPACITY);
    const double pool_mb=(pool.capacity()*sizeof(SensorReading))/(1024.0*1024.0);
    std::printf("Memory pool ready: %zu slots (%.2f MB)\n", pool.capacity(), pool_mb);

    unsigned int hw_threads = std::thread::hardware_concurrency();
    if(hw_threads==0) hw_threads = 4;
    std::printf("Spawning %u worker threads.\n", hw_threads);
    {
        WorkerPool workers(hw_threads);
        std::this_thread::sleep_for(std::chrono::seconds(1));

    }// destructor joins all threads here
    std::printf("All workers joined cleanly. \n");

    // sanity check: claim slot 0, write into it, mark it ready then free it.
    SensorReading& r=pool.slot(0);
    r.timestamp_ns=123456789;
    r.num_samples=4;
    r.voltages[0]=1.1f;
    r.voltages[1]=2.2f;
    r.voltages[2]=3.3f;
    r.voltages[3]=4.4f;
    pool.state(0).store(SlotState::Ready, std::memory_order_release);

    std::printf("Slot 0 timestamp=%llu num_samples=%u first_voltage=%.2f\n", static_cast<unsigned long long>(r.timestamp_ns), r.num_samples, r.voltages[0]);

    pool.state(0).store(SlotState::Free, std::memory_order_release);
    std::printf("Slot 0 recycled.\n");

    return 0;
}
