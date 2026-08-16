#pragma once

#include <atomic>
#include <cstdio>
#include <thread>
#include <vector>

#include "dsp.hpp"
#include "memory_pool.hpp"
#include "ring_buffer.hpp"

constexpr std::size_t MOVING_AVERAGE_WINDOW = 3;

// A fixed set of worker threads created once at startup and kept alive
// for the whole program. Each thread busy-spins on the ring buffer,
// claiming and processing pool slots as the listener publishes them.
template <std::size_t RingCapacity>
class WorkerPool {
public:
    WorkerPool(std::size_t num_threads, MemoryPool& pool, RingBuffer<RingCapacity>& ring)
        : running_(true), pool_(pool), ring_(ring) {
        threads_.reserve(num_threads);
        for (std::size_t i = 0; i < num_threads; ++i) {
            threads_.emplace_back([this, i] { this->worker_loop(i); });
        }
    }

    ~WorkerPool() {
        running_.store(false, std::memory_order_release);
        for (auto& t : threads_) {
            if (t.joinable()) t.join();
        }
    }

    WorkerPool(const WorkerPool&) = delete;
    WorkerPool& operator=(const WorkerPool&) = delete;

private:
    void worker_loop(std::size_t worker_id) {
        std::printf("Worker %zu started, waiting for work...\n", worker_id);
        while (running_.load(std::memory_order_acquire)) {
            std::size_t slot_index;
            if (ring_.try_claim(slot_index)) {
                SensorReading& r = pool_.slot(slot_index);
                std::printf("Worker %zu claimed slot %zu (timestamp=%llu, num_samples=%u)\n",
                            worker_id, slot_index,
                            static_cast<unsigned long long>(r.timestamp_ns), r.num_samples);

                // Smoothed output lives on this thread's own stack -- each
                // worker has its own local buffer, so no data race even
                // though many workers process concurrently.
                float smoothed[MAX_VOLTAGE_SAMPLES];
                moving_average(r.voltages, smoothed, r.num_samples, MOVING_AVERAGE_WINDOW);

                std::printf("Worker %zu smoothed voltages: [", worker_id);
                for (uint32_t i = 0; i < r.num_samples; ++i) {
                    std::printf("%.3f%s", smoothed[i], i + 1 < r.num_samples ? ", " : "");
                }
                std::printf("]\n");

                pool_.state(slot_index).store(SlotState::Free, std::memory_order_release);
            }
            // Busy-spin: no sleep_for here on purpose -- lowest possible latency.
        }
        std::printf("Worker %zu shutting down.\n", worker_id);
    }

    std::atomic<bool> running_;
    std::vector<std::thread> threads_;
    MemoryPool& pool_;
    RingBuffer<RingCapacity>& ring_;
};
