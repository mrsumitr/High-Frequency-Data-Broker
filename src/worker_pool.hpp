#pragma once

#include <atomic>
#include <cstdio>
#include <thread>
#include <vector>

#include "backoff.hpp"
#include "clock_utils.hpp"
#include "dsp.hpp"
#include "latency_stats.hpp"
#include "memory_pool.hpp"
#include "ring_buffer.hpp"

// A fixed set of worker threads created once at startup and kept alive
// for the whole program. Each thread busy-spins on the ring buffer,
// claiming and processing pool slots as the listener publishes them.
template <std::size_t RingCapacity>
class WorkerPool {
public:
    WorkerPool(std::size_t num_threads, MemoryPool& pool, RingBuffer<RingCapacity>& ring,
               LatencyStats& stats, std::size_t ma_window, std::size_t log_every)
        : running_(true), pool_(pool), ring_(ring), stats_(stats), ma_window_(ma_window),
          log_every_(log_every) {
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
        // Thread-local, not shared -- each worker decides independently
        // whether to log its own Nth packet. No atomic needed, and it
        // avoids adding any shared-state contention to the hot path.
        uint64_t processed_count = 0;
        SpinBackoff backoff;
        while (running_.load(std::memory_order_acquire)) {
            std::size_t slot_index;
            if (ring_.try_claim(slot_index)) {
                backoff.reset();
                SensorReading& r = pool_.slot(slot_index);

                // Smoothed output lives on this thread's own stack -- each
                // worker has its own local buffer, so no data race even
                // though many workers process concurrently.
                float smoothed[MAX_VOLTAGE_SAMPLES];
                moving_average(r.voltages, smoothed, r.num_samples, ma_window_);

                // Latency window: from the moment this packet crossed the
                // socket (r.recv_ns, stamped in packet_reader.hpp) to the
                // moment the math above finished. Deliberately measured
                // before the logging below -- printf is I/O, not part of
                // the pipeline's actual processing latency, and is in
                // fact the single biggest source of latency under burst
                // load if left unthrottled (see --log-every).
                uint64_t latency_ns = now_ns() - r.recv_ns;
                stats_.record(latency_ns);

                ++processed_count;
                bool should_log =
                    log_every_ != 0 && (processed_count % log_every_ == 0);

                if (should_log) {
                    // Build the whole log line in a thread-local buffer and
                    // emit it with a single printf() call. printf() calls
                    // are individually atomic w.r.t. other threads, but a
                    // "line" built from several printf() calls is not --
                    // other workers' output can interleave mid-line.
                    // Multiple threads sharing stdout is itself shared
                    // mutable state.
                    char line[256];
                    int off = std::snprintf(line, sizeof(line),
                        "Worker %zu claimed slot %zu (timestamp=%llu, num_samples=%u, latency=%.1fus) smoothed=[",
                        worker_id, slot_index, static_cast<unsigned long long>(r.timestamp_ns),
                        r.num_samples, latency_ns / 1000.0);
                    for (uint32_t i = 0; i < r.num_samples && off < static_cast<int>(sizeof(line)); ++i) {
                        off += std::snprintf(line + off, sizeof(line) - off, "%.3f%s", smoothed[i],
                                              i + 1 < r.num_samples ? ", " : "");
                    }
                    std::snprintf(line + off, sizeof(line) - off, "]\n");
                    std::printf("%s", line);
                }

                pool_.state(slot_index).store(SlotState::Free, std::memory_order_release);
            } else {
                backoff.spin();
            }
        }
        std::printf("Worker %zu shutting down.\n", worker_id);
    }

    std::atomic<bool> running_;
    std::vector<std::thread> threads_;
    MemoryPool& pool_;
    RingBuffer<RingCapacity>& ring_;
    LatencyStats& stats_;
    std::size_t ma_window_;
    std::size_t log_every_;
};
