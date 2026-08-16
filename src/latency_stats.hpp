#pragma once

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <memory>
#include <vector>

// Aggregate latency stats, updated concurrently by every worker thread.
// Running count/total/max are lock-free atomics -- no locking needed
// since each field updates independently. Percentiles need the actual
// distribution, though, so a fixed-capacity ring of raw samples is
// pre-allocated once at construction (same "no allocation after
// startup" philosophy as the memory pool). Once full, older samples
// get overwritten, so percentiles reflect a bounded recent window
// rather than true all-time history -- an intentional tradeoff to
// avoid unbounded memory growth over a long-running process.
class LatencyStats {
public:
    explicit LatencyStats(std::size_t sample_capacity)
        : sample_capacity_(sample_capacity),
          samples_(std::make_unique<std::atomic<uint64_t>[]>(sample_capacity)) {}

    void record(uint64_t latency_ns) {
        count_.fetch_add(1, std::memory_order_relaxed);
        total_ns_.fetch_add(latency_ns, std::memory_order_relaxed);

        // max_ns_ needs a compare-exchange loop, not a plain store:
        // several workers can race to update the max concurrently, and
        // a plain store could clobber a larger value with a smaller one.
        uint64_t prev = max_ns_.load(std::memory_order_relaxed);
        while (latency_ns > prev &&
               !max_ns_.compare_exchange_weak(prev, latency_ns, std::memory_order_relaxed)) {
            // prev is updated with the current value by compare_exchange_weak on failure; retry
        }

        std::size_t idx = sample_write_.fetch_add(1, std::memory_order_relaxed) % sample_capacity_;
        samples_[idx].store(latency_ns, std::memory_order_relaxed);
    }

    uint64_t count() const { return count_.load(std::memory_order_relaxed); }
    uint64_t total_ns() const { return total_ns_.load(std::memory_order_relaxed); }
    uint64_t max_ns() const { return max_ns_.load(std::memory_order_relaxed); }

    double average_us() const {
        uint64_t c = count();
        if (c == 0) return 0.0;
        return static_cast<double>(total_ns()) / static_cast<double>(c) / 1000.0;
    }

    // p in [0.0, 1.0]. Only meant to be called after all worker threads
    // have joined (e.g. in the final shutdown report) -- it reads the
    // sample ring without synchronization against concurrent record()
    // calls, and does a one-time allocation to sort a local copy, which
    // is fine for a shutdown-time report but not for the hot path.
    double percentile_us(double p) const {
        std::size_t n = static_cast<std::size_t>(std::min<uint64_t>(count(), sample_capacity_));
        if (n == 0) return 0.0;

        std::vector<uint64_t> sorted(n);
        for (std::size_t i = 0; i < n; ++i) {
            sorted[i] = samples_[i].load(std::memory_order_relaxed);
        }
        std::sort(sorted.begin(), sorted.end());

        std::size_t idx = static_cast<std::size_t>(p * static_cast<double>(n - 1));
        return static_cast<double>(sorted[idx]) / 1000.0;
    }

private:
    std::size_t sample_capacity_;
    std::unique_ptr<std::atomic<uint64_t>[]> samples_;
    std::atomic<std::size_t> sample_write_{0};
    std::atomic<uint64_t> count_{0};
    std::atomic<uint64_t> total_ns_{0};
    std::atomic<uint64_t> max_ns_{0};
};
