#pragma once

#include <atomic>
#include <cstdint>

// Aggregate latency stats, updated concurrently by every worker thread.
// Each field is its own atomic rather than one struct behind a mutex --
// there's no need for the three fields to update as a single atomic
// unit, so independent atomics avoid any locking entirely.
class LatencyStats {
public:
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
    }

    uint64_t count() const { return count_.load(std::memory_order_relaxed); }
    uint64_t total_ns() const { return total_ns_.load(std::memory_order_relaxed); }
    uint64_t max_ns() const { return max_ns_.load(std::memory_order_relaxed); }

    double average_us() const {
        uint64_t c = count();
        if (c == 0) return 0.0;
        return static_cast<double>(total_ns()) / static_cast<double>(c) / 1000.0;
    }

private:
    std::atomic<uint64_t> count_{0};
    std::atomic<uint64_t> total_ns_{0};
    std::atomic<uint64_t> max_ns_{0};
};
