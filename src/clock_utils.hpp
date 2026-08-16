#pragma once

#include <chrono>
#include <cstdint>

// Monotonic clock in nanoseconds. steady_clock (not system_clock) is used
// specifically because it can't jump backward/forward due to NTP or wall
// clock adjustments -- latency measurements need a clock that only moves
// forward at a steady rate.
inline uint64_t now_ns() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
}
