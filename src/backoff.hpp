#pragma once

#include <chrono>
#include <thread>

// Bounded, tiered backoff for busy-wait loops (idle workers, ring buffer
// backpressure). Tight-spins first -- cheapest, lowest-latency response
// once work actually shows up -- then yields the CPU to the scheduler,
// then falls back to a short real sleep if the wait drags on. A yield
// alone doesn't guarantee giving the core back (the OS can hand it
// straight back to the same thread if nothing else is runnable), so the
// sleep tier is what actually stops pinning a core at 100% during a
// long idle stretch.
class SpinBackoff {
public:
    void spin() {
        if (spins_ < kMaxPureSpins) {
            ++spins_;
            return;
        }
        if (spins_ < kMaxPureSpins + kMaxYields) {
            ++spins_;
            std::this_thread::yield();
            return;
        }
        std::this_thread::sleep_for(std::chrono::microseconds(50));
    }

    // Call once real work is found -- resets to the fastest (pure spin)
    // tier so the next wait starts back at lowest latency.
    void reset() { spins_ = 0; }

private:
    static constexpr int kMaxPureSpins = 1000;
    static constexpr int kMaxYields = 1000;
    int spins_ = 0;
};
