#pragma once

#include <atomic>
#include <cstdint>
#include <memory>

// One binary packet from the mock hardware = one SensorReading.
// Fixed-size POD so a memcpy from the socket buffer can fill it directly.
constexpr std::size_t MAX_VOLTAGE_SAMPLES = 32;

struct SensorReading {
    uint64_t timestamp_ns;
    uint32_t num_samples;
    float voltages[MAX_VOLTAGE_SAMPLES];
};

// Per-slot lifecycle. Set by the listener thread when it writes data,
// cleared by a worker thread once it has finished processing that slot.
enum class SlotState : uint8_t { Free = 0, Ready = 1 };

// Fixed-capacity pool of SensorReadings, allocated exactly once at startup.
// After construction, no new/malloc ever happens on this pool again —
// slots are reused in place for the lifetime of the program.
class MemoryPool {
public:
    explicit MemoryPool(std::size_t capacity)
        : capacity_(capacity),
          readings_(std::make_unique<SensorReading[]>(capacity)),
          states_(std::make_unique<std::atomic<SlotState>[]>(capacity)) {
        for (std::size_t i = 0; i < capacity_; ++i) {
            states_[i].store(SlotState::Free, std::memory_order_relaxed);
        }
    }

    std::size_t capacity() const { return capacity_; }

    SensorReading& slot(std::size_t index) { return readings_[index]; }

    std::atomic<SlotState>& state(std::size_t index) { return states_[index]; }

private:
    std::size_t capacity_;
    std::unique_ptr<SensorReading[]> readings_;
    std::unique_ptr<std::atomic<SlotState>[]> states_;
};
