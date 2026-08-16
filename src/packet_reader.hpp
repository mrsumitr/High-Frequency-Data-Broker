#pragma once

#include <cstdint>
#include <cstring>
#include <sys/socket.h>
#include <sys/types.h>

#include "memory_pool.hpp"

// Wire format is big-endian (network byte order), matching Python's
// struct.pack(">...", ...) on the sensor_sim.py side.
namespace wire {

inline uint64_t read_be64(const uint8_t* buf) {
    uint64_t v = 0;
    for (int i = 0; i < 8; ++i) v = (v << 8) | buf[i];
    return v;
}

inline uint32_t read_be32(const uint8_t* buf) {
    return (static_cast<uint32_t>(buf[0]) << 24) |
           (static_cast<uint32_t>(buf[1]) << 16) |
           (static_cast<uint32_t>(buf[2]) << 8) |
           static_cast<uint32_t>(buf[3]);
}

inline float read_be_float(const uint8_t* buf) {
    uint32_t bits = read_be32(buf);
    float f;
    std::memcpy(&f, &bits, sizeof(f));
    return f;
}

}  // namespace wire

// Raw TCP is a byte stream, not a message stream -- a single recv() is
// not guaranteed to return all `len` bytes in one call. Loop until the
// full amount has arrived (or the connection closes/errors).
inline bool read_exact(int fd, uint8_t* buf, std::size_t len) {
    std::size_t total = 0;
    while (total < len) {
        ssize_t n = recv(fd, buf + total, len - total, 0);
        if (n <= 0) return false;
        total += static_cast<std::size_t>(n);
    }
    return true;
}

// Reads one binary sensor packet from fd directly into the given pool
// slot. Wire layout: 8-byte timestamp_ns, 4-byte num_samples, then
// num_samples x 4-byte floats, all big-endian.
inline bool receive_sensor_reading(int fd, MemoryPool& pool, std::size_t slot_index) {
    uint8_t header[12];
    if (!read_exact(fd, header, sizeof(header))) return false;

    uint64_t timestamp_ns = wire::read_be64(header);
    uint32_t num_samples = wire::read_be32(header + 8);

    // Reject malformed/malicious packets that would overflow the
    // fixed-size voltages array.
    if (num_samples > MAX_VOLTAGE_SAMPLES) return false;

    SensorReading& reading = pool.slot(slot_index);

    // Bulk payload is received straight into the pool slot's own
    // memory -- no intermediate scratch buffer for the voltage data.
    uint8_t* voltage_bytes = reinterpret_cast<uint8_t*>(reading.voltages);
    if (!read_exact(fd, voltage_bytes, num_samples * sizeof(float))) return false;

    // Each float's 4 raw bytes are converted from big-endian in place;
    // read-then-overwrite of the same 4 bytes per index is safe since
    // no iteration touches another iteration's bytes.
    for (uint32_t i = 0; i < num_samples; ++i) {
        reading.voltages[i] = wire::read_be_float(voltage_bytes + i * sizeof(float));
    }

    reading.timestamp_ns = timestamp_ns;
    reading.num_samples = num_samples;
    return true;
}
