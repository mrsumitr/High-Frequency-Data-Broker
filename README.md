# High-Frequency Data Broker

A small C++ systems project simulating a real-time sensor telemetry pipeline: raw TCP ingestion, a pre-allocated memory pool, a lock-free ring buffer handoff, concurrent DSP processing, and a measured microsecond-latency performance report.

Built to demonstrate: fixed-budget memory management (no heap allocation after startup), lock-free concurrency (atomics instead of mutexes), raw POSIX networking, and evidence-based performance claims instead of guesses.

## Architecture

```
Python (mock hardware)  --raw TCP-->  TcpListener  --parse-->  MemoryPool slot
                                                                      |
                                                              RingBuffer.publish()
                                                                      |
                                                        (8 worker threads busy-spin)
                                                              RingBuffer.try_claim()
                                                                      |
                                                          moving_average() + latency stamp
                                                                      |
                                                              slot recycled to Free
```

- **`src/memory_pool.hpp`** — `MemoryPool` allocates every `SensorReading` slot once at startup (`std::make_unique<SensorReading[]>`). No `new`/`malloc` happens anywhere else in the program's lifetime; slots are reused via an atomic `Free`/`Ready` state per slot.
- **`src/tcp_listener.hpp`** — raw POSIX socket (`socket`/`bind`/`listen`/`accept`), no HTTP or framing library.
- **`src/packet_reader.hpp`** — parses the big-endian binary wire format directly into a `MemoryPool` slot's own memory (the bulk voltage payload is `recv()`'d straight into the slot, no scratch-buffer copy).
- **`src/ring_buffer.hpp`** — fixed-capacity, single-producer/multi-consumer lock-free ring. The listener `publish()`es a slot index; worker threads race to `try_claim()` one via `compare_exchange_weak`. `publish()` blocks (busy-spin) if the ring is full, so a fast producer can never silently overwrite an unclaimed entry.
- **`src/worker_pool.hpp`** — a fixed set of threads (one per hardware core) spawned once at startup, busy-spinning on the ring buffer for lowest latency.
- **`src/dsp.hpp`** — a single-pass O(n) moving average over each reading's voltages.
- **`src/latency_stats.hpp`** — lock-free aggregate stats (count / total / max latency) updated concurrently by all worker threads via atomics.
- **`src/clock_utils.hpp`** — monotonic timestamp helper (`std::chrono::steady_clock`, not wall-clock, since latency measurement needs a clock that can't jump).
- **`tools/sensor_sim.py`** — mock hardware: packs and streams binary sensor packets over TCP.

Shutdown is graceful: `SIGINT`/`SIGTERM` set a flag checked by the accept/read loops (with `SA_RESTART` deliberately disabled so blocking socket calls return `EINTR` immediately); all worker threads are joined before the final report is computed, so every in-flight packet is counted.

## Building and running

Requires `clang++` (or any C++17 compiler) and Python 3. No external dependencies.

```
make run
```

In a second terminal, generate traffic:

```
python3 tools/sensor_sim.py    # streams 2000 binary packets over one connection
```

Stop the broker with `Ctrl+C` to see the final performance report.

### Configuration

Everything is a runtime flag, no rebuilding needed to change defaults:

```
./broker --port 9000 --pool-size 10000 --workers 8 --window 3
```

| Flag | Default | Meaning |
|---|---|---|
| `--port` | 9000 | TCP port to listen on |
| `--pool-size` | 10000 | number of pre-allocated memory pool slots |
| `--workers` | hardware core count | number of worker threads |
| `--window` | 3 | moving average window size |

`--help` prints usage and exits. Via `make`, pass flags with `make run ARGS="--port 9500 --workers 2"`.

## Measured results

From actual test runs on Apple Silicon (M-series, 8 logical cores):

| Scenario | Avg latency | Peak latency |
|---|---|---|
| Single isolated packet, no contention | **7.5 µs** | 7.5 µs |
| 2000-packet burst over one connection | 2169.91 µs | 3620.04 µs |

The gap between the two isn't a bug — it's contention made visible: under a 2000-packet burst arriving almost instantly, all 8 worker threads compete for the ring buffer and for `stdout` (each claim logs a line), so per-packet latency rises with load. This is exactly the kind of number a system should *measure*, not assume — see the "verify by load-testing" note below.

Static memory pool: **1.45 MB** (10,000 pre-allocated slots). Actual peak process RSS (via `getrusage`): **2.94 MB**, the difference being thread stacks and normal process overhead.

## Notable bugs found and fixed during development

Two real concurrency bugs surfaced only under load-testing (2000 packets), not at single-packet scale — worth knowing about since they're the kind of bug that ships silently:

1. **Interleaved log lines** — building one log line out of multiple `printf()` calls let other threads' output land mid-line. Fixed by building the full line in a buffer and emitting it in one call.
2. **Silent ring buffer overflow** — `publish()` had no backpressure against `try_claim()`'s progress, so a listener that outpaced the workers would silently overwrite unclaimed entries (confirmed: 290 lost/duplicated claims out of 2000 before the fix). Fixed by having `publish()` wait until the ring has room.

## What's not implemented

- Single active client connection at a time (though the broker re-accepts after a disconnect).
- No automated test suite.
- No tail-latency percentiles (p50/p95/p99) — only average and max are tracked.
- Busy-spin loops (idle workers, ring buffer backpressure) pin CPU cores at 100% with no backoff.
