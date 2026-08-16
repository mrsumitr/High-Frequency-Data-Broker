# High-Frequency Data Broker

A small C++ systems project simulating a real-time sensor telemetry pipeline: raw TCP ingestion, a pre-allocated memory pool, a lock-free ring buffer handoff, concurrent DSP processing, and a measured microsecond-latency performance report.

Built to demonstrate: fixed-budget memory management (no heap allocation after startup), lock-free concurrency (atomics instead of mutexes), raw POSIX networking, and evidence-based performance claims instead of guesses.

## Architecture

```
Python (mock hardware)  --raw TCP-->  TcpListener  --parse-->  MemoryPool slot
                                                                      |
                                                              RingBuffer.publish()
                                                                      |
                                                    (8 worker threads, spin-then-backoff)
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
- **`src/worker_pool.hpp`** — a fixed set of threads (one per hardware core) spawned once at startup, polling the ring buffer via `SpinBackoff` for lowest latency when work is flowing.
- **`src/dsp.hpp`** — a single-pass O(n) moving average over each reading's voltages.
- **`src/latency_stats.hpp`** — lock-free aggregate stats (count / total / max / p50 / p95 / p99) updated concurrently by all worker threads via atomics, plus a fixed-capacity pre-allocated sample ring for percentile calculation (same "no allocation after startup" philosophy as the memory pool).
- **`src/clock_utils.hpp`** — monotonic timestamp helper (`std::chrono::steady_clock`, not wall-clock, since latency measurement needs a clock that can't jump).
- **`src/backoff.hpp`** — `SpinBackoff`: tiered wait strategy for every busy-wait loop in the project (idle workers, ring buffer backpressure, memory pool backpressure). Tight-spins first for lowest latency, then yields, then falls back to a short real sleep — see "Measured results" for the actual CPU-usage impact.
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
./broker --port 9000 --pool-size 10000 --workers 8 --window 3 --log-every 1
```

| Flag | Default | Meaning |
|---|---|---|
| `--port` | 9000 | TCP port to listen on |
| `--pool-size` | 10000 | number of pre-allocated memory pool slots |
| `--workers` | hardware core count | number of worker threads |
| `--window` | 3 | moving average window size |
| `--log-every` | 1 | log every Nth processed packet per worker; `0` disables per-packet logging entirely |

`--help` prints usage and exits. Via `make`, pass flags with `make run ARGS="--port 9500 --workers 2"`.

`--log-every 0` matters for real measurement, not just quieter output: `printf` is I/O, and under load it dominates the very latency numbers you're trying to measure (see below) — use it whenever you want to see the pipeline's actual cost.

## Measured results

From actual test runs on Apple Silicon (M-series, 8 logical cores). A 2000-packet burst sent over one connection, with and without per-packet logging:

| Scenario | Avg | p50 | p95 | p99 | Peak |
|---|---|---|---|---|---|
| `--log-every 1` (default, logs every packet) | 2225.14 µs | 2267.08 µs | 3780.12 µs | 3812.08 µs | 3825.21 µs |
| `--log-every 0` (silent hot path) | **1.78 µs** | **1.12 µs** | **3.08 µs** | 23.29 µs | 101.17 µs |

That's roughly a **1250x reduction** in average latency with logging off — confirms `printf` was the dominant cost under load, not the ring buffer or DSP work itself. Single isolated packet, no contention: **7.5 µs**. This is exactly the kind of number a system should *measure*, not assume — several of the numbers above were only discovered by deliberately load-testing at 2000 packets instead of trusting a single-packet smoke test (see "Notable bugs" below).

**Idle CPU usage**, before and after adding `SpinBackoff` (`src/backoff.hpp`) to the busy-wait loops: **~690% CPU** (8 threads pure-spinning, each pinning a core) down to **30.6% CPU** for the same idle scenario — roughly a 20x reduction, with zero change to under-load correctness (still 2000/2000 packets, 0 duplicates).

Static memory pool: **1.45 MB** (10,000 pre-allocated slots). Actual peak process RSS (via `getrusage`): **~3.0 MB**, the difference being thread stacks and normal process overhead.

## Notable bugs found and fixed during development

Two real concurrency bugs surfaced only under load-testing (2000 packets), not at single-packet scale — worth knowing about since they're the kind of bug that ships silently:

1. **Interleaved log lines** — building one log line out of multiple `printf()` calls let other threads' output land mid-line. Fixed by building the full line in a buffer and emitting it in one call.
2. **Silent ring buffer overflow** — `publish()` had no backpressure against `try_claim()`'s progress, so a listener that outpaced the workers would silently overwrite unclaimed entries (confirmed: 290 lost/duplicated claims out of 2000 before the fix). Fixed by having `publish()` wait until the ring has room.

## What's not implemented

- Single active client connection at a time (though the broker re-accepts after a disconnect).
- No automated test suite.
- Percentiles are computed from a fixed-capacity sample ring (sized to `--pool-size`); once total packets processed exceeds that, older samples are overwritten, so percentiles reflect a bounded recent window rather than true all-time history.
