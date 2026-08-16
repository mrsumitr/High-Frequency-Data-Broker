#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <string>
#include <unistd.h>
#include <csignal>
#include <sys/resource.h>
#include "memory_pool.hpp"
#include "worker_pool.hpp"
#include "tcp_listener.hpp"
#include "packet_reader.hpp"
#include "ring_buffer.hpp"
#include "latency_stats.hpp"
#include "clock_utils.hpp"
#include <thread>

// Ring capacity isn't exposed as a CLI flag: it's a template parameter
// (needed at compile time for the power-of-two bitmask trick), unlike
// the other knobs below which are plain runtime values.
constexpr std::size_t RING_CAPACITY = 1024;

struct Config {
    uint16_t port = 9000;
    std::size_t pool_capacity = 10000;
    unsigned int worker_count = 0;  // 0 => auto-detect from hardware_concurrency()
    std::size_t ma_window = 3;
};

void print_usage(const char* prog_name) {
    std::printf(
        "Usage: %s [options]\n"
        "  --port N       TCP port to listen on (default 9000)\n"
        "  --pool-size N  number of pre-allocated memory pool slots (default 10000)\n"
        "  --workers N    number of worker threads (default: hardware core count)\n"
        "  --window N     moving average window size (default 3)\n"
        "  --help         show this message\n",
        prog_name);
}

Config parse_args(int argc, char** argv) {
    Config cfg;
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];

        auto next_value = [&]() -> std::string {
            if (i + 1 >= argc) {
                std::fprintf(stderr, "Missing value for %s\n", arg.c_str());
                std::exit(1);
            }
            return argv[++i];
        };

        if (arg == "--port") {
            cfg.port = static_cast<uint16_t>(std::stoi(next_value()));
        } else if (arg == "--pool-size") {
            cfg.pool_capacity = static_cast<std::size_t>(std::stoul(next_value()));
        } else if (arg == "--workers") {
            cfg.worker_count = static_cast<unsigned int>(std::stoul(next_value()));
        } else if (arg == "--window") {
            cfg.ma_window = static_cast<std::size_t>(std::stoul(next_value()));
        } else if (arg == "--help" || arg == "-h") {
            print_usage(argv[0]);
            std::exit(0);
        } else {
            std::fprintf(stderr, "Unknown argument: %s (use --help)\n", arg.c_str());
            std::exit(1);
        }
    }
    return cfg;
}

// Set by the signal handler only, read everywhere else. sig_atomic_t is
// the one type the standard guarantees is safe to touch from a signal
// handler without risking a torn read/write.
volatile std::sig_atomic_t g_shutdown_requested = 0;

void handle_shutdown_signal(int) {
    g_shutdown_requested = 1;
}

int main(int argc, char** argv) {
    Config cfg = parse_args(argc, argv);

    std::printf("High-Frequency Data Broker starting up...\n");

    // the only and only allocation for whole program's life time.
    MemoryPool pool(cfg.pool_capacity);
    const double pool_mb=(pool.capacity()*sizeof(SensorReading))/(1024.0*1024.0);
    std::printf("Memory pool ready: %zu slots (%.2f MB)\n", pool.capacity(), pool_mb);

    RingBuffer<RING_CAPACITY> ring;
    LatencyStats stats;

    // Deliberately no SA_RESTART: a blocking accept()/recv() interrupted
    // by SIGINT/SIGTERM must return EINTR immediately so the loops below
    // can check g_shutdown_requested and unwind promptly, instead of the
    // OS silently restarting the same blocking call underneath us.
    struct sigaction sa{};
    sa.sa_handler = handle_shutdown_signal;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;
    sigaction(SIGINT, &sa, nullptr);
    sigaction(SIGTERM, &sa, nullptr);

    uint64_t program_start_ns = now_ns();

    unsigned int hw_threads = cfg.worker_count;
    if (hw_threads == 0) hw_threads = std::thread::hardware_concurrency();
    if (hw_threads == 0) hw_threads = 4;
    std::printf("Spawning %u worker threads (moving average window: %zu).\n", hw_threads,
                cfg.ma_window);
    {
        WorkerPool<RING_CAPACITY> workers(hw_threads, pool, ring, stats, cfg.ma_window);

        TcpListener listener(cfg.port);
        uint64_t next_slot = 0;

        while (!g_shutdown_requested) {  // outer loop: accept a new connection if one drops
            int client_fd = listener.accept_connectivity();
            if (client_fd < 0) {
                continue;  // EINTR -- loop condition will exit us if it was a shutdown signal
            }

            while (!g_shutdown_requested) {  // inner loop: read packets continuously from this connection
                std::size_t slot_index = next_slot % cfg.pool_capacity;

                // Backpressure: don't overwrite a slot a worker hasn't
                // finished with yet.
                while (pool.state(slot_index).load(std::memory_order_acquire) != SlotState::Free) {
                    if (g_shutdown_requested) break;
                }
                if (g_shutdown_requested) break;

                if (!receive_sensor_reading(client_fd, pool, slot_index)) {
                    break;  // connection closed or malformed packet
                }

                pool.state(slot_index).store(SlotState::Ready, std::memory_order_release);
                ring.publish(slot_index);
                ++next_slot;
            }

            close(client_fd);
            if (!g_shutdown_requested) {
                std::printf("Connection closed. Waiting for a new one...\n");
            }
        }

        std::printf("\nShutdown requested -- draining in-flight work...\n");
    }  // workers' destructor runs here: stops the spin loops and joins every
       // thread, so every packet already published to the ring gets fully
       // processed (and counted in `stats`) before we report below.

    uint64_t elapsed_ns = now_ns() - program_start_ns;
    double elapsed_s = static_cast<double>(elapsed_ns) / 1e9;

    struct rusage usage{};
    getrusage(RUSAGE_SELF, &usage);
    // macOS/BSD report ru_maxrss in bytes; Linux reports it in KB -- this
    // divisor is correct for macOS only.
    double peak_rss_mb = static_cast<double>(usage.ru_maxrss) / (1024.0 * 1024.0);

    std::printf("\n=== Performance Report ===\n");
    std::printf("Processed %llu packets in %.3f seconds\n",
                static_cast<unsigned long long>(stats.count()), elapsed_s);
    std::printf("Average latency: %.2f us\n", stats.average_us());
    std::printf("Peak latency:    %.2f us\n", static_cast<double>(stats.max_ns()) / 1000.0);
    std::printf("Memory pool:     %.2f MB (static, allocated once at startup)\n", pool_mb);
    std::printf("Peak RSS:        %.2f MB\n", peak_rss_mb);

    return 0;
}
