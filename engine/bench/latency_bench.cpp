#include <algorithm>
#include <array>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <ctime>
#include <format>
#include <fstream>
#include <memory>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include <x86intrin.h>

#include "matching/matching_engine.h"
#include "support/order_stream.h"

// latency and throughput benchmark for the matching engine (ADR 0010).
//
//   1. warm up: run a prefix so caches, branch predictors and pools are hot
//   2. latency run: time every single process_order() with the CPU's TSC. the order
//      generator runs in feedback mode (it sees the engine's events, so cancels and
//      modifies target live orders). it runs outside the timed region, and every
//      request is recorded
//   3. throughput run: a fresh engine replays the recorded requests, no per-request timing
//
// this is a closed loop (one request at a time, the next only after the previous
// returns), so it measures service time of the matching code, not queueing delay.
//
// usage: latency_bench [requests] [output.md]
//   env COMMIT=<sha>   recorded in the report

namespace {

    using namespace exchange;

    // capacities big enough that the stream never exhausts a pool
    using Engine = matching::MatchingEngine<262'144, 16'384, 8'192, 16'384>;

    const std::array SYMBOLS{ core::make_symbol("AAPL"), core::make_symbol("MSFT") };

    // ---------------- timing ----------------

    // rdtscp waits for earlier instructions to finish before reading the counter,
    // so the work being measured cannot leak past the second timestamp
    inline std::uint64_t ticks() noexcept {
        unsigned int aux = 0;
        return __rdtscp(&aux);
    }

    // TSC ticks per nanosecond, measured against the steady clock over 200 ms
    double calibrate_tsc() {
        const auto wall_start = std::chrono::steady_clock::now();
        const std::uint64_t tsc_start = ticks();
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
        const std::uint64_t tsc_end = ticks();
        const auto wall_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now() - wall_start).count();
        return static_cast<double>(tsc_end - tsc_start) / static_cast<double>(wall_ns);
    }

    // cost of the measurement itself: two back-to-back timestamps
    std::uint64_t timer_overhead_ticks() {
        std::vector<std::uint64_t> samples(100'000);
        for (auto& sample : samples) {
            const std::uint64_t start = ticks();
            sample = ticks() - start;
        }
        std::ranges::sort(samples);
        return samples[samples.size() / 2];
    }

    // ---------------- request classes ----------------

    enum Kind : std::size_t { ADD_RESTS, ADD_TRADES, ADD_OTHER, CANCEL, MODIFY, REJECTED, ALL, KIND_COUNT };
    constexpr std::array<const char*, KIND_COUNT> KIND_NAMES{
        "add, rests (no trade)", "add, trades", "add, IOC/FOK/stop/other", "cancel", "modify", "rejected", "all requests" };

    Kind classify(const matching::InboundOrder& order, std::span<const matching::Event> events) {
        bool trade = false;
        bool rested = false;
        for (const auto& event : events) {
            if (event.type == matching::EventType::ORDER_REJECTED && events.size() == 1)
                return REJECTED;
            trade |= event.type == matching::EventType::TRADE;
            rested |= event.type == matching::EventType::ORDER_RESTED;
        }
        switch (order.action) {
        case matching::RequestAction::CANCEL: return CANCEL;
        case matching::RequestAction::MODIFY: return MODIFY;
        case matching::RequestAction::ADD: break;
        }
        if (trade) return ADD_TRADES;
        if (rested) return ADD_RESTS;
        return ADD_OTHER;
    }

    struct Percentiles {
        std::size_t count = 0;
        double p50 = 0, p90 = 0, p99 = 0, p999 = 0, max = 0, mean = 0;
    };

    Percentiles summarize(std::vector<std::uint64_t>& samples, double ticks_per_ns) {
        Percentiles out;
        out.count = samples.size();
        if (samples.empty())
            return out;
        std::ranges::sort(samples);
        const auto at = [&](double q) {
            const auto index = static_cast<std::size_t>(q * static_cast<double>(samples.size() - 1));
            return static_cast<double>(samples[index]) / ticks_per_ns;
        };
        double sum = 0;
        for (const auto sample : samples) sum += static_cast<double>(sample);
        out.p50 = at(0.50);
        out.p90 = at(0.90);
        out.p99 = at(0.99);
        out.p999 = at(0.999);
        out.max = static_cast<double>(samples.back()) / ticks_per_ns;
        out.mean = sum / static_cast<double>(samples.size()) / ticks_per_ns;
        return out;
    }

    // ---------------- machine description ----------------

    std::string read_first_line_with(const char* path, const char* prefix) {
        std::ifstream in(path);
        std::string line;
        while (std::getline(in, line)) {
            if (line.starts_with(prefix)) {
                const auto colon = line.find(':');
                return colon == std::string::npos ? line : line.substr(colon + 2);
            }
        }
        return "unknown";
    }

    std::string read_all(const char* path) {
        std::ifstream in(path);
        std::string text;
        std::getline(in, text);
        return text;
    }

    std::string env_or(const char* name, const char* fallback) {
        const char* value = std::getenv(name);
        return value != nullptr && *value != '\0' ? value : fallback;
    }

    std::string today_utc() {
        const std::time_t now = std::time(nullptr);
        std::tm utc{};
        gmtime_r(&now, &utc);
        char buffer[16];
        std::strftime(buffer, sizeof(buffer), "%Y-%m-%d", &utc);
        return buffer;
    }
}

int main(int argc, char** argv) {
    const std::size_t measured = argc > 1 ? std::strtoull(argv[1], nullptr, 10) : 2'000'000;
    const std::size_t warmup = measured / 10;
    const std::string output_path = argc > 2 ? argv[2] : "";

    std::vector<matching::InboundOrder> stream;  // recorded during the latency run
    stream.reserve(warmup + measured);

    std::printf("calibrating timer...\n");
    const double ticks_per_ns = calibrate_tsc();
    const std::uint64_t overhead = timer_overhead_ticks();

    // ---- latency run ----
    std::printf("latency run: %zu warm-up + %zu measured requests\n", warmup, measured);
    std::array<std::vector<std::uint64_t>, KIND_COUNT> samples;
    for (auto& bucket : samples) bucket.reserve(measured);
    std::uint64_t trades = 0;
    std::uint64_t events_total = 0;
    {
        auto engine = std::make_unique<Engine>();
        engine->prepare_symbols(SYMBOLS);
        support::OrderStream generator(SYMBOLS, support::OrderStreamConfig{ .seed = 7 });

        for (std::size_t index = 0; index < warmup; ++index) {
            stream.push_back(generator.next());
            generator.observe(engine->process_order(stream.back()));
        }

        for (std::size_t index = 0; index < measured; ++index) {
            stream.push_back(generator.next());
            const matching::InboundOrder& order = stream.back();

            const std::uint64_t start = ticks();
            const auto events = engine->process_order(order);
            const std::uint64_t end = ticks();

            generator.observe(events);
            const std::uint64_t elapsed = end - start > overhead ? end - start - overhead : 0;
            const Kind kind = classify(order, events);
            samples[kind].push_back(elapsed);
            samples[ALL].push_back(elapsed);

            events_total += events.size();
            for (const auto& event : events) {
                trades += event.type == matching::EventType::TRADE ? 1 : 0;
            }
        }
    }

    // ---- throughput run: fresh engine, same stream, no per-request timers ----
    std::printf("throughput run...\n");
    double throughput = 0;
    {
        auto engine = std::make_unique<Engine>();
        engine->prepare_symbols(SYMBOLS);
        std::uint64_t checksum = 0;

        for (std::size_t index = 0; index < warmup; ++index) {
            checksum += engine->process_order(stream[index]).size();
        }
        const auto start = std::chrono::steady_clock::now();
        for (std::size_t index = warmup; index < stream.size(); ++index) {
            checksum += engine->process_order(stream[index]).size();
        }
        const auto seconds = std::chrono::duration<double>(std::chrono::steady_clock::now() - start).count();
        throughput = static_cast<double>(measured) / seconds;
        // use the checksum so the compiler cannot drop the loop
        if (checksum == 0) std::puts("(no events)");
    }

    // ---- report ----
    std::ostringstream report;
    report << "# Engine latency benchmark — " << today_utc() << "\n\n";
    report << "## Conditions\n\n";
    report << "| | |\n|---|---|\n";
    report << "| Commit | `" << env_or("COMMIT", "unknown") << "` |\n";
    report << "| CPU | " << read_first_line_with("/proc/cpuinfo", "model name") << " |\n";
    report << "| Logical CPUs visible | " << std::thread::hardware_concurrency() << " |\n";
    report << "| Memory | " << read_first_line_with("/proc/meminfo", "MemTotal") << " |\n";
    report << "| Kernel | " << read_all("/proc/sys/kernel/osrelease") << " |\n";
    report << "| Environment | " << env_or("BENCH_ENV", "unspecified") << " |\n";
    report << "| Compiler | GCC " << __VERSION__ << " |\n";
    report << "| Build | Release, `" << VORTEX_CXX_FLAGS << "` |\n";
    report << "| Timer | `rdtscp`, " << std::format("{:.3f}", ticks_per_ns)
           << " ticks/ns, overhead " << overhead << " ticks (subtracted) |\n";
    report << "| CPU pinning | " << env_or("BENCH_CPU", "none") << " |\n";
    report << "| Workload | seeded order stream (seed 7, feedback mode), 2 symbols, " << warmup << " warm-up + "
           << measured << " measured requests, single thread, closed loop |\n\n";

    report << "## Latency per request (ns)\n\n";
    report << "Time inside `MatchingEngine::process_order` for one request, including all events it produces.\n\n";
    report << "| Request | Count | p50 | p90 | p99 | p99.9 | max | mean |\n";
    report << "|---|---:|---:|---:|---:|---:|---:|---:|\n";
    for (std::size_t kind = 0; kind < KIND_COUNT; ++kind) {
        const Percentiles p = summarize(samples[kind], ticks_per_ns);
        if (p.count == 0) continue;
        report << std::format("| {} | {} | {:.0f} | {:.0f} | {:.0f} | {:.0f} | {:.0f} | {:.0f} |\n",
            KIND_NAMES[kind], p.count, p.p50, p.p90, p.p99, p.p999, p.max, p.mean);
    }

    report << "\n## Throughput\n\n";
    report << std::format("{:.2f} million requests/second (single thread, no per-request timers)\n\n", throughput / 1e6);
    report << std::format("Trades: {} ({:.1f}% of requests); events per request: {:.2f}\n",
        trades, 100.0 * static_cast<double>(trades) / static_cast<double>(measured),
        static_cast<double>(events_total) / static_cast<double>(measured));

    std::fputs(report.str().c_str(), stdout);
    if (!output_path.empty()) {
        std::ofstream(output_path) << report.str();
        std::printf("\nwritten to %s\n", output_path.c_str());
    }
    return EXIT_SUCCESS;
}
