#include <array>
#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <format>
#include <memory>
#include <thread>
#include <vector>

#include "bots/market_maker.h"
#include "bots/noise_trader.h"
#include "client/local_client.h"
#include "client/market_view.h"
#include "core/types.h"
#include "exchange/engine_runner.h"
#include "exchange/exchange_core.h"
#include "exchange/session.h"
#include "matching/matching_engine.h"

// everything in one process, no networking: engine + bots over SPSC queues.
// handy for debugging the bots, and as a baseline to compare against TCP.
//
// usage: simulation.exe [seconds]     (no argument = run until Ctrl+C)

using namespace exchange;

namespace {

    using Engine = matching::MatchingEngine<100'000, 10'000, 65'536, 8'192>;
    using Core = gateway::ExchangeCore<Engine>;
    using MarketMaker = bots::MarketMaker<client::LocalClient>;
    using NoiseTrader = bots::NoiseTrader<client::LocalClient>;

    std::atomic<bool> g_running{ true };

    void on_signal(int) {
        g_running.store(false);
    }

    void print_market_maker(const MarketMaker& mm) {
        const double pnl = static_cast<double>(mm.stat_pnl.load(std::memory_order_relaxed)) /
            static_cast<double>(core::PRICE_SCALE);
        std::fputs(std::format("  mm {:<6} fair {:>8}  position {:>+6}  pnl {:>+10.2f}  fills {:>6}  requests {:>7}\n",
            core::symbol_view(mm.symbol()),
            client::price_text(mm.stat_fair_value.load(std::memory_order_relaxed)),
            mm.stat_position.load(std::memory_order_relaxed), pnl,
            mm.stat_fills.load(std::memory_order_relaxed),
            mm.stat_requests.load(std::memory_order_relaxed)).c_str(), stdout);
    }
}

int main(int argc, char** argv) {
    const int run_seconds = argc > 1 ? std::atoi(argv[1]) : 0;
    std::signal(SIGINT, on_signal);

    const core::Price tick = core::to_price(0.01);
    const std::array symbols{ core::make_symbol("AAPL"), core::make_symbol("MSFT") };

    Core core(symbols);

    auto monitor_session = std::make_unique<gateway::Session>(1, "monitor");
    auto aapl_session = std::make_unique<gateway::Session>(2, "mm-aapl");
    auto msft_session = std::make_unique<gateway::Session>(3, "mm-msft");
    auto noise_session = std::make_unique<gateway::Session>(4, "noise");

    gateway::EngineRunner<Core> runner(core, {
        monitor_session.get(), aapl_session.get(), msft_session.get(), noise_session.get() });

    client::LocalClient aapl_client(*aapl_session);
    client::LocalClient msft_client(*msft_session);
    client::LocalClient noise_client(*noise_session);

    MarketMaker aapl_mm(aapl_client, bots::MarketMakerConfig{
        .symbol = symbols[0],
        .start_price = core::to_price(190.00),
        .tick_size = tick,
        .half_spread_ticks = 2,
        .levels = 5,
        .quote_qty = 100,
        .max_position = 2'000,
        .seed = 11,
        });

    MarketMaker msft_mm(msft_client, bots::MarketMakerConfig{
        .symbol = symbols[1],
        .start_price = core::to_price(415.00),
        .tick_size = tick,
        .half_spread_ticks = 3,
        .levels = 5,
        .quote_qty = 50,
        .max_position = 1'000,
        .volatility_ticks = 2.0,
        .seed = 22,
        });

    NoiseTrader noise(noise_client, bots::NoiseTraderConfig{
        .symbols = { symbols.begin(), symbols.end() },
        .tick_size = tick,
        .mean_interval = std::chrono::milliseconds(20),
        .seed = 33,
        });

    std::vector<std::thread> threads;
    threads.emplace_back([&] { runner.run(g_running); });
    threads.emplace_back([&] { aapl_mm.run(g_running); });
    threads.emplace_back([&] { msft_mm.run(g_running); });
    threads.emplace_back([&] { noise.run(g_running); });

    std::puts("simulation running, Ctrl+C to stop");

    client::LocalClient monitor(*monitor_session);
    client::MarketView market;
    for (const core::Symbol& symbol : symbols) {
        market.track(symbol);
    }

    const auto start = std::chrono::steady_clock::now();
    auto next_print = start + std::chrono::seconds(1);
    std::uint64_t last_requests = 0;

    while (g_running.load()) {
        monitor.poll([&](const protocol::ServerMessage& message) { market.on_message(message); });

        const auto now = std::chrono::steady_clock::now();
        if (now >= next_print) {
            const auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(now - start).count();
            const std::uint64_t requests = runner.requests_processed();

            std::fputs(std::format("\n===== t={}s   engine requests {} ({}/s)   md dropped {} =====\n",
                elapsed, requests, requests - last_requests, runner.dropped_market_data()).c_str(), stdout);
            market.print_all();
            print_market_maker(aapl_mm);
            print_market_maker(msft_mm);
            std::fputs(std::format("  noise        fills {:>6}  volume {:>8}  requests {:>7}\n",
                noise.stat_fills.load(), noise.stat_volume.load(), noise.stat_requests.load()).c_str(), stdout);
            std::fflush(stdout);

            last_requests = requests;
            next_print += std::chrono::seconds(1);

            if (run_seconds > 0 && elapsed >= run_seconds) {
                g_running.store(false);
            }
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }

    for (std::thread& thread : threads) {
        thread.join();
    }

    std::puts("\nsimulation stopped");
    return EXIT_SUCCESS;
}
