#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <format>
#include <list>
#include <memory>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "bots/market_maker.h"
#include "bots/noise_trader.h"
#include "client/market_view.h"
#include "client/tcp_client.h"
#include "core/types.h"
#include "net/socket.h"

// keeps the market alive over the network:
//   - one market maker per symbol the exchange lists (each its own connection)
//   - one noise trader taking liquidity across all symbols
//
// usage: liquidity_bot.exe [host] [port]       (default 127.0.0.1 9000)
//
// stop it with Ctrl+C and watch the exchange cancel all its quotes
// (cancel-on-disconnect in the gateway).

using namespace exchange;

namespace {

    using MarketMaker = bots::MarketMaker<client::TcpClient>;
    using NoiseTrader = bots::NoiseTrader<client::TcpClient>;

    std::atomic<bool> g_running{ true };

    void on_signal(int) {
        g_running.store(false);
    }

    // where each market maker starts quoting. unknown symbols start at 100.00
    core::Price start_price_for(const core::Symbol& symbol) {
        const std::string_view name = core::symbol_view(symbol);
        if (name == "AAPL")
            return core::to_price(190.00);
        if (name == "MSFT")
            return core::to_price(415.00);
        return core::to_price(100.00);
    }
}

int main(int argc, char** argv) {
    const std::string host = argc > 1 ? argv[1] : "127.0.0.1";
    const auto port = static_cast<std::uint16_t>(argc > 2 ? std::atoi(argv[2]) : 9000);
    std::signal(SIGINT, on_signal);

    net::NetworkInit network;
    const core::Price tick = core::to_price(0.01);

    // the noise trader connects first; its LOGIN_ACK tells us the symbols
    client::TcpClient noise_client;
    if (!noise_client.connect(host, port, "noise")) {
        std::fprintf(stderr, "could not connect to exchange at %s:%u\n", host.c_str(), port);
        return EXIT_FAILURE;
    }
    const std::vector<core::Symbol> symbols = noise_client.symbols();
    std::printf("connected, exchange lists %zu symbol(s)\n", symbols.size());

    // std::list: clients must not move once a bot holds a reference to them
    std::list<client::TcpClient> mm_clients;
    std::vector<std::unique_ptr<MarketMaker>> makers;
    std::uint32_t seed = 11;

    for (const core::Symbol& symbol : symbols) {
        client::TcpClient& mm_client = mm_clients.emplace_back();
        const std::string name = std::format("mm-{}", core::symbol_view(symbol));
        if (!mm_client.connect(host, port, name)) {
            std::fprintf(stderr, "%s could not connect\n", name.c_str());
            return EXIT_FAILURE;
        }
        std::printf("%s is participant %u\n", name.c_str(), mm_client.participant_id());

        const core::Price start = start_price_for(symbol);
        makers.push_back(std::make_unique<MarketMaker>(mm_client, bots::MarketMakerConfig{
            .symbol = symbol,
            .start_price = start,
            .tick_size = tick,
            .half_spread_ticks = 2,
            .levels = 5,
            // roughly the same notional per symbol
            .quote_qty = static_cast<core::Quantity>(std::max<core::Price>(10, core::to_price(20'000) / start / 10 * 10)),
            .max_position = 2'000,
            .seed = seed++,
            }));
    }

    NoiseTrader noise(noise_client, bots::NoiseTraderConfig{
        .symbols = symbols,
        .tick_size = tick,
        .mean_interval = std::chrono::milliseconds(20),
        .seed = 99,
        });

    // each bot leaves its loop when its connection drops; count who is still alive
    // (asking the clients directly from this thread would race with the bot threads)
    std::atomic<int> alive{ 0 };
    std::vector<std::thread> threads;
    for (const auto& maker : makers) {
        alive.fetch_add(1);
        threads.emplace_back([&maker, &alive] { maker->run(g_running); alive.fetch_sub(1); });
    }
    alive.fetch_add(1);
    threads.emplace_back([&] { noise.run(g_running); alive.fetch_sub(1); });

    std::puts("bots running, Ctrl+C to stop");

    auto next_print = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (g_running.load()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(50));

        if (alive.load() == 0) {
            std::puts("lost connection to the exchange");
            g_running.store(false);
            break;
        }

        if (std::chrono::steady_clock::now() < next_print)
            continue;
        next_print += std::chrono::seconds(2);

        std::puts("-----");
        for (const auto& maker : makers) {
            const double pnl = static_cast<double>(maker->stat_pnl.load()) /
                static_cast<double>(core::PRICE_SCALE);
            std::fputs(std::format("  mm {:<6} fair {:>8}  position {:>+6}  pnl {:>+10.2f}  fills {:>6}  requests {:>7}\n",
                core::symbol_view(maker->symbol()), client::price_text(maker->stat_fair_value.load()),
                maker->stat_position.load(), pnl, maker->stat_fills.load(), maker->stat_requests.load()).c_str(), stdout);
        }
        std::fputs(std::format("  noise        fills {:>6}  volume {:>8}  requests {:>7}\n",
            noise.stat_fills.load(), noise.stat_volume.load(), noise.stat_requests.load()).c_str(), stdout);
        std::fflush(stdout);
    }

    for (std::thread& thread : threads) {
        thread.join();
    }
    std::puts("bots stopped");
    return EXIT_SUCCESS;
}
