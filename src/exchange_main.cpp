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

#include "client/local_client.h"
#include "client/market_view.h"
#include "core/types.h"
#include "exchange/engine_runner.h"
#include "exchange/exchange_core.h"
#include "exchange/session.h"
#include "exchange/tcp_gateway.h"
#include "matching/matching_engine.h"
#include "net/socket.h"

// the exchange server.
//
//   clients ==TCP==> [ gateway thread ] ==SPSC==> [ engine thread ]
//                                                       |
//                    [ main thread: dashboard ] <==SPSC=/   (market data only)
//
// usage: exchange.exe [port]          (default 9000, Ctrl+C to stop)
// then connect:  liquidity_bot.exe    trader_terminal.exe

using namespace exchange;

namespace {

    // much smaller than the defaults (1M orders = ~300MB per book)
    using Engine = matching::MatchingEngine<100'000, 10'000, 65'536, 8'192>;
    using Core = gateway::ExchangeCore<Engine>;

    constexpr std::size_t MAX_CLIENTS = 16;

    std::atomic<bool> g_running{ true };

    void on_signal(int) {
        g_running.store(false);
    }
}

int main(int argc, char** argv) {
    const auto port = static_cast<std::uint16_t>(argc > 1 ? std::atoi(argv[1]) : 9000);
    std::signal(SIGINT, on_signal);

    net::NetworkInit network;
    if (!network.ok()) {
        std::fputs("failed to start networking\n", stderr);
        return EXIT_FAILURE;
    }

    const std::array symbols{ core::make_symbol("AAPL"), core::make_symbol("MSFT") };
    Core core(symbols);

    // ---- sessions: 1 = local dashboard, 2.. = slots lent to TCP clients ----
    std::vector<std::unique_ptr<gateway::Session>> sessions;
    sessions.push_back(std::make_unique<gateway::Session>(1, "dashboard"));
    for (std::size_t index = 0; index < MAX_CLIENTS; ++index) {
        const auto participant = static_cast<core::ParticipantId>(2 + index);
        sessions.push_back(std::make_unique<gateway::Session>(participant, std::format("slot-{}", index)));
    }

    std::vector<gateway::Session*> all_sessions;
    std::vector<gateway::Session*> client_slots;
    for (const auto& session : sessions) {
        all_sessions.push_back(session.get());
        if (session->participant_id != 1) {
            client_slots.push_back(session.get());
        }
    }

    gateway::EngineRunner<Core> runner(core, all_sessions);
    gateway::TcpGateway gateway(client_slots, symbols);

    if (!gateway.listen(port)) {
        std::fprintf(stderr, "could not listen on port %u (already in use?)\n", port);
        return EXIT_FAILURE;
    }

    std::thread engine_thread([&] { runner.run(g_running); });
    std::thread gateway_thread([&] { gateway.run(g_running); });

    std::printf("exchange listening on port %u, Ctrl+C to stop\n", port);

    // ---- main thread: dashboard ----
    client::LocalClient dashboard(*sessions.front());
    client::MarketView market;
    for (const core::Symbol& symbol : symbols) {
        market.track(symbol);
    }

    const auto start = std::chrono::steady_clock::now();
    auto next_print = start + std::chrono::seconds(2);
    std::uint64_t last_requests = 0;

    while (g_running.load()) {
        dashboard.poll([&](const protocol::ServerMessage& message) { market.on_message(message); });

        const auto now = std::chrono::steady_clock::now();
        if (now >= next_print) {
            const auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(now - start).count();
            const std::uint64_t requests = runner.requests_processed();

            std::fputs(std::format("\n===== t={}s   clients {}   engine requests {} (+{})   md dropped {} =====\n",
                elapsed, gateway.connected_clients(), requests, requests - last_requests,
                runner.dropped_market_data()).c_str(), stdout);
            market.print_all();
            std::fflush(stdout);

            last_requests = requests;
            next_print += std::chrono::seconds(2);
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }

    gateway_thread.join();
    engine_thread.join();
    std::puts("\nexchange stopped");
    return EXIT_SUCCESS;
}
