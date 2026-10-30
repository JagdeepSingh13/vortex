#pragma once

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <map>
#include <optional>
#include <random>
#include <thread>
#include <utility>
#include <vector>

#include "exchange/protocol.h"

// liquidity taker. a market maker alone never trades: its bids are always
// below its asks. this bot crosses the spread at random times so trades print.
//
// - waits an exponentially distributed time (random arrivals, like real flow)
// - picks a symbol and a side at random
// - sends an IOC a few ticks through the best price it saw in market data.
//   IOC + limit price = "take what is there, but never pay more than X":
//   unlike a pure MARKET order it can't sweep a thin book to a silly price
// - price band: if the best price is far from the last trade (someone's stray
//   order is the only thing on that side), it doesn't trade at all

namespace exchange::bots {

    struct NoiseTraderConfig {
        std::vector<core::Symbol> symbols{};
        core::Price tick_size{ 100 };
        core::Quantity min_qty{ 10 };
        core::Quantity max_qty{ 150 };
        int max_slippage_ticks{ 3 };
        // skip if the touch is further than this from the last trade
        int price_band_ticks{ 20 };
        std::chrono::milliseconds mean_interval{ 50 };
        std::uint32_t seed{ 2 };
    };

    // Client: LocalClient (in-process) or TcpClient (network)
    template <typename Client>
    class NoiseTrader {
    public:

        NoiseTrader(Client& client, NoiseTraderConfig config)
            : client_(client), config_(std::move(config)), rng_(config_.seed)
        {
        }

        void run(const std::atomic<bool>& running) {
            auto next_order = std::chrono::steady_clock::now() + next_delay();

            // stop on shutdown, or when the exchange connection drops
            while (running.load(std::memory_order_relaxed) && client_.connected()) {
                client_.poll([this](const protocol::ServerMessage& message) {
                    on_message(message);
                    });

                const auto now = std::chrono::steady_clock::now();
                if (now >= next_order) {
                    send_random_order();
                    next_order = now + next_delay();
                }

                stat_requests.store(client_.requests_sent(), std::memory_order_relaxed);
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }
        }

        // ---- read by the dashboard thread ----
        std::atomic<std::uint64_t> stat_fills{ 0 };
        std::atomic<std::uint64_t> stat_volume{ 0 };
        std::atomic<std::uint64_t> stat_requests{ 0 };

    private:

        struct TopOfBook {
            std::optional<core::Price> bid;
            std::optional<core::Price> ask;
            std::optional<core::Price> last_trade;
        };

        std::chrono::steady_clock::duration next_delay() {
            std::exponential_distribution<double> delay(
                1.0 / static_cast<double>(config_.mean_interval.count()));
            return std::chrono::duration_cast<std::chrono::steady_clock::duration>(
                std::chrono::duration<double, std::milli>(delay(rng_)));
        }

        void on_message(const protocol::ServerMessage& message) {
            if (message.kind == protocol::MessageKind::BOOK) {
                const protocol::BookUpdate& book = message.payload.book;
                TopOfBook& top = tops_[book.symbol];
                top.bid = book.bid_count > 0 ? std::optional(book.bids[0].price) : std::nullopt;
                top.ask = book.ask_count > 0 ? std::optional(book.asks[0].price) : std::nullopt;
                return;
            }

            if (message.kind == protocol::MessageKind::TRADE) {
                tops_[message.payload.trade.symbol].last_trade = message.payload.trade.price;
                return;
            }

            if (message.kind == protocol::MessageKind::EXECUTION &&
                message.payload.execution.type == protocol::ExecType::FILL) {
                stat_fills.fetch_add(1, std::memory_order_relaxed);
                stat_volume.fetch_add(message.payload.execution.qty, std::memory_order_relaxed);
            }
        }

        void send_random_order() {
            if (config_.symbols.empty())
                return;

            std::uniform_int_distribution<std::size_t> pick_symbol(0, config_.symbols.size() - 1);
            std::bernoulli_distribution pick_buy(0.5);
            std::uniform_int_distribution<core::Quantity> pick_qty(config_.min_qty, config_.max_qty);
            std::uniform_int_distribution<int> pick_slippage(0, config_.max_slippage_ticks);

            const core::Symbol& symbol = config_.symbols[pick_symbol(rng_)];
            const auto top_it = tops_.find(symbol);
            if (top_it == tops_.end())
                return;

            const bool buy = pick_buy(rng_);
            const std::optional<core::Price> touch = buy ? top_it->second.ask : top_it->second.bid;
            // nothing to hit on that side yet
            if (!touch)
                return;

            const std::optional<core::Price> last = top_it->second.last_trade;
            const core::Price band = config_.price_band_ticks * config_.tick_size;
            if (last && (*touch > *last + band || *touch < *last - band))
                return;

            const core::Price slippage = pick_slippage(rng_) * config_.tick_size;
            const core::Price limit = buy ? *touch + slippage : std::max(config_.tick_size, *touch - slippage);

            client_.send_limit(symbol, buy ? core::Side::BUY : core::Side::SELL,
                limit, pick_qty(rng_), core::OrderType::IOC);
        }

        Client& client_;
        NoiseTraderConfig config_;
        std::mt19937 rng_;
        std::map<core::Symbol, TopOfBook, core::SymbolLess> tops_;
    };
}
