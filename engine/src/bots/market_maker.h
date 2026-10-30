#pragma once

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <random>
#include <thread>
#include <unordered_map>
#include <vector>

#include "exchange/protocol.h"

// liquidity provider for one symbol.
//
// 1. fair value: a random walk in whole ticks (the "news" that moves the price)
// 2. quotes: `levels` bids below and asks above fair value, `half_spread` ticks away
// 3. inventory skew: when long, shift every quote down so selling is more likely
//    (and the opposite when short). this is what makes the noise traders' flow
//    actually move the price
// 4. hard limit: stop quoting the side that would grow the position past max
//
// each quote lives in a slot with its own little state machine:
//
//                                     +--modify--> PENDING_REPLACE --RESTED--+
//                                     |                                      |
//   EMPTY --add--> PENDING_NEW --RESTED--> LIVE <----------------------------+
//     ^                 |                   |
//     |                 |                   +--cancel--> PENDING_CANCEL
//     |                 |                   |                  |
//     +-- REJECTED / FILLED / CANCELED -----+------------------+
//
// a slot only sends something from EMPTY or LIVE, so we never fire a second
// request while the first is still in flight.
//
// reprice = MODIFY, not cancel + add: the engine cancels and re-inserts inside
// one request, so the book never shows a moment with our quote missing.
// (with cancel + add there was a gap, and a stray order far from the market
// could become the best price and get hit)

namespace exchange::bots {

    struct MarketMakerConfig {
        core::Symbol symbol{};
        core::Price start_price{ 0 };
        core::Price tick_size{ 100 };
        int half_spread_ticks{ 2 };
        int levels{ 3 };
        core::Quantity quote_qty{ 100 };
        std::int64_t max_position{ 2'000 };
        // ticks to lean the quotes per quote_qty of inventory
        double skew_ticks_per_lot{ 0.5 };
        // std deviation of one fair value step, in ticks
        double volatility_ticks{ 1.0 };
        std::chrono::milliseconds update_interval{ 100 };
        std::uint32_t seed{ 1 };
    };

    // Client: LocalClient (in-process) or TcpClient (network)
    template <typename Client>
    class MarketMaker {
    public:

        MarketMaker(Client& client, MarketMakerConfig config)
            : client_(client), config_(config), rng_(config.seed),
            slots_(static_cast<std::size_t>(config.levels) * 2U)
        {
            // keep fair value on the tick grid so every quote is too
            fair_value_ = (config_.start_price / config_.tick_size) * config_.tick_size;
            for (std::size_t index = 0; index < slots_.size(); ++index) {
                slots_[index].side = index < static_cast<std::size_t>(config_.levels)
                    ? core::Side::BUY : core::Side::SELL;
            }
        }

        void run(const std::atomic<bool>& running) {
            auto next_update = std::chrono::steady_clock::now();

            // stop on shutdown, or when the exchange connection drops
            while (running.load(std::memory_order_relaxed) && client_.connected()) {
                client_.poll([this](const protocol::ServerMessage& message) {
                    if (message.kind == protocol::MessageKind::EXECUTION) {
                        on_execution(message.payload.execution);
                    }
                    // market data is ignored: this bot *is* the price source
                    });

                const auto now = std::chrono::steady_clock::now();
                if (now >= next_update) {
                    move_fair_value();
                    next_update = now + config_.update_interval;
                }

                requote();
                publish_stats();

                // bots are not latency critical, give the cpu back
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }
        }

        // ---- read by the dashboard thread ----
        [[nodiscard]] const core::Symbol& symbol() const noexcept { return config_.symbol; }
        std::atomic<std::int64_t> stat_position{ 0 };
        std::atomic<core::Price> stat_fair_value{ 0 };
        // mark to market, in price units (divide by PRICE_SCALE)
        std::atomic<std::int64_t> stat_pnl{ 0 };
        std::atomic<std::uint64_t> stat_fills{ 0 };
        std::atomic<std::uint64_t> stat_requests{ 0 };

    private:

        enum class SlotState : std::uint8_t {
            EMPTY,
            PENDING_NEW,
            LIVE,
            PENDING_REPLACE,
            PENDING_CANCEL,
        };

        struct Slot {
            SlotState state{ SlotState::EMPTY };
            core::Side side{ core::Side::BUY };
            protocol::ClientOrderId id{ 0 };
            core::Price price{ 0 };
        };

        void move_fair_value() {
            std::normal_distribution<double> step(0.0, config_.volatility_ticks);
            const auto ticks = static_cast<core::Price>(std::llround(step(rng_)));

            const core::Price floor = config_.tick_size * 10;
            fair_value_ = std::max(floor, fair_value_ + ticks * config_.tick_size);
        }

        void requote() {
            const core::Price tick = config_.tick_size;

            // long -> negative skew -> quotes move down
            const double lots = static_cast<double>(position_) / static_cast<double>(config_.quote_qty);
            const core::Price skew = std::llround(lots * config_.skew_ticks_per_lot) * tick;
            const core::Price center = fair_value_ - skew;

            const bool may_buy = position_ < config_.max_position;
            const bool may_sell = position_ > -config_.max_position;

            for (int level = 0; level < config_.levels; ++level) {
                const core::Price offset = (config_.half_spread_ticks + level) * tick;
                const core::Price bid = center - offset;
                const core::Price ask = center + offset;

                manage(static_cast<std::size_t>(level), bid, may_buy && bid > 0);
                manage(static_cast<std::size_t>(config_.levels + level), ask, may_sell);
            }
        }

        void manage(std::size_t index, core::Price wanted_price, bool allowed) {
            Slot& slot = slots_[index];

            switch (slot.state) {
            case SlotState::EMPTY:
                if (allowed) {
                    slot.id = client_.send_limit(config_.symbol, slot.side, wanted_price,
                        config_.quote_qty);
                    slot.price = wanted_price;
                    slot.state = SlotState::PENDING_NEW;
                    slot_by_id_[slot.id] = index;
                }
                break;
            case SlotState::LIVE:
                if (!allowed) {
                    client_.send_cancel(config_.symbol, slot.id);
                    slot.state = SlotState::PENDING_CANCEL;
                }
                else if (slot.price != wanted_price) {
                    // also tops a partly filled quote back up to full size
                    client_.send_modify(config_.symbol, slot.id, config_.quote_qty, wanted_price);
                    slot.price = wanted_price;
                    slot.state = SlotState::PENDING_REPLACE;
                }
                break;
            case SlotState::PENDING_NEW:
            case SlotState::PENDING_REPLACE:
            case SlotState::PENDING_CANCEL:
                // wait for the exchange to answer first
                break;
            }
        }

        void on_execution(const protocol::ExecutionReport& report) {
            using protocol::ExecType;

            if (report.type == ExecType::FILL) {
                const auto qty = static_cast<std::int64_t>(report.qty);
                const std::int64_t signed_qty = report.side == core::Side::BUY ? qty : -qty;
                position_ += signed_qty;
                cash_ -= signed_qty * report.price;
                ++fills_;
                return;
            }

            const auto it = slot_by_id_.find(report.client_order_id);
            if (it == slot_by_id_.end())
                return;

            Slot& slot = slots_[it->second];
            switch (report.type) {
            case ExecType::RESTED:
                if (slot.state == SlotState::PENDING_NEW || slot.state == SlotState::PENDING_REPLACE) {
                    slot.state = SlotState::LIVE;
                }
                break;
            case ExecType::CANCELED:
                // a modify reports CANCELED(old) + ACCEPTED/RESTED(new) for the
                // same id: that cancel is half of the replace, not the end
                if (slot.state == SlotState::PENDING_REPLACE)
                    break;
                slot.state = SlotState::EMPTY;
                slot_by_id_.erase(it);
                break;
            case ExecType::FILLED:
            case ExecType::REJECTED:
                // order is gone from the book -> slot free again
                slot.state = SlotState::EMPTY;
                slot_by_id_.erase(it);
                break;
            default:
                break;
            }
        }

        void publish_stats() noexcept {
            stat_position.store(position_, std::memory_order_relaxed);
            stat_fair_value.store(fair_value_, std::memory_order_relaxed);
            stat_pnl.store(cash_ + position_ * fair_value_, std::memory_order_relaxed);
            stat_fills.store(fills_, std::memory_order_relaxed);
            stat_requests.store(client_.requests_sent(), std::memory_order_relaxed);
        }

        Client& client_;
        MarketMakerConfig config_;
        std::mt19937 rng_;

        std::vector<Slot> slots_;
        std::unordered_map<protocol::ClientOrderId, std::size_t> slot_by_id_;

        core::Price fair_value_{ 0 };
        std::int64_t position_{ 0 };
        // money spent (negative) / received (positive), price units * qty
        std::int64_t cash_{ 0 };
        std::uint64_t fills_{ 0 };
    };
}
