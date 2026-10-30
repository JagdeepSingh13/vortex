#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <unordered_map>
#include <vector>

#include "core/types.h"
#include "matching/matching_engine.h"

// deterministic stream of engine requests for the golden replay test and the benchmark.
// same seed -> same requests on every compiler and platform: it uses its own splitmix64
// generator and integer arithmetic only (std:: distributions differ between libraries).
// lives outside the engine: it allocates and is never on the matching thread.
//
// two modes:
//   open loop (default, golden test): cancels/modifies may target orders that are already
//     gone, so the stream does not depend on engine output at all
//   feedback (benchmark): pass each request's events to observe(); finished orders are
//     dropped, so cancels and modifies hit live orders like a real trading client

namespace exchange::support {

    class SplitMix64 {
    public:
        explicit SplitMix64(std::uint64_t seed) noexcept : state_(seed) {}

        std::uint64_t next() noexcept {
            std::uint64_t z = (state_ += 0x9E3779B97F4A7C15ULL);
            z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ULL;
            z = (z ^ (z >> 27)) * 0x94D049BB133111EBULL;
            return z ^ (z >> 31);
        }

        // integer in [low, high], inclusive
        std::int64_t between(std::int64_t low, std::int64_t high) noexcept {
            const auto span = static_cast<std::uint64_t>(high - low + 1);
            return low + static_cast<std::int64_t>(next() % span);
        }

        bool chance(int percent) noexcept { return between(1, 100) <= percent; }

    private:
        std::uint64_t state_;
    };

    struct OrderStreamConfig {
        std::uint64_t seed = 42;
        core::Price start_mid = 10'000;
        core::Price min_mid = 9'000;
        core::Price max_mid = 11'000;
        std::size_t max_live_orders = 3'000;  // keeps the book (and pools) bounded
    };

    class OrderStream {
    public:
        OrderStream(std::span<const core::Symbol> symbols, OrderStreamConfig config = {})
            : symbols_(symbols.begin(), symbols.end()), config_(config), random_(config.seed),
            mids_(symbols.size(), config.start_mid)
        {
            live_.reserve(config.max_live_orders + 1);
        }

        matching::InboundOrder next() {
            ++timestamp_;
            const auto symbol_index = static_cast<std::size_t>(
                random_.between(0, static_cast<std::int64_t>(symbols_.size()) - 1));
            drift(symbol_index);

            const int roll = static_cast<int>(random_.between(1, 100));
            if (live_.size() >= config_.max_live_orders || (!live_.empty() && roll <= 25)) {
                return cancel(pick_live());
            }
            if (!live_.empty() && roll <= 40) {
                return modify(pick_live());
            }
            return add(symbol_index);
        }

    private:
        struct Live {
            core::OrderId id;
            std::size_t symbol_index;
            core::Side side;
        };

        // the mid takes a small random walk so prices keep creating and emptying levels
        void drift(std::size_t symbol_index) {
            if (random_.chance(2)) {
                core::Price& mid = mids_[symbol_index];
                mid = std::clamp(mid + random_.between(-1, 1), config_.min_mid, config_.max_mid);
            }
        }

        core::OrderType pick_type() {
            const int roll = static_cast<int>(random_.between(1, 100));
            if (roll <= 45) return core::OrderType::LIMIT;
            if (roll <= 60) return core::OrderType::IOC;
            if (roll <= 65) return core::OrderType::MARKET;
            if (roll <= 70) return core::OrderType::GTC;
            if (roll <= 78) return core::OrderType::POST_ONLY;
            if (roll <= 83) return core::OrderType::FOK;
            if (roll <= 90) return core::OrderType::ICEBERG;
            if (roll <= 95) return core::OrderType::STOP;
            return core::OrderType::STOP_LIMIT;
        }

        matching::InboundOrder add(std::size_t symbol_index) {
            const core::Price mid = mids_[symbol_index];
            const core::Side side = random_.chance(50) ? core::Side::BUY : core::Side::SELL;
            const core::OrderType type = pick_type();
            const std::int64_t sign = side == core::Side::BUY ? -1 : 1;

            // mostly passive prices on our own side, sometimes crossing
            const core::Price offset = random_.chance(20) ? -random_.between(0, 5) : random_.between(0, 20);
            const core::Price limit_price = mid + sign * offset;
            const auto qty = static_cast<core::Quantity>(random_.between(1, 100));

            matching::InboundOrder order{
                .action = matching::RequestAction::ADD,
                .symbol = symbols_[symbol_index],
                .order_id = next_order_id_++,
                .side = side,
                .price = limit_price,
                .qty = qty,
                .order_type = type,
                .timestamp = timestamp_,
                .participant_id = static_cast<core::ParticipantId>(random_.between(1, 8)),
            };

            if (type == core::OrderType::MARKET || type == core::OrderType::STOP) {
                order.price = 0;
            }
            if (type == core::OrderType::STOP || type == core::OrderType::STOP_LIMIT) {
                // a stop sits on the far side of the mid and fires when price moves there
                order.trigger_price = mid - sign * random_.between(1, 10);
            }
            if (type == core::OrderType::ICEBERG) {
                order.display_qty = static_cast<core::Quantity>(random_.between(1, qty));
            }

            push_live(Live{ order.order_id, symbol_index, side });
            return order;
        }

        // the order may already be filled or canceled: the engine then rejects,
        // which keeps the reject paths in the stream too
        Live pick_live() {
            const auto index = static_cast<std::size_t>(
                random_.between(0, static_cast<std::int64_t>(live_.size()) - 1));
            const Live picked = live_[index];
            remove_at(index);
            return picked;
        }

        matching::InboundOrder cancel(const Live& target) {
            return matching::InboundOrder{
                .action = matching::RequestAction::CANCEL,
                .symbol = symbols_[target.symbol_index],
                .order_id = target.id,
                .timestamp = timestamp_,
            };
        }

        matching::InboundOrder modify(const Live& target) {
            const core::Price mid = mids_[target.symbol_index];
            const std::int64_t sign = target.side == core::Side::BUY ? -1 : 1;
            push_live(target);  // still alive after a modify (if it was before)
            return matching::InboundOrder{
                .action = matching::RequestAction::MODIFY,
                .symbol = symbols_[target.symbol_index],
                .order_id = target.id,
                .side = target.side,
                .price = mid + sign * random_.between(-2, 20),
                .qty = static_cast<core::Quantity>(random_.between(1, 100)),
                .timestamp = timestamp_,
            };
        }

        void push_live(const Live& entry) {
            position_[entry.id] = live_.size();
            live_.push_back(entry);
        }

        // swap-remove: O(1), order of live_ does not matter
        void remove_at(std::size_t index) {
            position_.erase(live_[index].id);
            if (index != live_.size() - 1) {
                live_[index] = live_.back();
                position_[live_[index].id] = index;
            }
            live_.pop_back();
        }

        std::optional<Live> remove_id(core::OrderId id) {
            const auto it = position_.find(id);
            if (it == position_.end())
                return std::nullopt;
            const Live entry = live_[it->second];
            remove_at(it->second);
            return entry;
        }

    public:
        // feedback mode: forget orders the engine reports as finished
        void observe(std::span<const matching::Event> events) {
            std::optional<Live> replaced;  // a modify is CANCELED then ACCEPTED under the same id
            for (const matching::Event& event : events) {
                switch (event.type) {
                case matching::EventType::ORDER_FILLED:
                    (void)remove_id(event.payload.order_filled.order_id);
                    break;
                case matching::EventType::ORDER_CANCELED:
                    replaced = remove_id(event.payload.order_canceled.order_id);
                    break;
                case matching::EventType::ORDER_ACCEPTED:
                    if (replaced && replaced->id == event.payload.order_accepted.order_id) {
                        push_live(*replaced);
                    }
                    replaced.reset();
                    break;
                case matching::EventType::ORDER_REJECTED:
                    // a rejected modify of a stop leaves the stop alive; any other reject
                    // means the id is not (or no longer) in the book
                    if (event.payload.order_rejected.reason_code != matching::ReasonCode::INVALID_MODIFICATION) {
                        (void)remove_id(event.payload.order_rejected.order_id);
                    }
                    break;
                default:
                    break;
                }
            }
        }

    private:
        std::vector<core::Symbol> symbols_;
        OrderStreamConfig config_;
        SplitMix64 random_;
        std::vector<core::Price> mids_;
        std::vector<Live> live_;
        std::unordered_map<core::OrderId, std::size_t> position_;
        core::OrderId next_order_id_{ 1 };
        core::Timestamp timestamp_{ 0 };
    };
}
