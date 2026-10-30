#pragma once

#include <cstddef>
#include <optional>
#include <span>
#include <vector>

#include "core/types.h"
#include "matching/events.h"
#include "matching/order_book.h"

// shared helpers for the order book tests

namespace exchange::testing {

    using matching::Event;
    using matching::EventType;
    using matching::ReasonCode;

    // small capacities so the recycling tests actually hit the limits
    using Book = matching::OrderBook<1024, 256, 1024, 64>;

    inline const core::Symbol SYMBOL = core::make_symbol("TEST");

    inline std::vector<EventType> types_of(std::span<const Event> events) {
        std::vector<EventType> types;
        for (const Event& event : events) {
            types.push_back(event.type);
        }
        return types;
    }

    inline std::size_t count_of(std::span<const Event> events, EventType type) {
        std::size_t count = 0;
        for (const Event& event : events) {
            if (event.type == type)
                ++count;
        }
        return count;
    }

    // all trades in the span, in order
    inline std::vector<matching::Trade> trades_of(std::span<const Event> events) {
        std::vector<matching::Trade> trades;
        for (const Event& event : events) {
            if (event.type == EventType::TRADE)
                trades.push_back(event.payload.trade);
        }
        return trades;
    }

    // reject reason if the span is a single ORDER_REJECTED, otherwise nullopt
    inline std::optional<ReasonCode> reject_reason(std::span<const Event> events) {
        if (events.size() != 1 || events[0].type != EventType::ORDER_REJECTED)
            return std::nullopt;
        return events[0].payload.order_rejected.reason_code;
    }
}
