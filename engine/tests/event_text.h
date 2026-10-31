#pragma once

#include <format>
#include <string>
#include <string_view>

#include "core/types.h"
#include "matching/events.h"
#include "matching/matching_engine.h"

// stable text form of requests and events for the golden replay test.
// every field that matching decides is printed explicitly. wall-clock time is
// deliberately NOT printed: it differs on every run, matching must not depend on it.

namespace exchange::testing {

    inline std::string_view name_of(core::Side side) {
        return side == core::Side::BUY ? "BUY" : "SELL";
    }

    inline std::string_view name_of(core::OrderType type) {
        switch (type) {
        case core::OrderType::LIMIT: return "LIMIT";
        case core::OrderType::MARKET: return "MARKET";
        case core::OrderType::IOC: return "IOC";
        case core::OrderType::FOK: return "FOK";
        case core::OrderType::GTC: return "GTC";
        case core::OrderType::STOP: return "STOP";
        case core::OrderType::STOP_LIMIT: return "STOP_LIMIT";
        case core::OrderType::ICEBERG: return "ICEBERG";
        case core::OrderType::POST_ONLY: return "POST_ONLY";
        }
        return "?";
    }

    inline std::string_view name_of(matching::RequestAction action) {
        switch (action) {
        case matching::RequestAction::ADD: return "ADD";
        case matching::RequestAction::CANCEL: return "CANCEL";
        case matching::RequestAction::MODIFY: return "MODIFY";
        }
        return "?";
    }

    inline std::string to_text(const matching::InboundOrder& order) {
        return std::format("R {} {} id={} {} {} px={} qty={} trig={} disp={} p={} ts={}",
            name_of(order.action), core::symbol_view(order.symbol), order.order_id,
            name_of(order.side), name_of(order.order_type), order.price, order.qty,
            order.trigger_price, order.display_qty, order.participant_id, order.timestamp);
    }

    inline std::string to_text(const matching::Event& event) {
        using matching::EventType;
        const auto& p = event.payload;
        switch (event.type) {
        case EventType::ORDER_ACCEPTED:
            return std::format("  ACCEPTED seq={} ts={} id={} {} {} {} px={} qty={}",
                p.order_accepted.sequence_number, p.order_accepted.timestamp, p.order_accepted.order_id,
                core::symbol_view(p.order_accepted.symbol), name_of(p.order_accepted.side),
                name_of(p.order_accepted.order_type), p.order_accepted.price, p.order_accepted.qty);
        case EventType::ORDER_RESTED:
            return std::format("  RESTED seq={} ts={} id={} {} {} px={} qty={}",
                p.order_rested.sequence_number, p.order_rested.timestamp, p.order_rested.order_id,
                core::symbol_view(p.order_rested.symbol), name_of(p.order_rested.side),
                p.order_rested.price, p.order_rested.qty);
        case EventType::ORDER_REJECTED:
            return std::format("  REJECTED seq={} ts={} id={} reason={}",
                p.order_rejected.sequence_number, p.order_rejected.timestamp, p.order_rejected.order_id,
                static_cast<int>(p.order_rejected.reason_code));
        case EventType::ORDER_REDUCED:
            return std::format("  REDUCED seq={} ts={} id={} qty={}",
                p.order_reduced.sequence_number, p.order_reduced.timestamp, p.order_reduced.order_id,
                p.order_reduced.new_qty);
        case EventType::ORDER_PARTIALLY_FILLED:
            return std::format("  PARTIAL seq={} ts={} id={} filled={} left={} px={}",
                p.order_partially_filled.sequence_number, p.order_partially_filled.timestamp,
                p.order_partially_filled.order_id, p.order_partially_filled.filled_qty,
                p.order_partially_filled.remaining_qty, p.order_partially_filled.price);
        case EventType::ORDER_FILLED:
            return std::format("  FILLED seq={} ts={} id={} filled={} px={}",
                p.order_filled.sequence_number, p.order_filled.timestamp, p.order_filled.order_id,
                p.order_filled.filled_qty, p.order_filled.price);
        case EventType::ORDER_CANCELED:
            return std::format("  CANCELED seq={} ts={} id={} qty={}",
                p.order_canceled.sequence_number, p.order_canceled.timestamp, p.order_canceled.order_id,
                p.order_canceled.canceled_qty);
        case EventType::TRADE:
            return std::format("  TRADE seq={} ts={} match={} {} buy={} sell={} px={} qty={} aggr={}",
                p.trade.sequence_number, p.trade.timestamp, p.trade.match_id,
                core::symbol_view(p.trade.symbol), p.trade.buy_order_id, p.trade.sell_order_id,
                p.trade.price, p.trade.qty, name_of(p.trade.aggressor_side));
        }
        return "  ?";
    }
}
