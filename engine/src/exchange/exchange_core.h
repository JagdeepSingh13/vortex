#pragma once

#include <map>
#include <memory>
#include <span>

#include "core/clock.h"
#include "core/types.h"
#include "exchange/protocol.h"
#include "matching/matching_engine.h"

// everything that runs on the engine thread between "a request arrived"
// and "messages go out":
//
//   ClientRequest -> validate -> InboundOrder -> MatchingEngine -> Events
//                 -> private ExecutionReports to each order's owner
//                 -> public TradeUpdates + BookUpdates to everyone
//
// it knows nothing about threads or queues: it hands messages to a Sink.
// a sink needs two functions:
//   void send(core::ParticipantId to, const protocol::ServerMessage&)
//   void broadcast(const protocol::ServerMessage&)
// so the same core works with the Kafka service (kafka_main.cpp) or a test.

namespace exchange::gateway {

    template <typename Engine>
    class ExchangeCore {
    public:

        explicit ExchangeCore(std::span<const core::Symbol> symbols)
            : engine_(std::make_unique<Engine>())
        {
            // books are created up front. a request for any other symbol is
            // rejected, otherwise a typo would allocate a whole new book
            engine_->prepare_symbols(symbols);
            for (const core::Symbol& symbol : symbols) {
                last_books_.try_emplace(symbol, protocol::BookUpdate{});
            }
        }

        template <typename Sink>
        void handle(core::ParticipantId participant,
            const protocol::ClientRequest& request, Sink& sink)
        {
            // two clocks, two jobs:
            //  - logical time goes into the book: deterministic, so a replay gives
            //    identical results (the golden replay test relies on this)
            //  - wall time is read once per request and stamped on every message the
            //    request produces; it never reaches matching (ADR 0009)
            const core::Timestamp timestamp = clock_.next();
            request_time_ns_ = core::wall_clock_ns();

            if (engine_->find_book(request.symbol) == nullptr) {
                reject(participant, request, matching::ReasonCode::UNKNOWN_SYMBOL, sink);
                return;
            }

            if (request.client_order_id == 0 ||
                request.client_order_id > protocol::MAX_CLIENT_ORDER_ID ||
                participant > protocol::MAX_PARTICIPANT_ID) {
                reject(participant, request, matching::ReasonCode::INVALID_CLIENT_ORDER_ID, sink);
                return;
            }

            const matching::InboundOrder order{
                .action = request.action,
                .symbol = request.symbol,
                .order_id = protocol::make_order_id(participant, request.client_order_id),
                .side = request.side,
                .price = request.price,
                .qty = request.qty,
                .order_type = request.order_type,
                .timestamp = timestamp,
                .participant_id = participant,
                .trigger_price = request.trigger_price,
                .display_qty = request.display_qty,
            };

            // the span points into the book's event buffer and is only valid
            // until the next process_order, so everything is translated right now
            const std::span<const matching::Event> events = engine_->process_order(order);

            for (const matching::Event& event : events) {
                translate(event, request.symbol, sink);
            }

            publish_book_if_changed(request.symbol, sink);
        }

        [[nodiscard]] const Engine& engine() const noexcept { return *engine_; }

    private:

        template <typename Sink>
        void reject(core::ParticipantId participant, const protocol::ClientRequest& request,
            matching::ReasonCode reason, Sink& sink)
        {
            protocol::ExecutionReport report{};
            report.type = protocol::ExecType::REJECTED;
            report.reason = reason;
            report.symbol = request.symbol;
            report.client_order_id = request.client_order_id;
            report.event_time_ns = request_time_ns_;
            sink.send(participant, protocol::ServerMessage::make(report));
        }

        // one engine event -> private report(s) for the owner(s) + maybe public data.
        // the owner comes from the order id itself, so a stop order belonging to
        // someone else that this request triggered is still routed correctly
        template <typename Sink>
        void translate(const matching::Event& event, const core::Symbol& symbol, Sink& sink) {
            using matching::EventType;
            using protocol::ExecType;

            protocol::ExecutionReport report{};
            report.symbol = symbol;
            report.event_time_ns = request_time_ns_;
            core::OrderId order_id = 0;

            switch (event.type) {
            case EventType::ORDER_ACCEPTED: {
                const auto& value = event.payload.order_accepted;
                order_id = value.order_id;
                report.type = ExecType::ACCEPTED;
                report.side = value.side;
                report.price = value.price;
                report.qty = value.qty;
                report.sequence_number = value.sequence_number;
                break;
            }
            case EventType::ORDER_RESTED: {
                const auto& value = event.payload.order_rested;
                order_id = value.order_id;
                report.type = ExecType::RESTED;
                report.side = value.side;
                report.price = value.price;
                report.qty = value.qty;
                report.sequence_number = value.sequence_number;
                break;
            }
            case EventType::ORDER_REJECTED: {
                const auto& value = event.payload.order_rejected;
                order_id = value.order_id;
                report.type = ExecType::REJECTED;
                report.reason = value.reason_code;
                report.sequence_number = value.sequence_number;
                break;
            }
            case EventType::ORDER_REDUCED: {
                const auto& value = event.payload.order_reduced;
                order_id = value.order_id;
                report.type = ExecType::REDUCED;
                report.qty = value.new_qty;
                report.sequence_number = value.sequence_number;
                break;
            }
            case EventType::ORDER_PARTIALLY_FILLED: {
                const auto& value = event.payload.order_partially_filled;
                order_id = value.order_id;
                report.type = ExecType::PARTIALLY_FILLED;
                report.price = value.price;
                report.qty = value.filled_qty;
                report.leaves_qty = value.remaining_qty;
                report.sequence_number = value.sequence_number;
                break;
            }
            case EventType::ORDER_FILLED: {
                const auto& value = event.payload.order_filled;
                order_id = value.order_id;
                report.type = ExecType::FILLED;
                report.price = value.price;
                report.qty = value.filled_qty;
                report.sequence_number = value.sequence_number;
                break;
            }
            case EventType::ORDER_CANCELED: {
                const auto& value = event.payload.order_canceled;
                order_id = value.order_id;
                report.type = ExecType::CANCELED;
                report.qty = value.canceled_qty;
                report.sequence_number = value.sequence_number;
                break;
            }
            case EventType::TRADE: {
                publish_trade(event.payload.trade, sink);
                return;
            }
            }

            report.client_order_id = protocol::client_order_id_of(order_id);
            sink.send(protocol::owner_of(order_id), protocol::ServerMessage::make(report));
        }

        // a trade has two private sides (a FILL for buyer and seller)
        // and one public side (the anonymous print everyone sees)
        template <typename Sink>
        void publish_trade(const matching::Trade& trade, Sink& sink) {
            // sinks that want the complete trade (both orders, aggressor side, match and
            // sequence numbers) provide trade(); the Kafka publisher builds trade.v1 from it
            if constexpr (requires { sink.trade(trade, request_time_ns_); }) {
                sink.trade(trade, request_time_ns_);
            }

            protocol::ExecutionReport fill{};
            fill.type = protocol::ExecType::FILL;
            fill.symbol = trade.symbol;
            fill.price = trade.price;
            fill.qty = trade.qty;
            fill.sequence_number = trade.sequence_number;
            fill.event_time_ns = request_time_ns_;

            fill.side = core::Side::BUY;
            fill.client_order_id = protocol::client_order_id_of(trade.buy_order_id);
            sink.send(protocol::owner_of(trade.buy_order_id), protocol::ServerMessage::make(fill));

            fill.side = core::Side::SELL;
            fill.client_order_id = protocol::client_order_id_of(trade.sell_order_id);
            sink.send(protocol::owner_of(trade.sell_order_id), protocol::ServerMessage::make(fill));

            sink.broadcast(protocol::ServerMessage::make(protocol::TradeUpdate{
                .symbol = trade.symbol,
                .sequence_number = ++market_data_sequence_,
                .match_id = trade.match_id,
                .price = trade.price,
                .qty = trade.qty,
                .event_time_ns = request_time_ns_,
                }));
        }

        // only publish the top of the book when it actually changed:
        // a cancel deep in the book or a reject costs subscribers nothing
        template <typename Sink>
        void publish_book_if_changed(const core::Symbol& symbol, Sink& sink) {
            const auto* book = engine_->find_book(symbol);

            protocol::BookUpdate update{};
            update.symbol = symbol;
            update.bid_count = static_cast<std::uint8_t>(book->depth(core::Side::BUY, update.bids));
            update.ask_count = static_cast<std::uint8_t>(book->depth(core::Side::SELL, update.asks));

            protocol::BookUpdate& last = last_books_[symbol];
            if (same_levels(update, last)) {
                return;
            }

            update.sequence_number = ++market_data_sequence_;
            update.event_time_ns = request_time_ns_;
            last = update;
            sink.broadcast(protocol::ServerMessage::make(update));
        }

        [[nodiscard]] static bool same_levels(const protocol::BookUpdate& lhs,
            const protocol::BookUpdate& rhs) noexcept
        {
            if (lhs.bid_count != rhs.bid_count || lhs.ask_count != rhs.ask_count)
                return false;

            for (std::size_t index = 0; index < lhs.bid_count; ++index) {
                if (lhs.bids[index].price != rhs.bids[index].price ||
                    lhs.bids[index].qty != rhs.bids[index].qty)
                    return false;
            }
            for (std::size_t index = 0; index < lhs.ask_count; ++index) {
                if (lhs.asks[index].price != rhs.asks[index].price ||
                    lhs.asks[index].qty != rhs.asks[index].qty)
                    return false;
            }
            return true;
        }

        std::unique_ptr<Engine> engine_;
        core::LogicalClock clock_{};
        // wall time of the request currently being handled (set at the top of handle)
        core::Timestamp request_time_ns_{ 0 };
        core::SequenceNumber market_data_sequence_{ 0 };
        std::map<core::Symbol, protocol::BookUpdate, core::SymbolLess> last_books_;
    };
}
