#pragma once

#include <array>
#include <cstdint>
#include <type_traits>

#include "core/types.h"
#include "matching/events.h"
#include "matching/matching_engine.h"

// messages between clients and the exchange core.
// all fixed size + trivially copyable: they go through SPSC queues today
// (the Kafka service turns them into order.v1 / trade.v1 JSON at its edges)

namespace exchange::protocol {

    // id chosen by the client, only unique within that client
    using ClientOrderId = std::uint64_t;

    // exchange order id = [ participant: 24 bits | client order id: 40 bits ]
    // - two clients can both use client id 1 without colliding in the book
    // - the owner of any order can be read straight out of its id
    //   (no extra map needed to route fills back)
    // - a client can only ever cancel/modify its own orders
    inline constexpr unsigned CLIENT_ORDER_ID_BITS = 40;
    inline constexpr ClientOrderId MAX_CLIENT_ORDER_ID =
        (ClientOrderId{ 1 } << CLIENT_ORDER_ID_BITS) - 1U;
    inline constexpr core::ParticipantId MAX_PARTICIPANT_ID = (1U << 24) - 1U;

    [[nodiscard]] constexpr core::OrderId
        make_order_id(core::ParticipantId participant, ClientOrderId client_order_id) noexcept
    {
        return (static_cast<core::OrderId>(participant) << CLIENT_ORDER_ID_BITS) | client_order_id;
    }

    [[nodiscard]] constexpr core::ParticipantId owner_of(core::OrderId order_id) noexcept {
        return static_cast<core::ParticipantId>(order_id >> CLIENT_ORDER_ID_BITS);
    }

    [[nodiscard]] constexpr ClientOrderId client_order_id_of(core::OrderId order_id) noexcept {
        return order_id & MAX_CLIENT_ORDER_ID;
    }

    // ---------------- client -> exchange ----------------

    struct ClientRequest {
        matching::RequestAction action{ matching::RequestAction::ADD };
        core::Side side{ core::Side::BUY };
        core::OrderType order_type{ core::OrderType::LIMIT };
        core::Symbol symbol{};
        // ADD: id of the new order, CANCEL / MODIFY: id of the order to change
        ClientOrderId client_order_id{ 0 };
        core::Price price{ 0 };
        core::Quantity qty{ 0 };
        core::Quantity display_qty{ 0 };
        core::Price trigger_price{ 0 };
    };

    // ---------------- exchange -> one client (private) ----------------

    enum class ExecType : std::uint8_t {
        ACCEPTED,
        RESTED,
        REJECTED,
        REDUCED,
        PARTIALLY_FILLED,
        FILLED,
        CANCELED,
        // one per trade slice, exact price + qty (use this for position / pnl)
        FILL,
    };

    // no default member initializers: these live inside a union
    struct ExecutionReport {
        ExecType type;
        // only meaningful on FILL
        core::Side side;
        matching::ReasonCode reason;
        core::Symbol symbol;
        ClientOrderId client_order_id;
        core::Price price;
        // ACCEPTED: order qty, FILL/FILLED/PARTIALLY_FILLED: filled qty,
        // CANCELED: canceled qty, REDUCED/RESTED: visible qty
        core::Quantity qty;
        // PARTIALLY_FILLED: what is still open
        core::Quantity leaves_qty;
        core::SequenceNumber sequence_number;
        // UTC ns since epoch, read once when the engine handled the request
        core::Timestamp event_time_ns;
    };

    // ---------------- exchange -> everyone (public market data) ----------------

    inline constexpr std::size_t BOOK_DEPTH = 5;

    struct BookUpdate {
        core::Symbol symbol;
        core::SequenceNumber sequence_number;
        std::uint8_t bid_count;
        std::uint8_t ask_count;
        std::array<core::PriceLevelView, BOOK_DEPTH> bids;
        std::array<core::PriceLevelView, BOOK_DEPTH> asks;
        core::Timestamp event_time_ns;
    };

    // anonymous: who traded is private, that it traded is public
    struct TradeUpdate {
        core::Symbol symbol;
        core::SequenceNumber sequence_number;
        core::MatchId match_id;
        core::Price price;
        core::Quantity qty;
        core::Timestamp event_time_ns;
    };

    // ---------------- one envelope for everything going to a client ----------------

    enum class MessageKind : std::uint8_t {
        EXECUTION,
        BOOK,
        TRADE,
    };

    struct ServerMessage {
        MessageKind kind{ MessageKind::EXECUTION };

        // same trick as matching::Event: only one payload is valid at a time
        union Payload {
            ExecutionReport execution;
            BookUpdate book;
            TradeUpdate trade;
        } payload{};

        static ServerMessage make(const ExecutionReport& value) noexcept {
            ServerMessage message{};
            message.kind = MessageKind::EXECUTION;
            message.payload.execution = value;
            return message;
        }

        static ServerMessage make(const BookUpdate& value) noexcept {
            ServerMessage message{};
            message.kind = MessageKind::BOOK;
            message.payload.book = value;
            return message;
        }

        static ServerMessage make(const TradeUpdate& value) noexcept {
            ServerMessage message{};
            message.kind = MessageKind::TRADE;
            message.payload.trade = value;
            return message;
        }
    };

    static_assert(std::is_trivially_copyable_v<ClientRequest>);
    static_assert(std::is_trivially_copyable_v<ExecutionReport>);
    static_assert(std::is_trivially_copyable_v<BookUpdate>);
    static_assert(std::is_trivially_copyable_v<TradeUpdate>);
    static_assert(std::is_trivially_copyable_v<ServerMessage>);

    [[nodiscard]] constexpr const char* to_string(ExecType type) noexcept {
        switch (type) {
        case ExecType::ACCEPTED: return "ACCEPTED";
        case ExecType::RESTED: return "RESTED";
        case ExecType::REJECTED: return "REJECTED";
        case ExecType::REDUCED: return "REDUCED";
        case ExecType::PARTIALLY_FILLED: return "PARTIALLY_FILLED";
        case ExecType::FILLED: return "FILLED";
        case ExecType::CANCELED: return "CANCELED";
        case ExecType::FILL: return "FILL";
        }
        return "?";
    }

    [[nodiscard]] constexpr const char* to_string(matching::ReasonCode reason) noexcept {
        using matching::ReasonCode;
        switch (reason) {
        case ReasonCode::NONE: return "NONE";
        case ReasonCode::ZERO_QUANTITY: return "ZERO_QUANTITY";
        case ReasonCode::NEGATIVE_PRICE: return "NEGATIVE_PRICE";
        case ReasonCode::BOOK_EMPTY: return "BOOK_EMPTY";
        case ReasonCode::DUPLICATE_ORDER_ID: return "DUPLICATE_ORDER_ID";
        case ReasonCode::ORDER_NOT_FOUND: return "ORDER_NOT_FOUND";
        case ReasonCode::ORDER_NOT_ACTIVE: return "ORDER_NOT_ACTIVE";
        case ReasonCode::FOK_INSUFFICIENT_LIQUIDITY: return "FOK_INSUFFICIENT_LIQUIDITY";
        case ReasonCode::POST_ONLY_WOULD_CROSS: return "POST_ONLY_WOULD_CROSS";
        case ReasonCode::INVALID_MODIFICATION: return "INVALID_MODIFICATION";
        case ReasonCode::INVALID_ICEBERG_DISPLAY: return "INVALID_ICEBERG_DISPLAY";
        case ReasonCode::ORDER_POOL_EXHAUSTED: return "ORDER_POOL_EXHAUSTED";
        case ReasonCode::UNKNOWN_SYMBOL: return "UNKNOWN_SYMBOL";
        case ReasonCode::INVALID_CLIENT_ORDER_ID: return "INVALID_CLIENT_ORDER_ID";
        }
        return "?";
    }
}
