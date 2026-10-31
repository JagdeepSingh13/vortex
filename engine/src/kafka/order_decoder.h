#pragma once

#include <cstdint>
#include <cstring>
#include <optional>
#include <span>
#include <string_view>
#include <vector>

#include <simdjson.h>

#include "core/types.h"
#include "kafka/messages.h"
#include "kafka/symbol_config.h"

// order.v1 JSON -> InboundRequest, on the consumer thread (never the matching thread).
//
// one pass over the object with simdjson's on-demand API: fields are read in whatever
// order they arrive and converted straight into engine units. it checks what the engine
// needs (types, presence, ranges, known symbol); full schema validation is the
// producer's job (contract tests, simulator test). integers are strict: simdjson's
// get_int64() rejects 18990.0, matching the "never floats" rule.
// no exceptions: every simdjson call returns an error code that is checked.

namespace exchange::kafka {

    enum class DecodeError : std::uint8_t {
        NONE,
        BAD_JSON,        // not valid JSON or not an object
        WRONG_VERSION,   // schema_version != 1
        MISSING_FIELD,   // a field required for this action is absent
        BAD_VALUE,       // wrong type, unknown enum value, out of range
        UNKNOWN_SYMBOL,  // not in config/symbols.yaml
    };

    inline constexpr const char* to_string(DecodeError error) noexcept {
        switch (error) {
        case DecodeError::NONE: return "none";
        case DecodeError::BAD_JSON: return "bad_json";
        case DecodeError::WRONG_VERSION: return "wrong_version";
        case DecodeError::MISSING_FIELD: return "missing_field";
        case DecodeError::BAD_VALUE: return "bad_value";
        case DecodeError::UNKNOWN_SYMBOL: return "unknown_symbol";
        }
        return "?";
    }

    class OrderDecoder {
    public:
        explicit OrderDecoder(std::span<const SymbolConfig> symbols)
            : symbols_(symbols.begin(), symbols.end())
        {
            buffer_.resize(4096);
        }

        // received_ns is stamped by the caller; on failure `error` says why
        [[nodiscard]] std::optional<InboundRequest>
            decode(std::string_view json, core::Timestamp received_ns, DecodeError& error)
        {
            error = parse(json);
            if (error != DecodeError::NONE) {
                return std::nullopt;
            }
            error = check_and_convert();
            if (error != DecodeError::NONE) {
                return std::nullopt;
            }
            out_.received_ns = received_ns;
            return out_;
        }

    private:
        // which fields were present (bit per field)
        enum Field : std::uint32_t {
            VERSION = 1u << 0, ORDER_ID = 1u << 1, SYMBOL = 1u << 2, ACTION = 1u << 3,
            PARTICIPANT = 1u << 4, CLIENT_TIME = 1u << 5, SIDE = 1u << 6, TYPE = 1u << 7,
            PRICE = 1u << 8, QTY = 1u << 9, TRIGGER = 1u << 10, DISPLAY = 1u << 11,
        };
        static constexpr std::uint32_t ALWAYS = VERSION | ORDER_ID | SYMBOL | ACTION | PARTICIPANT | CLIENT_TIME;

        DecodeError parse(std::string_view json) {
            // simdjson reads up to SIMDJSON_PADDING bytes past the end, so the message is
            // copied into a buffer that is reused and only grows
            if (buffer_.size() < json.size() + simdjson::SIMDJSON_PADDING) {
                buffer_.resize(json.size() + simdjson::SIMDJSON_PADDING);
            }
            std::memcpy(buffer_.data(), json.data(), json.size());
            const simdjson::padded_string_view padded(buffer_.data(), json.size(), buffer_.size());

            seen_ = 0;
            symbol_ = nullptr;
            out_ = InboundRequest{};
            price_ticks_ = trigger_ticks_ = 0;
            qty_lots_ = display_lots_ = 0;

            simdjson::ondemand::document document;
            simdjson::ondemand::object object;
            if (parser_.iterate(padded).get(document) || document.get_object().get(object)) {
                return DecodeError::BAD_JSON;
            }

            for (auto field : object) {
                std::string_view key;
                if (field.unescaped_key().get(key)) {
                    return DecodeError::BAD_JSON;
                }
                const DecodeError error = read_field(key, field.value());
                if (error != DecodeError::NONE) {
                    return error;
                }
            }
            return DecodeError::NONE;
        }

        DecodeError read_field(std::string_view key, simdjson::simdjson_result<simdjson::ondemand::value> value) {
            std::int64_t number = 0;
            std::string_view text;

            if (key == "schema_version") {
                if (value.get_int64().get(number)) return DecodeError::BAD_VALUE;
                if (number != 1) return DecodeError::WRONG_VERSION;
                seen_ |= VERSION;
            }
            else if (key == "order_id") {
                if (value.get_int64().get(number) || number < 1 || number > static_cast<std::int64_t>(protocol::MAX_CLIENT_ORDER_ID))
                    return DecodeError::BAD_VALUE;
                out_.request.client_order_id = static_cast<protocol::ClientOrderId>(number);
                seen_ |= ORDER_ID;
            }
            else if (key == "participant_id") {
                if (value.get_int64().get(number) || number < 1 || number > static_cast<std::int64_t>(protocol::MAX_PARTICIPANT_ID))
                    return DecodeError::BAD_VALUE;
                out_.participant = static_cast<core::ParticipantId>(number);
                seen_ |= PARTICIPANT;
            }
            else if (key == "client_time_ns") {
                std::uint64_t time = 0;
                if (value.get_uint64().get(time)) return DecodeError::BAD_VALUE;
                out_.client_time_ns = time;
                seen_ |= CLIENT_TIME;
            }
            else if (key == "symbol") {
                if (value.get_string().get(text)) return DecodeError::BAD_VALUE;
                for (const SymbolConfig& symbol : symbols_) {
                    if (symbol.name == text) {
                        symbol_ = &symbol;
                    }
                }
                if (symbol_ == nullptr) return DecodeError::UNKNOWN_SYMBOL;
                out_.request.symbol = symbol_->symbol;
                seen_ |= SYMBOL;
            }
            else if (key == "action") {
                if (value.get_string().get(text)) return DecodeError::BAD_VALUE;
                if (text == "ADD") out_.request.action = matching::RequestAction::ADD;
                else if (text == "CANCEL") out_.request.action = matching::RequestAction::CANCEL;
                else if (text == "MODIFY") out_.request.action = matching::RequestAction::MODIFY;
                else return DecodeError::BAD_VALUE;
                seen_ |= ACTION;
            }
            else if (key == "side") {
                if (value.get_string().get(text)) return DecodeError::BAD_VALUE;
                if (text == "BUY") out_.request.side = core::Side::BUY;
                else if (text == "SELL") out_.request.side = core::Side::SELL;
                else return DecodeError::BAD_VALUE;
                seen_ |= SIDE;
            }
            else if (key == "order_type") {
                if (value.get_string().get(text)) return DecodeError::BAD_VALUE;
                const auto type = order_type_of(text);
                if (!type) return DecodeError::BAD_VALUE;
                out_.request.order_type = *type;
                seen_ |= TYPE;
            }
            else if (key == "price_ticks") {
                if (value.get_int64().get(price_ticks_) || price_ticks_ < 1) return DecodeError::BAD_VALUE;
                seen_ |= PRICE;
            }
            else if (key == "trigger_price_ticks") {
                if (value.get_int64().get(trigger_ticks_) || trigger_ticks_ < 1) return DecodeError::BAD_VALUE;
                seen_ |= TRIGGER;
            }
            else if (key == "qty_lots") {
                if (value.get_int64().get(qty_lots_) || qty_lots_ < 1) return DecodeError::BAD_VALUE;
                seen_ |= QTY;
            }
            else if (key == "display_qty_lots") {
                if (value.get_int64().get(display_lots_) || display_lots_ < 1) return DecodeError::BAD_VALUE;
                seen_ |= DISPLAY;
            }
            // unknown keys are ignored: the schema forbids them, producers are tested
            return DecodeError::NONE;
        }

        // presence rules per action (the engine enforces the per-order-type ones and
        // rejects with a reason code), then ticks/lots -> engine units
        DecodeError check_and_convert() {
            if ((seen_ & ALWAYS) != ALWAYS) return DecodeError::MISSING_FIELD;

            const auto action = out_.request.action;
            if (action == matching::RequestAction::ADD && (seen_ & (SIDE | TYPE | QTY)) != (SIDE | TYPE | QTY))
                return DecodeError::MISSING_FIELD;
            if (action == matching::RequestAction::MODIFY && (seen_ & (PRICE | QTY)) != (PRICE | QTY))
                return DecodeError::MISSING_FIELD;

            if (!to_engine_price(price_ticks_, out_.request.price) ||
                !to_engine_price(trigger_ticks_, out_.request.trigger_price) ||
                !to_engine_qty(qty_lots_, out_.request.qty) ||
                !to_engine_qty(display_lots_, out_.request.display_qty)) {
                return DecodeError::BAD_VALUE;
            }
            return DecodeError::NONE;
        }

        bool to_engine_price(std::int64_t ticks, core::Price& out) const {
            return !__builtin_mul_overflow(ticks, symbol_->price_per_tick, &out);
        }

        bool to_engine_qty(std::int64_t lots, core::Quantity& out) const {
            std::int64_t qty = 0;
            if (__builtin_mul_overflow(lots, static_cast<std::int64_t>(symbol_->lot_size), &qty) ||
                qty > static_cast<std::int64_t>(UINT32_MAX)) {
                return false;
            }
            out = static_cast<core::Quantity>(qty);
            return true;
        }

        static std::optional<core::OrderType> order_type_of(std::string_view text) {
            using core::OrderType;
            if (text == "LIMIT") return OrderType::LIMIT;
            if (text == "MARKET") return OrderType::MARKET;
            if (text == "IOC") return OrderType::IOC;
            if (text == "FOK") return OrderType::FOK;
            if (text == "GTC") return OrderType::GTC;
            if (text == "STOP") return OrderType::STOP;
            if (text == "STOP_LIMIT") return OrderType::STOP_LIMIT;
            if (text == "ICEBERG") return OrderType::ICEBERG;
            if (text == "POST_ONLY") return OrderType::POST_ONLY;
            return std::nullopt;
        }

        std::vector<SymbolConfig> symbols_;
        std::vector<char> buffer_;
        simdjson::ondemand::parser parser_;

        // state of the message being decoded
        InboundRequest out_{};
        const SymbolConfig* symbol_{ nullptr };
        std::uint32_t seen_{ 0 };
        std::int64_t price_ticks_{ 0 };
        std::int64_t trigger_ticks_{ 0 };
        std::int64_t qty_lots_{ 0 };
        std::int64_t display_lots_{ 0 };
    };
}
