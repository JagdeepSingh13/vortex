#pragma once

#include <format>
#include <iterator>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "core/types.h"
#include "exchange/protocol.h"
#include "kafka/messages.h"
#include "kafka/symbol_config.h"

// OutboundTrade -> trade.v1 JSON, on the publisher thread.
//
// reverses the consumer's conversion: engine units -> ticks and lots, and the engine's
// packed 64-bit order id -> the producer's (participant_id, order_id) pair. every value
// is an integer written with std::format, so no float ever appears in the output.

namespace exchange::kafka {

    class TradeEncoder {
    public:
        TradeEncoder(std::span<const SymbolConfig> symbols, std::string_view engine_run_id)
            : symbols_(symbols.begin(), symbols.end()), engine_run_id_(engine_run_id)
        {
            json_.reserve(512);
        }

        // returns the JSON (valid until the next call) and sets `symbol` for the message key;
        // empty if the symbol is unknown or a price is not a whole number of ticks
        [[nodiscard]] std::string_view encode(const OutboundTrade& out, std::string_view& symbol) {
            const matching::Trade& trade = out.trade;
            const SymbolConfig* config = find(trade.symbol);
            if (config == nullptr || trade.price % config->price_per_tick != 0 || trade.qty % config->lot_size != 0) {
                return {};
            }
            symbol = config->name;

            json_.clear();
            std::format_to(std::back_inserter(json_),
                R"({{"schema_version":1,"engine_run_id":"{}","match_id":{},"symbol":"{}",)"
                R"("price_ticks":{},"qty_lots":{},)"
                R"("buy_participant_id":{},"buy_order_id":{},"sell_participant_id":{},"sell_order_id":{},)"
                R"("aggressor_side":"{}","sequence_number":{},"event_time_ns":{}}})",
                engine_run_id_, trade.match_id, config->name,
                trade.price / config->price_per_tick, trade.qty / config->lot_size,
                protocol::owner_of(trade.buy_order_id), protocol::client_order_id_of(trade.buy_order_id),
                protocol::owner_of(trade.sell_order_id), protocol::client_order_id_of(trade.sell_order_id),
                trade.aggressor_side == core::Side::BUY ? "BUY" : "SELL",
                trade.sequence_number, out.event_time_ns);
            return json_;
        }

    private:
        const SymbolConfig* find(const core::Symbol& symbol) const {
            for (const SymbolConfig& config : symbols_) {
                if (config.symbol == symbol) {
                    return &config;
                }
            }
            return nullptr;
        }

        std::vector<SymbolConfig> symbols_;
        std::string engine_run_id_;
        std::string json_;
    };
}
