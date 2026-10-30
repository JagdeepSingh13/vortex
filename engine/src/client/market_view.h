#pragma once

#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <format>
#include <map>
#include <string>

#include "core/types.h"
#include "exchange/protocol.h"

// keeps the latest public picture of every symbol from the market data stream
// and prints it as a ladder. used by the server dashboard, the simulation
// and the trader terminal.

namespace exchange::client {

    [[nodiscard]] inline std::string price_text(core::Price price) {
        return std::format("{:.2f}", core::to_double(price));
    }

    struct SymbolView {
        protocol::BookUpdate book{};
        core::Price last_price{ 0 };
        std::uint64_t trades{ 0 };
        std::uint64_t volume{ 0 };
    };

    class MarketView {
    public:

        // returns true if the message was market data
        bool on_message(const protocol::ServerMessage& message) {
            if (message.kind == protocol::MessageKind::BOOK) {
                views_[message.payload.book.symbol].book = message.payload.book;
                return true;
            }
            if (message.kind == protocol::MessageKind::TRADE) {
                SymbolView& view = views_[message.payload.trade.symbol];
                view.last_price = message.payload.trade.price;
                ++view.trades;
                view.volume += message.payload.trade.qty;
                return true;
            }
            return false;
        }

        void track(const core::Symbol& symbol) { (void)views_[symbol]; }

        [[nodiscard]] const SymbolView* find(const core::Symbol& symbol) const {
            const auto it = views_.find(symbol);
            return it == views_.end() ? nullptr : &it->second;
        }

        void print_all() const {
            for (const auto& [symbol, view] : views_) {
                print(symbol, view);
            }
        }

        static void print(const core::Symbol& symbol, const SymbolView& view) {
            std::fputs(std::format("  {:<6} last {:>8}   trades {:>6}   volume {:>8}\n",
                core::symbol_view(symbol), price_text(view.last_price), view.trades, view.volume).c_str(), stdout);
            std::fputs("         bid qty      bid |      ask  ask qty\n", stdout);

            const protocol::BookUpdate& book = view.book;
            const std::size_t rows = std::max(book.bid_count, book.ask_count);
            if (rows == 0) {
                std::fputs("                    (empty book)\n", stdout);
            }
            for (std::size_t row = 0; row < rows; ++row) {
                std::string bid(25, ' ');
                std::string ask;
                if (row < book.bid_count) {
                    bid = std::format("    {:>12} {:>8}", book.bids[row].qty, price_text(book.bids[row].price));
                }
                if (row < book.ask_count) {
                    ask = std::format(" {:>8}  {:<8}", price_text(book.asks[row].price), book.asks[row].qty);
                }
                std::fputs(std::format("{} |{}\n", bid, ask).c_str(), stdout);
            }
        }

    private:
        std::map<core::Symbol, SymbolView, core::SymbolLess> views_;
    };
}
