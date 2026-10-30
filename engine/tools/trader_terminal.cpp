#include <algorithm>
#include <atomic>
#include <cctype>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <deque>
#include <format>
#include <iostream>
#include <map>
#include <mutex>
#include <optional>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "client/market_view.h"
#include "client/tcp_client.h"
#include "core/types.h"
#include "exchange/protocol.h"
#include "net/socket.h"

// manual trading over TCP.
//
// usage: trader_terminal.exe [host] [port] [name]     (default 127.0.0.1 9000 trader)
//
// stdin is read on its own thread (getline blocks), lines are handed to the
// main thread, which also polls the network. so reports print while you type.

using namespace exchange;

namespace {

    constexpr const char* HELP = R"(commands:
  buy  SYM QTY [PRICE] [TYPE]     TYPE: limit (default) ioc fok post gtc
  sell SYM QTY [PRICE] [TYPE]     no PRICE -> market order
  stop buy|sell SYM QTY TRIGGER [LIMIT]
  iceberg buy|sell SYM QTY PRICE VISIBLE
  cancel ID
  modify ID QTY PRICE
  book [SYM]      orders      pos      help      quit
)";

    // ---------------- stdin reader thread ----------------

    class LineReader {
    public:
        LineReader() {
            std::thread([this] {
                std::string line;
                while (std::getline(std::cin, line)) {
                    push(line);
                }
                push("quit");  // stdin closed (e.g. piped script finished)
                }).detach();   // getline can't be interrupted; process exit ends it
        }

        std::optional<std::string> next() {
            std::lock_guard lock(mutex_);
            if (lines_.empty())
                return std::nullopt;
            std::string line = std::move(lines_.front());
            lines_.pop_front();
            return line;
        }

    private:
        void push(std::string line) {
            std::lock_guard lock(mutex_);
            lines_.push_back(std::move(line));
        }

        std::mutex mutex_;
        std::deque<std::string> lines_;
    };

    // ---------------- parsing helpers ----------------

    std::vector<std::string> split(const std::string& line) {
        std::istringstream stream(line);
        std::vector<std::string> words;
        std::string word;
        while (stream >> word) {
            words.push_back(word);
        }
        return words;
    }

    std::string lower(std::string text) {
        std::transform(text.begin(), text.end(), text.begin(),
            [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
        return text;
    }

    core::Symbol parse_symbol(std::string text) {
        std::transform(text.begin(), text.end(), text.begin(),
            [](unsigned char c) { return static_cast<char>(std::toupper(c)); });
        return core::make_symbol(text);
    }

    std::optional<std::uint64_t> parse_number(const std::string& text) {
        char* end = nullptr;
        const unsigned long long value = std::strtoull(text.c_str(), &end, 10);
        if (end == text.c_str() || *end != '\0' || value == 0)
            return std::nullopt;
        return value;
    }

    std::optional<core::Price> parse_price(const std::string& text) {
        char* end = nullptr;
        const double value = std::strtod(text.c_str(), &end);
        if (end == text.c_str() || *end != '\0' || value <= 0.0)
            return std::nullopt;
        return core::to_price(value);
    }

    std::optional<core::Side> parse_side(const std::string& text) {
        const std::string word = lower(text);
        if (word == "buy" || word == "b")
            return core::Side::BUY;
        if (word == "sell" || word == "s")
            return core::Side::SELL;
        return std::nullopt;
    }

    std::optional<core::OrderType> parse_type(const std::string& text) {
        const std::string word = lower(text);
        if (word == "limit") return core::OrderType::LIMIT;
        if (word == "ioc") return core::OrderType::IOC;
        if (word == "fok") return core::OrderType::FOK;
        if (word == "post") return core::OrderType::POST_ONLY;
        if (word == "gtc") return core::OrderType::GTC;
        if (word == "market") return core::OrderType::MARKET;
        return std::nullopt;
    }

    const char* side_text(core::Side side) {
        return side == core::Side::BUY ? "BUY" : "SELL";
    }

    // ---------------- the terminal ----------------

    class Terminal {
    public:

        explicit Terminal(client::TcpClient& client) : client_(client) {
            for (const core::Symbol& symbol : client_.symbols()) {
                market_.track(symbol);
            }
        }

        void on_message(const protocol::ServerMessage& message) {
            if (market_.on_message(message))
                return;
            on_execution(message.payload.execution);
        }

        // returns false on quit
        bool on_command(const std::string& line) {
            const std::vector<std::string> words = split(line);
            if (words.empty())
                return true;

            const std::string command = lower(words[0]);
            if (command == "quit" || command == "exit")
                return false;

            if (command == "help") std::fputs(HELP, stdout);
            else if (command == "buy" || command == "sell") place(words);
            else if (command == "stop") place_stop(words);
            else if (command == "iceberg") place_iceberg(words);
            else if (command == "cancel") cancel(words);
            else if (command == "modify") modify(words);
            else if (command == "book") show_book(words);
            else if (command == "orders") show_orders();
            else if (command == "pos") show_positions();
            else std::puts("unknown command, try: help");

            std::fflush(stdout);
            return true;
        }

    private:

        struct OrderInfo {
            core::Symbol symbol{};
            core::Side side{ core::Side::BUY };
            core::OrderType type{ core::OrderType::LIMIT };
            core::Price price{ 0 };
            core::Quantity qty{ 0 };
            core::Quantity filled{ 0 };
            bool open{ true };
        };

        struct Position {
            std::int64_t qty{ 0 };
            // money spent (negative) / received (positive), price units * qty
            std::int64_t cash{ 0 };
        };

        // buy SYM QTY [PRICE] [TYPE]
        void place(const std::vector<std::string>& words) {
            if (words.size() < 3) {
                std::puts("usage: buy|sell SYM QTY [PRICE] [TYPE]");
                return;
            }
            const core::Side side = *parse_side(words[0]);
            const core::Symbol symbol = parse_symbol(words[1]);
            const auto qty = parse_number(words[2]);
            if (!qty) {
                std::puts("bad quantity");
                return;
            }

            std::optional<core::Price> price;
            core::OrderType type = core::OrderType::MARKET;
            std::size_t next = 3;
            if (words.size() > next && parse_price(words[next])) {
                price = parse_price(words[next]);
                type = core::OrderType::LIMIT;
                ++next;
            }
            if (words.size() > next) {
                const auto parsed = parse_type(words[next]);
                if (!parsed) {
                    std::printf("unknown order type '%s'\n", words[next].c_str());
                    return;
                }
                type = *parsed;
            }
            if (type != core::OrderType::MARKET && !price) {
                std::puts("this order type needs a price");
                return;
            }

            submit(symbol, side, type, price.value_or(0), static_cast<core::Quantity>(*qty));
        }

        // stop buy|sell SYM QTY TRIGGER [LIMIT]
        void place_stop(const std::vector<std::string>& words) {
            if (words.size() < 5 || !parse_side(words[1]) || !parse_number(words[3]) || !parse_price(words[4])) {
                std::puts("usage: stop buy|sell SYM QTY TRIGGER [LIMIT]");
                return;
            }
            const auto limit = words.size() > 5 ? parse_price(words[5]) : std::nullopt;
            submit(parse_symbol(words[2]), *parse_side(words[1]),
                limit ? core::OrderType::STOP_LIMIT : core::OrderType::STOP,
                limit.value_or(0), static_cast<core::Quantity>(*parse_number(words[3])),
                *parse_price(words[4]));
        }

        // iceberg buy|sell SYM QTY PRICE VISIBLE
        void place_iceberg(const std::vector<std::string>& words) {
            if (words.size() < 6 || !parse_side(words[1]) || !parse_number(words[3]) ||
                !parse_price(words[4]) || !parse_number(words[5])) {
                std::puts("usage: iceberg buy|sell SYM QTY PRICE VISIBLE");
                return;
            }
            submit(parse_symbol(words[2]), *parse_side(words[1]), core::OrderType::ICEBERG,
                *parse_price(words[4]), static_cast<core::Quantity>(*parse_number(words[3])),
                0, static_cast<core::Quantity>(*parse_number(words[5])));
        }

        void submit(const core::Symbol& symbol, core::Side side, core::OrderType type,
            core::Price price, core::Quantity qty, core::Price trigger = 0, core::Quantity display = 0)
        {
            const protocol::ClientOrderId id =
                client_.send_limit(symbol, side, price, qty, type, trigger, display);
            orders_[id] = OrderInfo{ .symbol = symbol, .side = side, .type = type, .price = price, .qty = qty };
            std::printf(">> #%llu sent\n", static_cast<unsigned long long>(id));
        }

        void cancel(const std::vector<std::string>& words) {
            const auto id = words.size() > 1 ? parse_number(words[1]) : std::nullopt;
            const OrderInfo* order = id ? find_open(*id) : nullptr;
            if (order == nullptr) {
                std::puts("usage: cancel ID   (ID of one of your open orders, see: orders)");
                return;
            }
            client_.send_cancel(order->symbol, *id);
        }

        void modify(const std::vector<std::string>& words) {
            // check the word count first: `cond ? parse(...) : nullopt` triggered a GCC 13
            // -Wmaybe-uninitialized false positive at -O3
            if (words.size() < 4) {
                std::puts("usage: modify ID QTY PRICE");
                return;
            }
            const auto id = parse_number(words[1]);
            const auto qty = parse_number(words[2]);
            const auto price = parse_price(words[3]);
            const OrderInfo* order = id ? find_open(*id) : nullptr;
            if (order == nullptr || !qty || !price) {
                std::puts("usage: modify ID QTY PRICE");
                return;
            }
            client_.send_modify(order->symbol, *id, static_cast<core::Quantity>(*qty), *price);
        }

        [[nodiscard]] const OrderInfo* find_open(protocol::ClientOrderId id) const {
            const auto it = orders_.find(id);
            return it != orders_.end() && it->second.open ? &it->second : nullptr;
        }

        void show_book(const std::vector<std::string>& words) {
            if (words.size() < 2) {
                market_.print_all();
                return;
            }
            const core::Symbol symbol = parse_symbol(words[1]);
            if (const client::SymbolView* view = market_.find(symbol)) {
                client::MarketView::print(symbol, *view);
            }
            else {
                std::puts("unknown symbol");
            }
        }

        void show_orders() const {
            bool any = false;
            for (const auto& [id, order] : orders_) {
                if (!order.open)
                    continue;
                any = true;
                std::fputs(std::format("  #{:<5} {:<4} {:<6} {:>6} @ {:>8}   filled {}\n",
                    id, side_text(order.side), core::symbol_view(order.symbol), order.qty,
                    order.price == 0 ? std::string("market") : client::price_text(order.price),
                    order.filled).c_str(), stdout);
            }
            if (!any)
                std::puts("  no open orders");
        }

        void show_positions() const {
            if (positions_.empty()) {
                std::puts("  no trades yet");
                return;
            }
            for (const auto& [symbol, position] : positions_) {
                // mark to the last traded price
                const client::SymbolView* view = market_.find(symbol);
                const core::Price mark = view != nullptr ? view->last_price : 0;
                const double pnl = static_cast<double>(position.cash + position.qty * mark) /
                    static_cast<double>(core::PRICE_SCALE);
                std::fputs(std::format("  {:<6} position {:>+6}   pnl {:>+10.2f}  (marked at {})\n",
                    core::symbol_view(symbol), position.qty, pnl, client::price_text(mark)).c_str(), stdout);
            }
        }

        void on_execution(const protocol::ExecutionReport& report) {
            using protocol::ExecType;
            OrderInfo* order = nullptr;
            if (const auto it = orders_.find(report.client_order_id); it != orders_.end()) {
                order = &it->second;
            }

            std::string detail;
            switch (report.type) {
            case ExecType::ACCEPTED:
                if (order != nullptr) {
                    // also arrives after a reprice: the order starts a new life
                    order->price = report.price;
                    order->qty = report.qty;
                    order->filled = 0;
                    order->open = true;
                }
                detail = std::format("{} {} {} @ {}", side_text(report.side),
                    report.qty, core::symbol_view(report.symbol),
                    report.price == 0 ? std::string("market") : client::price_text(report.price));
                break;
            case ExecType::RESTED:
                detail = std::format("resting {} @ {}", report.qty, client::price_text(report.price));
                break;
            case ExecType::FILL: {
                Position& position = positions_[report.symbol];
                const auto qty = static_cast<std::int64_t>(report.qty);
                const std::int64_t signed_qty = report.side == core::Side::BUY ? qty : -qty;
                position.qty += signed_qty;
                position.cash -= signed_qty * report.price;
                if (order != nullptr) {
                    order->filled += report.qty;
                }
                detail = std::format("{} {} {} @ {}", side_text(report.side), report.qty,
                    core::symbol_view(report.symbol), client::price_text(report.price));
                break;
            }
            case ExecType::PARTIALLY_FILLED:
                detail = std::format("{} open", report.leaves_qty);
                break;
            case ExecType::REDUCED:
                detail = std::format("visible qty now {}", report.qty);
                break;
            case ExecType::FILLED:
            case ExecType::CANCELED:
                if (order != nullptr) {
                    order->open = false;
                }
                if (report.type == ExecType::CANCELED) {
                    detail = std::format("{} canceled", report.qty);
                }
                break;
            case ExecType::REJECTED:
                if (order != nullptr && order->filled == 0 && report.reason != matching::ReasonCode::ORDER_NOT_FOUND) {
                    order->open = false;
                }
                detail = protocol::to_string(report.reason);
                break;
            }

            std::fputs(std::format("<< #{} {} {}\n", report.client_order_id,
                protocol::to_string(report.type), detail).c_str(), stdout);
            std::fflush(stdout);
        }

        client::TcpClient& client_;
        client::MarketView market_;
        std::map<protocol::ClientOrderId, OrderInfo> orders_;
        std::map<core::Symbol, Position, core::SymbolLess> positions_;
    };
}

int main(int argc, char** argv) {
    const std::string host = argc > 1 ? argv[1] : "127.0.0.1";
    const auto port = static_cast<std::uint16_t>(argc > 2 ? std::atoi(argv[2]) : 9000);
    const std::string name = argc > 3 ? argv[3] : "trader";

    net::NetworkInit network;
    client::TcpClient client;
    if (!client.connect(host, port, name)) {
        std::fprintf(stderr, "could not connect to exchange at %s:%u\n", host.c_str(), port);
        return EXIT_FAILURE;
    }

    std::printf("connected as '%s' (participant %u). symbols:", name.c_str(), client.participant_id());
    for (const core::Symbol& symbol : client.symbols()) {
        std::printf(" %.*s", static_cast<int>(core::symbol_view(symbol).size()), core::symbol_view(symbol).data());
    }
    std::printf("\n%s", HELP);
    std::fflush(stdout);

    Terminal terminal(client);
    LineReader input;

    while (client.connected()) {
        client.poll([&](const protocol::ServerMessage& message) { terminal.on_message(message); });

        bool quit = false;
        while (auto line = input.next()) {
            if (!terminal.on_command(*line)) {
                quit = true;
                break;
            }
        }
        if (quit)
            break;

        std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }

    if (!client.connected()) {
        std::puts("disconnected from exchange");
    }
    return EXIT_SUCCESS;
}
