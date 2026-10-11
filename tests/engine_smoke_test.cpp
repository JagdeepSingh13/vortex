#include <array>
#include <cstdio>
#include <cstdlib>
#include <span>
#include <vector>

#include "core/types.h"
#include "matching/order_book.h"

// no exceptions in this project, so a tiny CHECK macro instead of a test framework

namespace {
    int failures = 0;

#define CHECK(condition)                                                   \
    do {                                                                   \
        if (!(condition)) {                                                \
            std::printf("  FAILED %s:%d  %s\n", __FILE__, __LINE__, #condition); \
            ++failures;                                                    \
        }                                                                  \
    } while (0)

    using namespace exchange;
    using matching::Event;
    using matching::EventType;
    using matching::ReasonCode;

    // small capacities so the recycling tests actually hit the limits
    using Book = matching::OrderBook<1024, 256, 1024, 64>;

    const core::Symbol SYMBOL = core::make_symbol("TEST");

    std::vector<EventType> types_of(std::span<const Event> events) {
        std::vector<EventType> types;
        for (const Event& event : events) {
            types.push_back(event.type);
        }
        return types;
    }

    std::size_t count_of(std::span<const Event> events, EventType type) {
        std::size_t count = 0;
        for (const Event& event : events) {
            if (event.type == type)
                ++count;
        }
        return count;
    }

    void test_resting_orders_do_not_cross() {
        std::printf("resting orders do not cross\n");
        Book book(SYMBOL);

        auto events = book.add_order(1, core::Side::BUY, 100, 10, core::OrderType::LIMIT, 1, 1);
        CHECK((types_of(events) == std::vector{ EventType::ORDER_ACCEPTED, EventType::ORDER_RESTED }));

        events = book.add_order(2, core::Side::SELL, 101, 5, core::OrderType::LIMIT, 2, 2);
        CHECK(count_of(events, EventType::TRADE) == 0);

        CHECK(book.best_bid().has_value() && book.best_bid()->price == 100);
        CHECK(book.best_ask().has_value() && book.best_ask()->price == 101);
        CHECK(book.best_ask()->qty == 5);
    }

    void test_crossing_order_trades_at_passive_price() {
        std::printf("crossing order trades at passive price\n");
        Book book(SYMBOL);

        (void)book.add_order(1, core::Side::SELL, 101, 5, core::OrderType::LIMIT, 1, 1);
        (void)book.add_order(2, core::Side::SELL, 102, 5, core::OrderType::LIMIT, 2, 1);

        // buy 7 @ 105 -> 5 @ 101 then 2 @ 102
        auto events = book.add_order(3, core::Side::BUY, 105, 7, core::OrderType::LIMIT, 3, 2);
        CHECK(count_of(events, EventType::TRADE) == 2);

        std::vector<core::Price> prices;
        for (const Event& event : events) {
            if (event.type == EventType::TRADE) {
                prices.push_back(event.payload.trade.price);
                CHECK(event.payload.trade.buy_order_id == 3);
            }
        }
        CHECK((prices == std::vector<core::Price>{ 101, 102 }));

        // aggressor is fully filled -> must be ORDER_FILLED (this was inverted before)
        bool aggressor_filled = false;
        for (const Event& event : events) {
            if (event.type == EventType::ORDER_FILLED && event.payload.order_filled.order_id == 3) {
                aggressor_filled = event.payload.order_filled.filled_qty == 7;
            }
        }
        CHECK(aggressor_filled);

        CHECK(book.best_ask().has_value() && book.best_ask()->price == 102);
        CHECK(book.best_ask()->qty == 3);
        CHECK(!book.best_bid().has_value());
        CHECK(book.last_trade_price() == 102);
    }

    void test_partial_fill_rests_remainder() {
        std::printf("partial fill rests remainder\n");
        Book book(SYMBOL);

        (void)book.add_order(1, core::Side::SELL, 100, 4, core::OrderType::LIMIT, 1, 1);
        auto events = book.add_order(2, core::Side::BUY, 100, 10, core::OrderType::LIMIT, 2, 2);

        CHECK(count_of(events, EventType::ORDER_PARTIALLY_FILLED) == 1);
        CHECK(count_of(events, EventType::ORDER_RESTED) == 1);
        CHECK(book.best_bid().has_value() && book.best_bid()->qty == 6);
        CHECK(!book.best_ask().has_value());
    }

    void test_cancel_and_modify() {
        std::printf("cancel and modify\n");
        Book book(SYMBOL);

        (void)book.add_order(1, core::Side::BUY, 100, 10, core::OrderType::LIMIT, 1, 1);
        (void)book.add_order(2, core::Side::BUY, 99, 10, core::OrderType::LIMIT, 2, 1);

        auto events = book.cancel_order(1, 3);
        CHECK((types_of(events) == std::vector{ EventType::ORDER_CANCELED }));
        CHECK(book.best_bid()->price == 99);

        // canceling twice -> rejected
        events = book.cancel_order(1, 4);
        CHECK(events.size() == 1 && events[0].type == EventType::ORDER_REJECTED);
        CHECK(events[0].payload.order_rejected.reason_code == ReasonCode::ORDER_NOT_FOUND);

        // same price, smaller qty -> reduce in place
        events = book.modify_order(2, 4, 99, 5);
        CHECK((types_of(events) == std::vector{ EventType::ORDER_REDUCED }));
        CHECK(book.best_bid()->qty == 4);

        // new price -> cancel + re-insert under the same id
        events = book.modify_order(2, 4, 98, 6);
        CHECK((types_of(events) == std::vector{ EventType::ORDER_CANCELED,
                                                EventType::ORDER_ACCEPTED,
                                                EventType::ORDER_RESTED }));
        CHECK(book.best_bid()->price == 98);
    }

    void test_ioc_and_market() {
        std::printf("ioc and market orders\n");
        Book book(SYMBOL);

        // market into empty book -> reject
        auto events = book.add_order(1, core::Side::BUY, 0, 5, core::OrderType::MARKET, 1, 1);
        CHECK(events.size() == 1 && events[0].payload.order_rejected.reason_code == ReasonCode::BOOK_EMPTY);

        (void)book.add_order(2, core::Side::SELL, 100, 3, core::OrderType::LIMIT, 2, 2);

        // IOC for 5, only 3 available -> fill 3, cancel 2, nothing rests
        events = book.add_order(3, core::Side::BUY, 100, 5, core::OrderType::IOC, 3, 1);
        CHECK(count_of(events, EventType::TRADE) == 1);
        CHECK(count_of(events, EventType::ORDER_CANCELED) == 1);
        CHECK(count_of(events, EventType::ORDER_RESTED) == 0);
        CHECK(!book.best_bid().has_value());
        CHECK(!book.best_ask().has_value());
    }

    void test_stop_order_triggers() {
        std::printf("stop order triggers on trade\n");
        Book book(SYMBOL);

        (void)book.add_order(1, core::Side::SELL, 100, 5, core::OrderType::LIMIT, 1, 1);
        (void)book.add_order(2, core::Side::SELL, 105, 5, core::OrderType::LIMIT, 2, 1);
        // stop buy: becomes a market order once a trade prints at >= 100
        (void)book.add_order(3, core::Side::BUY, 0, 2, core::OrderType::STOP, 3, 2, 100);

        auto events = book.add_order(4, core::Side::BUY, 100, 5, core::OrderType::LIMIT, 4, 3);
        // first trade: order 4 vs 1 at 100, then the stop wakes up and buys 2 at 105
        CHECK(count_of(events, EventType::TRADE) == 2);
        CHECK(book.best_ask().has_value() && book.best_ask()->qty == 3);
    }

    void test_depth() {
        std::printf("depth snapshot\n");
        Book book(SYMBOL);

        (void)book.add_order(1, core::Side::BUY, 100, 1, core::OrderType::LIMIT, 1, 1);
        (void)book.add_order(2, core::Side::BUY, 102, 2, core::OrderType::LIMIT, 2, 1);
        (void)book.add_order(3, core::Side::BUY, 101, 3, core::OrderType::LIMIT, 3, 1);
        (void)book.add_order(4, core::Side::BUY, 101, 4, core::OrderType::LIMIT, 4, 1);

        std::array<core::PriceLevelView, 2> levels{};
        const std::size_t count = book.depth(core::Side::BUY, levels);
        CHECK(count == 2);
        CHECK(levels[0].price == 102 && levels[0].qty == 2);
        CHECK(levels[1].price == 101 && levels[1].qty == 7);
    }

    // a market maker cancels/replaces all day. with 1024 order slots and 256 level
    // slots, this only survives if orders, index nodes and levels are all recycled
    void test_long_running_churn() {
        std::printf("long running churn reuses memory\n");
        Book book(SYMBOL);

        core::OrderId next_id = 1;
        bool ok = true;
        for (int round = 0; round < 50'000 && ok; ++round) {
            // drifting prices -> keeps creating brand new levels
            const core::Price price = 1'000 + (round % 5'000);
            const core::OrderId id = next_id++;

            auto events = book.add_order(id, core::Side::BUY, price, 10, core::OrderType::LIMIT,
                static_cast<core::Timestamp>(round), 1);
            ok = ok && events.size() == 2 && events[1].type == EventType::ORDER_RESTED;

            events = book.cancel_order(id, static_cast<core::Timestamp>(round));
            ok = ok && events.size() == 1 && events[0].type == EventType::ORDER_CANCELED;

            // and some trading so fills also recycle
            if (round % 3 == 0) {
                (void)book.add_order(next_id++, core::Side::SELL, price, 5,
                    core::OrderType::LIMIT, static_cast<core::Timestamp>(round), 2);
                events = book.add_order(next_id++, core::Side::BUY, price, 5,
                    core::OrderType::LIMIT, static_cast<core::Timestamp>(round), 3);
                ok = ok && count_of(events, EventType::TRADE) == 1;
            }
        }
        CHECK(ok);
        CHECK(book.active_order_count() == 0);
        CHECK(!book.best_bid().has_value());
    }
}

int main() {
    test_resting_orders_do_not_cross();
    test_crossing_order_trades_at_passive_price();
    test_partial_fill_rests_remainder();
    test_cancel_and_modify();
    test_ioc_and_market();
    test_stop_order_triggers();
    test_depth();
    test_long_running_churn();

    if (failures == 0) {
        std::printf("all engine tests passed\n");
        return EXIT_SUCCESS;
    }

    std::printf("%d check(s) failed\n", failures);
    return EXIT_FAILURE;
}
