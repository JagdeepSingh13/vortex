#include <array>
#include <vector>

#include <gtest/gtest.h>

#include "test_helpers.h"

// the original engine_smoke_test scenarios, ported one-to-one to GoogleTest.
// same orders, same expectations: only the CHECK macro became EXPECT/ASSERT.

namespace exchange::testing {
    namespace {

        TEST(OrderBookBasics, RestingOrdersDoNotCross) {
            Book book(SYMBOL);

            auto events = book.add_order(1, core::Side::BUY, 100, 10, core::OrderType::LIMIT, 1, 1);
            EXPECT_EQ(types_of(events), (std::vector{ EventType::ORDER_ACCEPTED, EventType::ORDER_RESTED }));

            events = book.add_order(2, core::Side::SELL, 101, 5, core::OrderType::LIMIT, 2, 2);
            EXPECT_EQ(count_of(events, EventType::TRADE), 0U);

            ASSERT_TRUE(book.best_bid().has_value());
            EXPECT_EQ(book.best_bid()->price, 100);
            ASSERT_TRUE(book.best_ask().has_value());
            EXPECT_EQ(book.best_ask()->price, 101);
            EXPECT_EQ(book.best_ask()->qty, 5U);
        }

        TEST(OrderBookBasics, CrossingOrderTradesAtPassivePrice) {
            Book book(SYMBOL);

            (void)book.add_order(1, core::Side::SELL, 101, 5, core::OrderType::LIMIT, 1, 1);
            (void)book.add_order(2, core::Side::SELL, 102, 5, core::OrderType::LIMIT, 2, 1);

            // buy 7 @ 105 -> 5 @ 101 then 2 @ 102
            auto events = book.add_order(3, core::Side::BUY, 105, 7, core::OrderType::LIMIT, 3, 2);
            const auto trades = trades_of(events);
            ASSERT_EQ(trades.size(), 2U);
            EXPECT_EQ(trades[0].price, 101);
            EXPECT_EQ(trades[1].price, 102);
            for (const auto& trade : trades) {
                EXPECT_EQ(trade.buy_order_id, 3U);
            }

            // aggressor is fully filled -> must be ORDER_FILLED
            bool aggressor_filled = false;
            for (const Event& event : events) {
                if (event.type == EventType::ORDER_FILLED && event.payload.order_filled.order_id == 3) {
                    aggressor_filled = event.payload.order_filled.filled_qty == 7;
                }
            }
            EXPECT_TRUE(aggressor_filled);

            ASSERT_TRUE(book.best_ask().has_value());
            EXPECT_EQ(book.best_ask()->price, 102);
            EXPECT_EQ(book.best_ask()->qty, 3U);
            EXPECT_FALSE(book.best_bid().has_value());
            EXPECT_EQ(book.last_trade_price(), 102);
        }

        TEST(OrderBookBasics, PartialFillRestsRemainder) {
            Book book(SYMBOL);

            (void)book.add_order(1, core::Side::SELL, 100, 4, core::OrderType::LIMIT, 1, 1);
            auto events = book.add_order(2, core::Side::BUY, 100, 10, core::OrderType::LIMIT, 2, 2);

            EXPECT_EQ(count_of(events, EventType::ORDER_PARTIALLY_FILLED), 1U);
            EXPECT_EQ(count_of(events, EventType::ORDER_RESTED), 1U);
            ASSERT_TRUE(book.best_bid().has_value());
            EXPECT_EQ(book.best_bid()->qty, 6U);
            EXPECT_FALSE(book.best_ask().has_value());
        }

        TEST(OrderBookBasics, CancelAndModify) {
            Book book(SYMBOL);

            (void)book.add_order(1, core::Side::BUY, 100, 10, core::OrderType::LIMIT, 1, 1);
            (void)book.add_order(2, core::Side::BUY, 99, 10, core::OrderType::LIMIT, 2, 1);

            auto events = book.cancel_order(1, 3);
            EXPECT_EQ(types_of(events), (std::vector{ EventType::ORDER_CANCELED }));
            EXPECT_EQ(book.best_bid()->price, 99);

            // canceling twice -> rejected
            events = book.cancel_order(1, 4);
            EXPECT_EQ(reject_reason(events), ReasonCode::ORDER_NOT_FOUND);

            // same price, smaller qty -> reduce in place
            events = book.modify_order(2, 4, 99, 5);
            EXPECT_EQ(types_of(events), (std::vector{ EventType::ORDER_REDUCED }));
            EXPECT_EQ(book.best_bid()->qty, 4U);

            // new price -> cancel + re-insert under the same id
            events = book.modify_order(2, 4, 98, 6);
            EXPECT_EQ(types_of(events), (std::vector{ EventType::ORDER_CANCELED,
                                                      EventType::ORDER_ACCEPTED,
                                                      EventType::ORDER_RESTED }));
            EXPECT_EQ(book.best_bid()->price, 98);
        }

        TEST(OrderBookBasics, IocAndMarket) {
            Book book(SYMBOL);

            // market into empty book -> reject
            auto events = book.add_order(1, core::Side::BUY, 0, 5, core::OrderType::MARKET, 1, 1);
            EXPECT_EQ(reject_reason(events), ReasonCode::BOOK_EMPTY);

            (void)book.add_order(2, core::Side::SELL, 100, 3, core::OrderType::LIMIT, 2, 2);

            // IOC for 5, only 3 available -> fill 3, cancel 2, nothing rests
            events = book.add_order(3, core::Side::BUY, 100, 5, core::OrderType::IOC, 3, 1);
            EXPECT_EQ(count_of(events, EventType::TRADE), 1U);
            EXPECT_EQ(count_of(events, EventType::ORDER_CANCELED), 1U);
            EXPECT_EQ(count_of(events, EventType::ORDER_RESTED), 0U);
            EXPECT_FALSE(book.best_bid().has_value());
            EXPECT_FALSE(book.best_ask().has_value());
        }

        TEST(OrderBookBasics, StopOrderTriggersOnTrade) {
            Book book(SYMBOL);

            (void)book.add_order(1, core::Side::SELL, 100, 5, core::OrderType::LIMIT, 1, 1);
            (void)book.add_order(2, core::Side::SELL, 105, 5, core::OrderType::LIMIT, 2, 1);
            // stop buy: becomes a market order once a trade prints at >= 100
            (void)book.add_order(3, core::Side::BUY, 0, 2, core::OrderType::STOP, 3, 2, 100);

            auto events = book.add_order(4, core::Side::BUY, 100, 5, core::OrderType::LIMIT, 4, 3);
            // first trade: order 4 vs 1 at 100, then the stop wakes up and buys 2 at 105
            EXPECT_EQ(count_of(events, EventType::TRADE), 2U);
            ASSERT_TRUE(book.best_ask().has_value());
            EXPECT_EQ(book.best_ask()->qty, 3U);
        }

        TEST(OrderBookBasics, DepthSnapshot) {
            Book book(SYMBOL);

            (void)book.add_order(1, core::Side::BUY, 100, 1, core::OrderType::LIMIT, 1, 1);
            (void)book.add_order(2, core::Side::BUY, 102, 2, core::OrderType::LIMIT, 2, 1);
            (void)book.add_order(3, core::Side::BUY, 101, 3, core::OrderType::LIMIT, 3, 1);
            (void)book.add_order(4, core::Side::BUY, 101, 4, core::OrderType::LIMIT, 4, 1);

            std::array<core::PriceLevelView, 2> levels{};
            const std::size_t count = book.depth(core::Side::BUY, levels);
            ASSERT_EQ(count, 2U);
            EXPECT_EQ(levels[0].price, 102);
            EXPECT_EQ(levels[0].qty, 2U);
            EXPECT_EQ(levels[1].price, 101);
            EXPECT_EQ(levels[1].qty, 7U);
        }

        // a market maker cancels/replaces all day. with 1024 order slots and 256 level
        // slots, 50k rounds only pass if orders, index nodes and levels are recycled
        TEST(OrderBookBasics, LongRunningChurnReusesMemory) {
            Book book(SYMBOL);

            core::OrderId next_id = 1;
            for (int round = 0; round < 50'000; ++round) {
                // drifting prices -> keeps creating brand new levels
                const core::Price price = 1'000 + (round % 5'000);
                const core::OrderId id = next_id++;
                const auto timestamp = static_cast<core::Timestamp>(round);

                auto events = book.add_order(id, core::Side::BUY, price, 10, core::OrderType::LIMIT, timestamp, 1);
                ASSERT_EQ(events.size(), 2U) << "round " << round;
                ASSERT_EQ(events[1].type, EventType::ORDER_RESTED) << "round " << round;

                events = book.cancel_order(id, timestamp);
                ASSERT_EQ(events.size(), 1U) << "round " << round;
                ASSERT_EQ(events[0].type, EventType::ORDER_CANCELED) << "round " << round;

                // and some trading so fills also recycle
                if (round % 3 == 0) {
                    (void)book.add_order(next_id++, core::Side::SELL, price, 5,
                        core::OrderType::LIMIT, timestamp, 2);
                    events = book.add_order(next_id++, core::Side::BUY, price, 5,
                        core::OrderType::LIMIT, timestamp, 3);
                    ASSERT_EQ(count_of(events, EventType::TRADE), 1U) << "round " << round;
                }
            }
            EXPECT_EQ(book.active_order_count(), 0U);
            EXPECT_FALSE(book.best_bid().has_value());
        }
    }
}
