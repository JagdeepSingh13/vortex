#include <array>
#include <span>
#include <vector>

#include <gtest/gtest.h>

#include "test_helpers.h"

// behaviour of the order book, one rule per test.
// prices and quantities are small integers (engine units); ids are arbitrary.

namespace exchange::testing {
    namespace {

        using core::OrderType;
        using core::Side;

        // ---------------- price-time priority ----------------

        TEST(PriceTimePriority, EarlierOrderAtSamePriceFillsFirst) {
            Book book(SYMBOL);
            (void)book.add_order(1, Side::SELL, 100, 5, OrderType::LIMIT, 1, 1);
            (void)book.add_order(2, Side::SELL, 100, 5, OrderType::LIMIT, 2, 1);

            const auto trades = trades_of(book.add_order(3, Side::BUY, 100, 5, OrderType::LIMIT, 3, 2));
            ASSERT_EQ(trades.size(), 1U);
            EXPECT_EQ(trades[0].sell_order_id, 1U);
        }

        TEST(PriceTimePriority, BetterPriceFillsBeforeEarlierOrder) {
            Book book(SYMBOL);
            (void)book.add_order(1, Side::SELL, 101, 5, OrderType::LIMIT, 1, 1);
            (void)book.add_order(2, Side::SELL, 100, 5, OrderType::LIMIT, 2, 1);

            const auto trades = trades_of(book.add_order(3, Side::BUY, 101, 5, OrderType::LIMIT, 3, 2));
            ASSERT_EQ(trades.size(), 1U);
            EXPECT_EQ(trades[0].sell_order_id, 2U);
            EXPECT_EQ(trades[0].price, 100);
        }

        TEST(PriceTimePriority, SweepStopsAtLimitPriceAndRestsRemainder) {
            Book book(SYMBOL);
            (void)book.add_order(1, Side::SELL, 100, 2, OrderType::LIMIT, 1, 1);
            (void)book.add_order(2, Side::SELL, 101, 2, OrderType::LIMIT, 2, 1);
            (void)book.add_order(3, Side::SELL, 102, 2, OrderType::LIMIT, 3, 1);

            const auto events = book.add_order(4, Side::BUY, 101, 10, OrderType::LIMIT, 4, 2);
            const auto trades = trades_of(events);
            ASSERT_EQ(trades.size(), 2U);
            EXPECT_EQ(trades[0].price, 100);
            EXPECT_EQ(trades[1].price, 101);
            EXPECT_EQ(count_of(events, EventType::ORDER_RESTED), 1U);

            ASSERT_TRUE(book.best_bid().has_value());
            EXPECT_EQ(book.best_bid()->price, 101);
            EXPECT_EQ(book.best_bid()->qty, 6U);
            EXPECT_EQ(book.best_ask()->price, 102);
        }

        TEST(PriceTimePriority, SellAggressorHitsHighestBidAndReportsSides) {
            Book book(SYMBOL);
            (void)book.add_order(1, Side::BUY, 99, 5, OrderType::LIMIT, 1, 1);
            (void)book.add_order(2, Side::BUY, 100, 5, OrderType::LIMIT, 2, 1);

            const auto trades = trades_of(book.add_order(3, Side::SELL, 90, 5, OrderType::LIMIT, 3, 2));
            ASSERT_EQ(trades.size(), 1U);
            EXPECT_EQ(trades[0].price, 100);
            EXPECT_EQ(trades[0].buy_order_id, 2U);
            EXPECT_EQ(trades[0].sell_order_id, 3U);
        }

        TEST(Sequencing, SequenceNumbersAndMatchIdsIncreaseAcrossCalls) {
            Book book(SYMBOL);
            core::SequenceNumber last_sequence = 0;
            std::vector<core::MatchId> match_ids;

            // the returned span points into the book's event buffer and is only
            // valid until the next call, so each one is checked straight away
            const auto check = [&](std::span<const Event> events) {
                for (const Event& event : events) {
                    // every payload starts with sequence_number
                    const core::SequenceNumber sequence = event.payload.order_rejected.sequence_number;
                    EXPECT_GT(sequence, last_sequence);
                    last_sequence = sequence;
                }
                for (const auto& trade : trades_of(events)) {
                    match_ids.push_back(trade.match_id);
                }
            };

            for (core::OrderId id = 1; id <= 20; id += 2) {
                check(book.add_order(id, Side::SELL, 100, 1, OrderType::LIMIT, id, 1));
                check(book.add_order(id + 1, Side::BUY, 100, 1, OrderType::LIMIT, id + 1, 2));
            }
            ASSERT_EQ(match_ids.size(), 10U);
            for (std::size_t index = 0; index < match_ids.size(); ++index) {
                EXPECT_EQ(match_ids[index], index + 1);
            }
        }

        // ---------------- order types ----------------

        TEST(OrderTypes, GtcRestsLikeLimit) {
            Book book(SYMBOL);
            const auto events = book.add_order(1, Side::BUY, 100, 5, OrderType::GTC, 1, 1);
            EXPECT_EQ(types_of(events), (std::vector{ EventType::ORDER_ACCEPTED, EventType::ORDER_RESTED }));
            EXPECT_EQ(book.best_bid()->price, 100);
        }

        TEST(OrderTypes, IocWithNothingMarketableIsCanceledWithoutResting) {
            Book book(SYMBOL);
            (void)book.add_order(1, Side::SELL, 100, 5, OrderType::LIMIT, 1, 1);

            const auto events = book.add_order(2, Side::BUY, 99, 5, OrderType::IOC, 2, 2);
            EXPECT_EQ(types_of(events), (std::vector{ EventType::ORDER_ACCEPTED, EventType::ORDER_CANCELED }));
            EXPECT_FALSE(book.best_bid().has_value());
        }

        TEST(OrderTypes, FokRejectedWhenLiquidityIsShortAndBookUnchanged) {
            Book book(SYMBOL);
            (void)book.add_order(1, Side::SELL, 100, 3, OrderType::LIMIT, 1, 1);
            (void)book.add_order(2, Side::SELL, 101, 3, OrderType::LIMIT, 2, 1);

            // 7 wanted up to 101, only 6 available
            const auto events = book.add_order(3, Side::BUY, 101, 7, OrderType::FOK, 3, 2);
            EXPECT_EQ(reject_reason(events), ReasonCode::FOK_INSUFFICIENT_LIQUIDITY);
            EXPECT_EQ(book.best_ask()->qty, 3U);
            EXPECT_EQ(book.active_order_count(), 2U);
        }

        TEST(OrderTypes, FokFillsCompletelyAcrossLevels) {
            Book book(SYMBOL);
            (void)book.add_order(1, Side::SELL, 100, 3, OrderType::LIMIT, 1, 1);
            (void)book.add_order(2, Side::SELL, 101, 3, OrderType::LIMIT, 2, 1);

            const auto events = book.add_order(3, Side::BUY, 101, 6, OrderType::FOK, 3, 2);
            EXPECT_EQ(count_of(events, EventType::TRADE), 2U);
            EXPECT_EQ(count_of(events, EventType::ORDER_FILLED), 3U);  // two passive + the FOK
            EXPECT_FALSE(book.best_ask().has_value());
        }

        // current behaviour: FOK only counts visible quantity, so hidden iceberg
        // quantity that could complete the fill is ignored
        TEST(OrderTypes, FokCountsOnlyVisibleIcebergQuantity) {
            Book book(SYMBOL);
            (void)book.add_order(1, Side::SELL, 100, 30, OrderType::ICEBERG, 1, 1, 0, 10);

            const auto events = book.add_order(2, Side::BUY, 100, 20, OrderType::FOK, 2, 2);
            EXPECT_EQ(reject_reason(events), ReasonCode::FOK_INSUFFICIENT_LIQUIDITY);
        }

        TEST(OrderTypes, PostOnlyRejectedWhenItWouldCross) {
            Book book(SYMBOL);
            (void)book.add_order(1, Side::SELL, 100, 5, OrderType::LIMIT, 1, 1);

            // touching the best ask counts as crossing
            EXPECT_EQ(reject_reason(book.add_order(2, Side::BUY, 100, 5, OrderType::POST_ONLY, 2, 2)),
                ReasonCode::POST_ONLY_WOULD_CROSS);

            const auto events = book.add_order(3, Side::BUY, 99, 5, OrderType::POST_ONLY, 3, 2);
            EXPECT_EQ(types_of(events), (std::vector{ EventType::ORDER_ACCEPTED, EventType::ORDER_RESTED }));
            EXPECT_EQ(book.best_bid()->price, 99);
        }

        TEST(OrderTypes, MarketSweepsLevelsAndCancelsUnfilledRest) {
            Book book(SYMBOL);
            (void)book.add_order(1, Side::SELL, 100, 2, OrderType::LIMIT, 1, 1);
            (void)book.add_order(2, Side::SELL, 105, 2, OrderType::LIMIT, 2, 1);

            const auto events = book.add_order(3, Side::BUY, 0, 6, OrderType::MARKET, 3, 2);
            const auto trades = trades_of(events);
            ASSERT_EQ(trades.size(), 2U);
            EXPECT_EQ(trades[0].price, 100);
            EXPECT_EQ(trades[1].price, 105);
            EXPECT_EQ(count_of(events, EventType::ORDER_PARTIALLY_FILLED), 1U);
            EXPECT_EQ(count_of(events, EventType::ORDER_CANCELED), 1U);
            EXPECT_EQ(count_of(events, EventType::ORDER_RESTED), 0U);
            EXPECT_EQ(book.active_order_count(), 0U);
        }

        // ---------------- iceberg ----------------

        TEST(Iceberg, ShowsOnlyDisplayQuantity) {
            Book book(SYMBOL);
            const auto events = book.add_order(1, Side::BUY, 100, 30, OrderType::ICEBERG, 1, 1, 0, 10);
            ASSERT_EQ(count_of(events, EventType::ORDER_RESTED), 1U);
            EXPECT_EQ(events[1].payload.order_rested.qty, 10U);
            EXPECT_EQ(book.best_bid()->qty, 10U);
        }

        TEST(Iceberg, ReplenishedSliceLosesTimePriority) {
            Book book(SYMBOL);
            (void)book.add_order(1, Side::BUY, 100, 30, OrderType::ICEBERG, 1, 1, 0, 10);
            (void)book.add_order(2, Side::BUY, 100, 5, OrderType::LIMIT, 2, 1);

            // eats the visible slice of 1, which then replenishes at the back of the queue
            auto trades = trades_of(book.add_order(3, Side::SELL, 100, 10, OrderType::LIMIT, 3, 2));
            ASSERT_EQ(trades.size(), 1U);
            EXPECT_EQ(trades[0].buy_order_id, 1U);
            EXPECT_EQ(book.best_bid()->qty, 15U);  // 5 from order 2 + new slice of 10

            trades = trades_of(book.add_order(4, Side::SELL, 100, 5, OrderType::LIMIT, 4, 2));
            ASSERT_EQ(trades.size(), 1U);
            EXPECT_EQ(trades[0].buy_order_id, 2U);
        }

        TEST(Iceberg, AggressiveIcebergRestsWithDisplaySlice) {
            Book book(SYMBOL);
            (void)book.add_order(1, Side::BUY, 100, 20, OrderType::LIMIT, 1, 1);

            const auto events = book.add_order(2, Side::SELL, 100, 50, OrderType::ICEBERG, 2, 2, 0, 10);
            EXPECT_EQ(count_of(events, EventType::TRADE), 1U);
            ASSERT_TRUE(book.best_ask().has_value());
            EXPECT_EQ(book.best_ask()->qty, 10U);
        }

        TEST(Iceberg, InvalidDisplayQuantityIsRejected) {
            Book book(SYMBOL);
            EXPECT_EQ(reject_reason(book.add_order(1, Side::BUY, 100, 10, OrderType::ICEBERG, 1, 1, 0, 0)),
                ReasonCode::INVALID_ICEBERG_DISPLAY);
            EXPECT_EQ(reject_reason(book.add_order(2, Side::BUY, 100, 10, OrderType::ICEBERG, 2, 1, 0, 11)),
                ReasonCode::INVALID_ICEBERG_DISPLAY);
        }

        TEST(Iceberg, ReductionTakesHiddenQuantityFirst) {
            Book book(SYMBOL);
            (void)book.add_order(1, Side::BUY, 100, 30, OrderType::ICEBERG, 1, 1, 0, 10);

            // 30 -> 25: only hidden shrinks, the visible book does not change, no event
            auto events = book.modify_order(1, 25, 100, 2);
            EXPECT_TRUE(events.empty());
            EXPECT_EQ(book.best_bid()->qty, 10U);

            // 25 -> 5: hidden gone, visible shrinks to 5
            events = book.modify_order(1, 5, 100, 3);
            ASSERT_EQ(types_of(events), (std::vector{ EventType::ORDER_REDUCED }));
            EXPECT_EQ(events[0].payload.order_reduced.new_qty, 5U);
            EXPECT_EQ(book.best_bid()->qty, 5U);
        }

        // ---------------- stop orders ----------------

        TEST(StopOrders, AcceptedEventCarriesTriggerPrice) {
            Book book(SYMBOL);
            const auto events = book.add_order(1, Side::BUY, 0, 2, OrderType::STOP, 1, 1, 105);
            EXPECT_EQ(types_of(events), (std::vector{ EventType::ORDER_ACCEPTED }));
            EXPECT_EQ(events[0].payload.order_accepted.price, 105);
            EXPECT_FALSE(book.best_bid().has_value());  // parked, not in the book
            EXPECT_EQ(book.active_order_count(), 1U);
        }

        TEST(StopOrders, StopBuyWaitsUntilTriggerIsReached) {
            Book book(SYMBOL);
            (void)book.add_order(1, Side::BUY, 0, 2, OrderType::STOP, 1, 1, 105);
            (void)book.add_order(2, Side::SELL, 100, 5, OrderType::LIMIT, 2, 2);
            (void)book.add_order(3, Side::SELL, 110, 5, OrderType::LIMIT, 3, 2);

            // trade at 100 < 105: stop stays parked
            const auto events = book.add_order(4, Side::BUY, 100, 5, OrderType::LIMIT, 4, 3);
            EXPECT_EQ(count_of(events, EventType::TRADE), 1U);
            EXPECT_EQ(book.best_ask()->price, 110);
            EXPECT_EQ(book.best_ask()->qty, 5U);
        }

        TEST(StopOrders, StopSellTriggersWhenPriceFallsToTrigger) {
            Book book(SYMBOL);
            (void)book.add_order(1, Side::BUY, 100, 5, OrderType::LIMIT, 1, 1);
            (void)book.add_order(2, Side::BUY, 95, 5, OrderType::LIMIT, 2, 1);
            (void)book.add_order(3, Side::SELL, 0, 3, OrderType::STOP, 3, 2, 100);

            // trade at 100 -> stop sell becomes a market sell and hits 95
            const auto events = book.add_order(4, Side::SELL, 100, 5, OrderType::LIMIT, 4, 3);
            const auto trades = trades_of(events);
            ASSERT_EQ(trades.size(), 2U);
            EXPECT_EQ(trades[0].price, 100);
            EXPECT_EQ(trades[1].price, 95);
            EXPECT_EQ(trades[1].sell_order_id, 3U);
            EXPECT_EQ(book.last_trade_price(), 95);
            EXPECT_EQ(book.best_bid()->qty, 2U);
        }

        TEST(StopOrders, StopLimitBecomesLimitAndRestsIfUnfilled) {
            Book book(SYMBOL);
            (void)book.add_order(1, Side::BUY, 101, 10, OrderType::STOP_LIMIT, 1, 1, 100);
            (void)book.add_order(2, Side::SELL, 100, 5, OrderType::LIMIT, 2, 2);

            // trade at 100 triggers the stop-limit; no asks left at <= 101, so it rests
            (void)book.add_order(3, Side::BUY, 100, 5, OrderType::LIMIT, 3, 3);
            ASSERT_TRUE(book.best_bid().has_value());
            EXPECT_EQ(book.best_bid()->price, 101);
            EXPECT_EQ(book.best_bid()->qty, 10U);
        }

        TEST(StopOrders, TriggeredStopCanTriggerAnotherStop) {
            Book book(SYMBOL);
            (void)book.add_order(1, Side::SELL, 100, 1, OrderType::LIMIT, 1, 1);
            (void)book.add_order(2, Side::SELL, 105, 1, OrderType::LIMIT, 2, 1);
            (void)book.add_order(3, Side::SELL, 110, 1, OrderType::LIMIT, 3, 1);
            (void)book.add_order(4, Side::BUY, 0, 1, OrderType::STOP, 4, 2, 100);  // fires at 100, buys at 105
            (void)book.add_order(5, Side::BUY, 0, 1, OrderType::STOP, 5, 2, 105);  // fires at 105, buys at 110

            const auto trades = trades_of(book.add_order(6, Side::BUY, 100, 1, OrderType::LIMIT, 6, 3));
            ASSERT_EQ(trades.size(), 3U);
            EXPECT_EQ(trades[0].price, 100);
            EXPECT_EQ(trades[1].price, 105);
            EXPECT_EQ(trades[2].price, 110);
            EXPECT_FALSE(book.best_ask().has_value());
        }

        TEST(StopOrders, CanceledStopNeverFires) {
            Book book(SYMBOL);
            (void)book.add_order(1, Side::BUY, 0, 2, OrderType::STOP, 1, 1, 100);
            EXPECT_EQ(types_of(book.cancel_order(1, 2)), (std::vector{ EventType::ORDER_CANCELED }));

            (void)book.add_order(2, Side::SELL, 100, 5, OrderType::LIMIT, 3, 2);
            const auto events = book.add_order(3, Side::BUY, 100, 1, OrderType::LIMIT, 4, 3);
            EXPECT_EQ(count_of(events, EventType::TRADE), 1U);
            EXPECT_EQ(book.best_ask()->qty, 4U);
        }

        TEST(StopOrders, ModifyingAStopIsRejected) {
            Book book(SYMBOL);
            (void)book.add_order(1, Side::BUY, 0, 2, OrderType::STOP, 1, 1, 100);
            EXPECT_EQ(reject_reason(book.modify_order(1, 3, 0, 2)), ReasonCode::INVALID_MODIFICATION);
        }

        // ---------------- modify ----------------

        TEST(Modify, ReducingQuantityKeepsTimePriority) {
            Book book(SYMBOL);
            (void)book.add_order(1, Side::BUY, 100, 10, OrderType::LIMIT, 1, 1);
            (void)book.add_order(2, Side::BUY, 100, 10, OrderType::LIMIT, 2, 1);
            (void)book.modify_order(1, 4, 100, 3);

            const auto trades = trades_of(book.add_order(3, Side::SELL, 100, 1, OrderType::LIMIT, 4, 2));
            ASSERT_EQ(trades.size(), 1U);
            EXPECT_EQ(trades[0].buy_order_id, 1U);
        }

        TEST(Modify, IncreasingQuantityLosesTimePriority) {
            Book book(SYMBOL);
            (void)book.add_order(1, Side::BUY, 100, 10, OrderType::LIMIT, 1, 1);
            (void)book.add_order(2, Side::BUY, 100, 10, OrderType::LIMIT, 2, 1);

            const auto events = book.modify_order(1, 20, 100, 3);
            EXPECT_EQ(types_of(events), (std::vector{ EventType::ORDER_CANCELED,
                                                      EventType::ORDER_ACCEPTED,
                                                      EventType::ORDER_RESTED }));

            const auto trades = trades_of(book.add_order(3, Side::SELL, 100, 1, OrderType::LIMIT, 4, 2));
            ASSERT_EQ(trades.size(), 1U);
            EXPECT_EQ(trades[0].buy_order_id, 2U);
        }

        TEST(Modify, UnchangedPriceAndQuantityDoesNothing) {
            Book book(SYMBOL);
            (void)book.add_order(1, Side::BUY, 100, 10, OrderType::LIMIT, 1, 1);
            EXPECT_TRUE(book.modify_order(1, 10, 100, 2).empty());
        }

        TEST(Modify, ToCrossingPriceTradesImmediately) {
            Book book(SYMBOL);
            (void)book.add_order(1, Side::SELL, 101, 5, OrderType::LIMIT, 1, 1);
            (void)book.add_order(2, Side::BUY, 99, 5, OrderType::LIMIT, 2, 2);

            const auto events = book.modify_order(2, 5, 101, 3);
            EXPECT_EQ(count_of(events, EventType::TRADE), 1U);
            EXPECT_FALSE(book.best_ask().has_value());
            EXPECT_FALSE(book.best_bid().has_value());
        }

        // ---------------- rejects ----------------

        TEST(Rejects, NewOrderValidation) {
            Book book(SYMBOL);
            EXPECT_EQ(reject_reason(book.add_order(1, Side::BUY, 100, 0, OrderType::LIMIT, 1, 1)),
                ReasonCode::ZERO_QUANTITY);
            EXPECT_EQ(reject_reason(book.add_order(2, Side::BUY, 0, 5, OrderType::LIMIT, 2, 1)),
                ReasonCode::NEGATIVE_PRICE);
            EXPECT_EQ(reject_reason(book.add_order(3, Side::BUY, -1, 5, OrderType::LIMIT, 3, 1)),
                ReasonCode::NEGATIVE_PRICE);
            EXPECT_EQ(reject_reason(book.add_order(4, Side::BUY, 0, 5, OrderType::STOP, 4, 1, 0)),
                ReasonCode::NEGATIVE_PRICE);
            EXPECT_EQ(book.active_order_count(), 0U);
        }

        TEST(Rejects, DuplicateIdWhileLiveButReusableAfterFill) {
            Book book(SYMBOL);
            (void)book.add_order(1, Side::BUY, 100, 5, OrderType::LIMIT, 1, 1);
            EXPECT_EQ(reject_reason(book.add_order(1, Side::BUY, 99, 5, OrderType::LIMIT, 2, 1)),
                ReasonCode::DUPLICATE_ORDER_ID);

            (void)book.add_order(2, Side::SELL, 100, 5, OrderType::LIMIT, 3, 2);
            // order 1 is filled and dropped from the index, so the id is free again
            const auto events = book.add_order(1, Side::BUY, 99, 5, OrderType::LIMIT, 4, 1);
            EXPECT_EQ(types_of(events), (std::vector{ EventType::ORDER_ACCEPTED, EventType::ORDER_RESTED }));
        }

        TEST(Rejects, CancelAndModifyUnknownOrder) {
            Book book(SYMBOL);
            EXPECT_EQ(reject_reason(book.cancel_order(42, 1)), ReasonCode::ORDER_NOT_FOUND);
            EXPECT_EQ(reject_reason(book.modify_order(42, 5, 100, 2)), ReasonCode::ORDER_NOT_FOUND);
        }

        TEST(Rejects, ModifyValidation) {
            Book book(SYMBOL);
            (void)book.add_order(1, Side::BUY, 100, 5, OrderType::LIMIT, 1, 1);
            EXPECT_EQ(reject_reason(book.modify_order(1, 0, 100, 2)), ReasonCode::ZERO_QUANTITY);
            EXPECT_EQ(reject_reason(book.modify_order(1, 5, -1, 3)), ReasonCode::NEGATIVE_PRICE);
        }

        TEST(Rejects, OrderPoolExhaustion) {
            Book book(SYMBOL);  // 1024 order slots
            for (core::OrderId id = 1; id <= 1024; ++id) {
                ASSERT_EQ(count_of(book.add_order(id, Side::BUY, 100, 1, OrderType::LIMIT, id, 1),
                    EventType::ORDER_RESTED), 1U);
            }
            EXPECT_EQ(reject_reason(book.add_order(1025, Side::BUY, 100, 1, OrderType::LIMIT, 1025, 1)),
                ReasonCode::ORDER_POOL_EXHAUSTED);

            // freeing one slot makes room again
            (void)book.cancel_order(1, 1026);
            EXPECT_EQ(count_of(book.add_order(1025, Side::BUY, 100, 1, OrderType::LIMIT, 1027, 1),
                EventType::ORDER_RESTED), 1U);
        }

        // ---------------- market data ----------------

        TEST(MarketData, AskDepthIsAscendingAndSkipsConsumedLevels) {
            Book book(SYMBOL);
            (void)book.add_order(1, Side::SELL, 103, 1, OrderType::LIMIT, 1, 1);
            (void)book.add_order(2, Side::SELL, 101, 2, OrderType::LIMIT, 2, 1);
            (void)book.add_order(3, Side::SELL, 102, 3, OrderType::LIMIT, 3, 1);
            (void)book.add_order(4, Side::BUY, 101, 2, OrderType::LIMIT, 4, 2);  // consumes 101

            std::array<core::PriceLevelView, 5> levels{};
            ASSERT_EQ(book.depth(Side::SELL, levels), 2U);
            EXPECT_EQ(levels[0].price, 102);
            EXPECT_EQ(levels[1].price, 103);
            EXPECT_EQ(book.best_ask()->price, 102);
        }
    }
}
