#include <cstdint>
#include <set>

#include <gtest/gtest.h>

#include "core/memory_pool.h"
#include "core/types.h"
#include "matching/matching_engine.h"

namespace exchange::testing {
    namespace {

        struct Tracked {
            static inline int destroyed = 0;
            std::uint64_t payload{ 0 };
            ~Tracked() { ++destroyed; }
        };

        TEST(MemoryPool, HandsOutDistinctAlignedSlotsUntilFull) {
            core::MemoryPool<Tracked, 16> pool;
            std::set<Tracked*> seen;
            for (int index = 0; index < 16; ++index) {
                Tracked* slot = pool.allocate();
                ASSERT_NE(slot, nullptr);
                EXPECT_EQ(reinterpret_cast<std::uintptr_t>(slot) % 64, 0U) << "slots are cache-line aligned";
                EXPECT_TRUE(seen.insert(slot).second) << "slot handed out twice";
            }
            EXPECT_EQ(pool.allocate(), nullptr);
        }

        TEST(MemoryPool, ReusesTheMostRecentlyFreedSlot) {
            core::MemoryPool<Tracked, 4> pool;
            Tracked* first = std::construct_at(pool.allocate());
            Tracked* second = std::construct_at(pool.allocate());
            (void)first;

            pool.deallocate(second);
            EXPECT_EQ(pool.allocate(), second);  // free list is LIFO: hot in cache
        }

        TEST(MemoryPool, DeallocateRunsTheDestructor) {
            core::MemoryPool<Tracked, 4> pool;
            Tracked::destroyed = 0;
            pool.deallocate(std::construct_at(pool.allocate()));
            EXPECT_EQ(Tracked::destroyed, 1);
            pool.deallocate(nullptr);  // ignored
            EXPECT_EQ(Tracked::destroyed, 1);
        }

        // ---------------- matching engine routing ----------------

        using Engine = matching::MatchingEngine<1024, 256, 1024, 64>;

        TEST(MatchingEngine, KeepsOneIndependentBookPerSymbol) {
            static Engine engine;
            const core::Symbol aapl = core::make_symbol("AAPL");
            const core::Symbol msft = core::make_symbol("MSFT");
            const std::array symbols{ aapl, msft };
            engine.prepare_symbols(symbols);

            (void)engine.process_order({ .symbol = aapl, .order_id = 1, .side = core::Side::SELL,
                .price = 100, .qty = 5, .timestamp = 1, .participant_id = 1 });
            // same price on another symbol must not trade against AAPL
            const auto events = engine.process_order({ .symbol = msft, .order_id = 2, .side = core::Side::BUY,
                .price = 100, .qty = 5, .timestamp = 2, .participant_id = 2 });

            for (const auto& event : events) {
                EXPECT_NE(event.type, matching::EventType::TRADE);
            }
            ASSERT_NE(engine.find_book(aapl), nullptr);
            EXPECT_EQ(engine.find_book(aapl)->best_ask()->price, 100);
            EXPECT_EQ(engine.find_book(msft)->best_bid()->price, 100);
            EXPECT_EQ(engine.find_book(core::make_symbol("GOOG")), nullptr);
        }
    }
}
