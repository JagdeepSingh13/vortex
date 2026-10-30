#include <atomic>
#include <cstdint>
#include <thread>

#include <gtest/gtest.h>

#include "core/spsc_queue.h"

namespace exchange::testing {
    namespace {

        using Queue = core::SPSCQueue<std::uint64_t, 8>;

        TEST(SpscQueue, PopFromEmptyFails) {
            Queue queue;
            std::uint64_t value = 0;
            EXPECT_TRUE(queue.empty());
            EXPECT_FALSE(queue.pop(value));
        }

        TEST(SpscQueue, KeepsFifoOrder) {
            Queue queue;
            for (std::uint64_t value = 1; value <= 5; ++value) {
                ASSERT_TRUE(queue.push(value));
            }
            for (std::uint64_t expected = 1; expected <= 5; ++expected) {
                std::uint64_t value = 0;
                ASSERT_TRUE(queue.pop(value));
                EXPECT_EQ(value, expected);
            }
            EXPECT_TRUE(queue.empty());
        }

        TEST(SpscQueue, PushToFullFailsWithoutOverwriting) {
            Queue queue;
            for (std::uint64_t value = 0; value < 8; ++value) {
                ASSERT_TRUE(queue.push(value));
            }
            EXPECT_FALSE(queue.push(99));

            std::uint64_t value = 0;
            ASSERT_TRUE(queue.pop(value));
            EXPECT_EQ(value, 0U);
            EXPECT_TRUE(queue.push(8));  // room again
        }

        // head/tail grow forever and are masked into the buffer;
        // go around the ring many times with the queue nearly full
        TEST(SpscQueue, WrapsAroundManyTimes) {
            Queue queue;
            std::uint64_t next_push = 0;
            std::uint64_t next_pop = 0;
            for (int i = 0; i < 7; ++i) {
                ASSERT_TRUE(queue.push(next_push++));
            }
            for (int round = 0; round < 10'000; ++round) {
                ASSERT_TRUE(queue.push(next_push++));
                std::uint64_t value = 0;
                ASSERT_TRUE(queue.pop(value));
                ASSERT_EQ(value, next_pop++);
            }
        }

        // one producer thread, one consumer thread, every value arrives exactly once and in order.
        // run under ThreadSanitizer (make test-tsan) this also checks the memory ordering.
        TEST(SpscQueue, ProducerAndConsumerThreadsLoseNothing) {
            constexpr std::uint64_t COUNT = 2'000'000;
            static core::SPSCQueue<std::uint64_t, 1024> queue;  // static: too big for the stack

            std::thread producer([] {
                for (std::uint64_t value = 0; value < COUNT; ++value) {
                    while (!queue.push(value)) {
                        std::this_thread::yield();
                    }
                }
            });

            std::uint64_t expected = 0;
            std::uint64_t out_of_order = 0;
            while (expected < COUNT) {
                std::uint64_t value = 0;
                if (!queue.pop(value)) {
                    std::this_thread::yield();
                    continue;
                }
                if (value != expected) {
                    ++out_of_order;
                }
                expected = value + 1;
            }
            producer.join();

            EXPECT_EQ(out_of_order, 0U);
            EXPECT_EQ(expected, COUNT);
            EXPECT_TRUE(queue.empty());
        }
    }
}
