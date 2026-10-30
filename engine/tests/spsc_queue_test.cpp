#include <cstdint>
#include <thread>

#include <gtest/gtest.h>

#include "core/spsc_queue.h"

namespace exchange::testing {
    namespace {

        // one producer thread, one consumer thread, every value arrives exactly once and in order.
        // the small queue keeps it full or empty most of the time, so both wrap-around and
        // the full/empty edges are hit millions of times. run under ThreadSanitizer
        // (make test-tsan) this also checks the acquire/release memory ordering.
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
