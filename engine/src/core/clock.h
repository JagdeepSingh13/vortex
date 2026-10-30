#pragma once

#include <chrono>

#include "core/types.h"

namespace exchange::core {
    class LogicalClock {
    public:
        [[nodiscard]] Timestamp next() noexcept { return ++current_; }

        [[nodiscard]] Timestamp current() const noexcept { return current_; }

        void reset() noexcept { current_ = 0; }

    private:
        Timestamp current_{ 0 };
    };

    [[nodiscard]] inline Timestamp steady_timestamp_now() noexcept {
        // steady clock is monotonic
        const auto now = std::chrono::steady_clock::now().time_since_epoch();

        return static_cast<Timestamp> (
            std::chrono::duration_cast<std::chrono::nanoseconds>(now).count());
    }

    [[nodiscard]] inline Timestamp now_ns() noexcept {
        return steady_timestamp_now();
    }

    // UTC nanoseconds since the Unix epoch (CLOCK_REALTIME on Linux, read through the
    // vDSO, so no system call). this is the event_time_ns of the contracts.
    // it can jump when NTP corrects the clock: use it to stamp events,
    // never to measure durations (that is what the steady clock is for)
    [[nodiscard]] inline Timestamp wall_clock_ns() noexcept {
        const auto now = std::chrono::system_clock::now().time_since_epoch();

        return static_cast<Timestamp>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(now).count());
    }
}