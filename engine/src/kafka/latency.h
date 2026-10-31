#pragma once

#include <algorithm>
#include <cstdint>
#include <format>
#include <string>
#include <vector>

// latency samples in microseconds, kept by one thread (consumer or publisher) and turned
// into percentiles at the end of a run. allocation is fine here: these threads are not
// the matching thread.

namespace exchange::kafka {

    class LatencyRecorder {
    public:
        explicit LatencyRecorder(std::size_t expected = 1'000'000) { samples_.reserve(expected); }

        void record(std::int64_t microseconds) {
            samples_.push_back(microseconds < 0 ? 0 : microseconds);
        }

        [[nodiscard]] std::size_t count() const { return samples_.size(); }

        // "n=… p50=… p90=… p99=… p99.9=… max=… (us)"; sorts in place, call once at the end
        [[nodiscard]] std::string summary() {
            if (samples_.empty()) {
                return "n=0";
            }
            std::ranges::sort(samples_);
            const auto at = [&](double q) {
                return samples_[static_cast<std::size_t>(q * static_cast<double>(samples_.size() - 1))];
            };
            return std::format("n={} p50={} p90={} p99={} p99.9={} max={} (us)",
                samples_.size(), at(0.50), at(0.90), at(0.99), at(0.999), samples_.back());
        }

    private:
        std::vector<std::int64_t> samples_;
    };
}
