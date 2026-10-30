#include <array>
#include <cstdint>
#include <cstdlib>
#include <fstream>
#include <sstream>
#include <string>

#include <gtest/gtest.h>

#include "event_text.h"
#include "matching/matching_engine.h"
#include "support/order_stream.h"

// golden replay: a fixed, seeded order stream goes through the engine and every
// request + resulting event is written as text. the output must match the files in
// tests/golden exactly. this is the proof required before changing anything near
// matching: if behaviour changes, this test shows the first line that differs.
//
//   replay_small.txt   1,000 requests, full text, readable in a diff
//   replay_large.txt   300,000 requests, line and event counts + FNV-1a hash
//
// to accept an intended change: VORTEX_UPDATE_GOLDEN=1 make test-engine,
// then review the diff of tests/golden before committing it.

#ifndef VORTEX_GOLDEN_DIR
#error "VORTEX_GOLDEN_DIR must point at engine/tests/golden"
#endif

namespace exchange::testing {
    namespace {

        using Engine = matching::MatchingEngine<16'384, 4'096, 8'192, 4'096>;

        const std::array SYMBOLS{ core::make_symbol("AAPL"), core::make_symbol("MSFT") };

        std::string golden_path(const char* name) {
            return std::string(VORTEX_GOLDEN_DIR) + "/" + name;
        }

        bool update_mode() {
            const char* flag = std::getenv("VORTEX_UPDATE_GOLDEN");
            return flag != nullptr && std::string(flag) == "1";
        }

        // runs the stream and hands every line (request or event) to `sink`
        template <typename Sink>
        void replay(std::size_t requests, Sink&& sink) {
            static Engine engine;  // static: the books are several MB, too big for the stack
            engine.prepare_symbols(SYMBOLS);

            support::OrderStream stream(SYMBOLS);
            for (std::size_t index = 0; index < requests; ++index) {
                const matching::InboundOrder order = stream.next();
                sink(to_text(order), false);
                for (const matching::Event& event : engine.process_order(order)) {
                    sink(to_text(event), true);
                }
            }
        }

        struct Digest {
            std::uint64_t lines = 0;
            std::uint64_t events = 0;
            std::uint64_t trades = 0;
            std::uint64_t hash = 0xcbf29ce484222325ULL;  // FNV-1a 64 offset basis

            void add(const std::string& line, bool is_event) {
                ++lines;
                events += is_event ? 1 : 0;
                trades += line.starts_with("  TRADE") ? 1 : 0;
                for (const char c : line + "\n") {
                    hash ^= static_cast<unsigned char>(c);
                    hash *= 0x100000001b3ULL;  // FNV-1a 64 prime
                }
            }

            std::string text() const {
                std::ostringstream out;
                out << "requests 300000\nlines " << lines << "\nevents " << events
                    << "\ntrades " << trades << "\nfnv1a64 " << std::hex << hash << "\n";
                return out.str();
            }
        };

        std::string read_file(const std::string& path) {
            std::ifstream in(path, std::ios::binary);
            std::ostringstream out;
            out << in.rdbuf();
            return out.str();
        }

        void write_file(const std::string& path, const std::string& content) {
            std::ofstream(path, std::ios::binary) << content;
        }

        TEST(GoldenReplay, MatchesRecordedRun) {
            std::ostringstream small;
            Digest digest;

            // first 1,000 requests are kept as full text, the whole run is hashed.
            // both come from one replay so the engine state is a single continuous run.
            constexpr std::size_t SMALL = 1'000;
            constexpr std::size_t LARGE = 300'000;
            std::size_t requests_seen = 0;
            replay(LARGE, [&](const std::string& line, bool is_event) {
                if (!is_event) {
                    ++requests_seen;
                }
                if (requests_seen <= SMALL) {
                    small << line << '\n';
                }
                digest.add(line, is_event);
            });

            const std::string small_path = golden_path("replay_small.txt");
            const std::string large_path = golden_path("replay_large.txt");

            if (update_mode()) {
                write_file(small_path, small.str());
                write_file(large_path, digest.text());
                GTEST_SKIP() << "golden files rewritten; review `git diff engine/tests/golden`";
            }

            const std::string expected_small = read_file(small_path);
            ASSERT_FALSE(expected_small.empty()) << "missing " << small_path
                << " (create it with VORTEX_UPDATE_GOLDEN=1)";

            // report the first differing line, not a megabyte of diff
            std::istringstream got(small.str());
            std::istringstream want(expected_small);
            std::string got_line;
            std::string want_line;
            for (int line_number = 1;; ++line_number) {
                const bool has_got = static_cast<bool>(std::getline(got, got_line));
                const bool has_want = static_cast<bool>(std::getline(want, want_line));
                if (!has_got && !has_want)
                    break;
                ASSERT_EQ(got_line, want_line) << "replay_small.txt differs at line " << line_number;
            }

            EXPECT_EQ(digest.text(), read_file(large_path)) << "300k-request replay differs";
        }
    }
}
