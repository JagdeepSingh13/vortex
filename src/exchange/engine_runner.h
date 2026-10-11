#pragma once

#include <atomic>
#include <cstdint>
#include <thread>
#include <vector>

#include "exchange/exchange_core.h"
#include "exchange/session.h"

// the engine thread. single threaded on purpose: the book is never shared,
// so there are no locks anywhere in matching.
//
//   loop:
//     for each session: pop up to BATCH requests -> core.handle()
//     nothing to do?    -> yield the cpu
//
// polling in batches keeps one very busy client from starving the others.

namespace exchange::gateway {

    template <typename Core>
    class EngineRunner {
        static constexpr int BATCH = 64;

    public:

        // sessions must be registered before run() starts; index = participant id
        EngineRunner(Core& core, std::vector<Session*> sessions)
            : core_(core), sink_{ *this }
        {
            for (Session* session : sessions) {
                if (session->participant_id >= by_participant_.size()) {
                    by_participant_.resize(session->participant_id + 1U, nullptr);
                }
                by_participant_[session->participant_id] = session;
                sessions_.push_back(session);
            }
        }

        void run(const std::atomic<bool>& running) {
            running_ = &running;

            while (running.load(std::memory_order_relaxed)) {
                bool did_work = false;

                for (Session* session : sessions_) {
                    protocol::ClientRequest request{};
                    for (int count = 0; count < BATCH && session->requests.pop(request); ++count) {
                        core_.handle(session->participant_id, request, sink_);
                        requests_processed_.fetch_add(1, std::memory_order_relaxed);
                        did_work = true;
                    }
                }

                // a real exchange would busy spin on a pinned core here.
                // yield keeps the latency low without burning a laptop core at 100%
                if (!did_work) {
                    std::this_thread::yield();
                }
            }
        }

        [[nodiscard]] std::uint64_t requests_processed() const noexcept {
            return requests_processed_.load(std::memory_order_relaxed);
        }

        [[nodiscard]] std::uint64_t dropped_market_data() const noexcept {
            std::uint64_t total = 0;
            for (const Session* session : sessions_) {
                total += session->dropped_market_data.load(std::memory_order_relaxed);
            }
            return total;
        }

    private:

        struct QueueSink {
            EngineRunner& runner;

            // private reports must never be lost: wait for the client to make room.
            // (a real exchange would disconnect a client that stays this slow)
            void send(core::ParticipantId to, const protocol::ServerMessage& message) {
                Session* session = runner.session_for(to);
                if (session == nullptr)
                    return;

                while (!session->responses.push(message)) {
                    if (!runner.running_->load(std::memory_order_relaxed))
                        return;
                    std::this_thread::yield();
                }
            }

            // market data is a snapshot stream, the next update replaces this one,
            // so a slow client just misses some instead of stalling the engine
            void broadcast(const protocol::ServerMessage& message) {
                for (Session* session : runner.sessions_) {
                    if (!session->responses.push(message)) {
                        session->dropped_market_data.fetch_add(1, std::memory_order_relaxed);
                    }
                }
            }
        };

        [[nodiscard]] Session* session_for(core::ParticipantId participant) const noexcept {
            if (participant >= by_participant_.size())
                return nullptr;
            return by_participant_[participant];
        }

        Core& core_;
        QueueSink sink_;
        std::vector<Session*> sessions_;
        std::vector<Session*> by_participant_;
        const std::atomic<bool>* running_{ nullptr };
        std::atomic<std::uint64_t> requests_processed_{ 0 };
    };
}
