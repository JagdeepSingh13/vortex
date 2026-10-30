#pragma once

#include <atomic>
#include <cstdint>
#include <string>
#include <string_view>

#include "core/spsc_queue.h"
#include "core/types.h"
#include "exchange/protocol.h"

// one connected client = one session = two one-way queues.
//
//   client thread  --requests-->   engine thread
//   client thread  <--responses--  engine thread
//
// each queue has exactly one producer and one consumer, which is what lets
// us use the lock free SPSC queue. with many clients the engine simply polls
// every session's request queue in turn.
//
// today the "client thread" is a bot in the same process. in step 4 it
// becomes the TCP gateway thread acting on behalf of a remote client,
// and nothing on the engine side has to change.

namespace exchange::gateway {

    inline constexpr std::size_t REQUEST_QUEUE_SIZE = 4'096;
    inline constexpr std::size_t RESPONSE_QUEUE_SIZE = 16'384;

    struct Session {
        Session(core::ParticipantId id, std::string_view session_name)
            : participant_id(id), name(session_name)
        {
        }

        Session(const Session&) = delete;
        Session& operator=(const Session&) = delete;

        const core::ParticipantId participant_id;
        const std::string name;

        // written by client, read by engine
        core::SPSCQueue<protocol::ClientRequest, REQUEST_QUEUE_SIZE> requests;
        // written by engine, read by client
        core::SPSCQueue<protocol::ServerMessage, RESPONSE_QUEUE_SIZE> responses;

        // market data the engine had to drop because this client was too slow
        std::atomic<std::uint64_t> dropped_market_data{ 0 };
    };
}
