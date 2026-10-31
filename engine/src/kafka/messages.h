#pragma once

#include <type_traits>

#include "core/types.h"
#include "exchange/protocol.h"
#include "matching/events.h"

// what travels through the two SPSC queues around the engine thread.
// trivially copyable and fixed size: a push is a plain copy, no allocation.

namespace exchange::kafka {

    // consumer thread -> engine thread
    struct InboundRequest {
        core::ParticipantId participant;
        protocol::ClientRequest request;   // prices and quantities already in engine units
        core::Timestamp client_time_ns;    // when the producer created the order (its clock)
        core::Timestamp received_ns;       // when the consumer thread decoded it
    };

    // engine thread -> publisher thread
    struct OutboundTrade {
        matching::Trade trade;             // engine units, packed order ids
        core::Timestamp event_time_ns;     // UTC, read once for the request (ADR 0009)
        core::Timestamp order_time_ns;     // client_time_ns of the order whose request caused
                                           // this trade: start of the end-to-end measurement
    };

    static_assert(std::is_trivially_copyable_v<InboundRequest>);
    static_assert(std::is_trivially_copyable_v<OutboundTrade>);
}
