#pragma once

#include <deque>

#include "client/order_entry.h"
#include "exchange/protocol.h"
#include "exchange/session.h"

// in-process client: talks to the engine directly through a Session's queues.
//
// sending never blocks. if the request queue is full the request waits in a
// local backlog. blocking here could deadlock: the engine may be waiting for
// us to drain our responses while we wait for it to drain our requests.

namespace exchange::client {

    class LocalClient : public OrderEntry<LocalClient> {
    public:

        explicit LocalClient(gateway::Session& session) : session_(session) {}

        void transmit(const protocol::ClientRequest& request) {
            if (!backlog_.empty() || !session_.requests.push(request)) {
                backlog_.push_back(request);
            }
        }

        // flush the backlog, then hand every waiting message to `handler`
        template <typename Handler>
        std::size_t poll(Handler&& handler) {
            while (!backlog_.empty() && session_.requests.push(backlog_.front())) {
                backlog_.pop_front();
            }

            std::size_t count = 0;
            protocol::ServerMessage message{};
            while (session_.responses.pop(message)) {
                handler(message);
                ++count;
            }
            return count;
        }

        [[nodiscard]] bool connected() const noexcept { return true; }

        [[nodiscard]] core::ParticipantId participant_id() const noexcept {
            return session_.participant_id;
        }

    private:
        gateway::Session& session_;
        std::deque<protocol::ClientRequest> backlog_;
    };
}
