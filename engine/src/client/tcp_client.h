#pragma once

#include <chrono>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "client/order_entry.h"
#include "net/connection.h"
#include "net/socket.h"
#include "net/wire.h"

// network client: same interface as LocalClient, but over TCP.
//
//   TcpClient client;
//   client.connect("127.0.0.1", 9000, "alice");   // blocks until LOGIN_ACK
//   client.send_limit(...);                         // never blocks
//   client.poll([](const protocol::ServerMessage& m) { ... });
//
// a NetworkInit must be alive while it is used (Winsock).

namespace exchange::client {

    class TcpClient : public OrderEntry<TcpClient> {
    public:

        bool connect(const std::string& host, std::uint16_t port, std::string_view name,
            std::chrono::milliseconds timeout = std::chrono::seconds(3))
        {
            const net::socket_t socket = net::connect_to(host, port);
            if (socket == net::INVALID_SOCKET_VALUE)
                return false;

            connection_.emplace(socket);
            connection_->queue(net::make_login(name));

            // wait for the server to tell us who we are
            const auto deadline = std::chrono::steady_clock::now() + timeout;
            while (std::chrono::steady_clock::now() < deadline) {
                if (!connection_->flush() || !connection_->receive()) {
                    break;
                }

                bool got_ack = false;
                connection_->for_each_frame([&](net::FrameType type, std::span<const char> payload) {
                    if (type != net::FrameType::LOGIN_ACK)
                        return net::FrameAction::STOP;  // leave anything else for poll()
                    const auto ack = net::read_payload<net::LoginAck>(payload);
                    participant_id_ = ack.participant_id;
                    symbols_.assign(ack.symbols.begin(), ack.symbols.begin() + ack.symbol_count);
                    got_ack = true;
                    return net::FrameAction::CONSUMED;
                    });

                if (got_ack)
                    return true;

                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }

            connection_.reset();
            return false;
        }

        void transmit(const protocol::ClientRequest& request) {
            if (!connected())
                return;
            connection_->queue(request);
            if (!connection_->flush()) {
                connection_.reset();
            }
        }

        template <typename Handler>
        std::size_t poll(Handler&& handler) {
            if (!connected())
                return 0;

            if (!connection_->flush() || !connection_->receive()) {
                connection_.reset();
                return 0;
            }

            std::size_t count = 0;
            const bool ok = connection_->for_each_frame(
                [&](net::FrameType type, std::span<const char> payload) {
                    if (type != net::FrameType::MESSAGE)
                        return net::FrameAction::PROTOCOL_ERROR;
                    handler(net::read_payload<protocol::ServerMessage>(payload));
                    ++count;
                    return net::FrameAction::CONSUMED;
                });

            if (!ok) {
                connection_.reset();
            }
            return count;
        }

        [[nodiscard]] bool connected() const noexcept {
            return connection_.has_value() && connection_->is_open();
        }

        [[nodiscard]] core::ParticipantId participant_id() const noexcept { return participant_id_; }

        // symbols the exchange trades (sent in LOGIN_ACK)
        [[nodiscard]] const std::vector<core::Symbol>& symbols() const noexcept { return symbols_; }

    private:
        std::optional<net::Connection> connection_;
        core::ParticipantId participant_id_{ 0 };
        std::vector<core::Symbol> symbols_;
    };
}
