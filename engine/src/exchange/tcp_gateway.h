#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <map>
#include <optional>
#include <span>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "exchange/protocol.h"
#include "exchange/session.h"
#include "net/connection.h"
#include "net/socket.h"
#include "net/wire.h"

// the gateway thread: sockets on one side, engine sessions on the other.
//
//   remote client <--TCP--> [ gateway thread ] <--SPSC--> [ engine thread ]
//
// the engine still only sees fixed Sessions, created at startup. the gateway
// owns a pool of them ("slots") and lends one to every new connection.
// for each slot the gateway is the only producer of requests and the only
// consumer of responses, so the SPSC rules still hold.
//
// slot lifecycle:
//
//   FREE --accept--> CONNECTED --socket closed--> DRAINING --all orders gone--> FREE
//
// DRAINING = cancel-on-disconnect: a client that vanishes must not leave
// orders in the book, and the slot's participant id must not be reused
// while orders under that id are still alive. so the gateway remembers each
// client's open orders and, on disconnect, cancels them on the client's behalf.

namespace exchange::gateway {

    class TcpGateway {
        // a client that falls this far behind on reading gets disconnected,
        // instead of making us buffer forever (or stall the engine)
        static constexpr std::size_t MAX_PENDING_OUTPUT = 8U * 1024U * 1024U;
        static constexpr int RESPONSE_BATCH = 1'024;
        static constexpr auto DRAIN_TIMEOUT = std::chrono::seconds(5);

    public:

        TcpGateway(std::vector<Session*> sessions, std::span<const core::Symbol> symbols) {
            for (Session* session : sessions) {
                slots_.push_back(Slot{ .session = session });
            }

            login_ack_.symbol_count = static_cast<std::uint32_t>(
                std::min(symbols.size(), net::MAX_SYMBOLS));
            for (std::size_t index = 0; index < login_ack_.symbol_count; ++index) {
                login_ack_.symbols[index] = symbols[index];
            }
        }

        ~TcpGateway() { net::close_socket(listener_); }

        TcpGateway(const TcpGateway&) = delete;
        TcpGateway& operator=(const TcpGateway&) = delete;

        bool listen(std::uint16_t port) {
            listener_ = net::listen_on(port);
            return listener_ != net::INVALID_SOCKET_VALUE;
        }

        void run(const std::atomic<bool>& running) {
            int idle_rounds = 0;

            while (running.load(std::memory_order_relaxed)) {
                bool did_work = accept_new_clients();

                for (Slot& slot : slots_) {
                    did_work |= read_from_client(slot);
                    did_work |= forward_to_client(slot);
                    if (slot.state == SlotState::DRAINING) {
                        did_work |= drain(slot);
                    }
                }

                // spin while busy, back off when nothing is happening
                if (did_work) {
                    idle_rounds = 0;
                }
                else if (++idle_rounds < 1'000) {
                    std::this_thread::yield();
                }
                else {
                    std::this_thread::sleep_for(std::chrono::milliseconds(1));
                }
            }

            for (Slot& slot : slots_) {
                slot.connection.reset();
            }
        }

        [[nodiscard]] std::size_t connected_clients() const noexcept {
            return connected_.load(std::memory_order_relaxed);
        }

    private:

        enum class SlotState : std::uint8_t {
            FREE,
            CONNECTED,
            DRAINING,
        };

        struct Slot {
            Session* session{ nullptr };
            SlotState state{ SlotState::FREE };
            std::optional<net::Connection> connection{};
            bool logged_in{ false };
            std::string name{};

            // ADD sent to the engine, no ACCEPTED/REJECTED yet
            std::unordered_set<protocol::ClientOrderId> pending_adds{};
            // accepted and not yet filled/canceled -> symbol (needed to cancel it)
            std::unordered_map<protocol::ClientOrderId, core::Symbol> open_orders{};
            // cancels we sent ourselves while draining
            std::unordered_set<protocol::ClientOrderId> cancel_sent{};
            std::chrono::steady_clock::time_point drain_started{};
        };

        bool accept_new_clients() {
            bool accepted_any = false;

            while (true) {
                const net::socket_t socket = net::accept_client(listener_);
                if (socket == net::INVALID_SOCKET_VALUE)
                    return accepted_any;

                accepted_any = true;
                Slot* slot = find_free_slot();
                if (slot == nullptr) {
                    std::printf("[gateway] connection refused: all %zu slots busy\n", slots_.size());
                    net::close_socket(socket);
                    continue;
                }

                slot->connection.emplace(socket);
                slot->state = SlotState::CONNECTED;
                slot->logged_in = false;
                slot->name.clear();
                connected_.fetch_add(1, std::memory_order_relaxed);
            }
        }

        [[nodiscard]] Slot* find_free_slot() noexcept {
            for (Slot& slot : slots_) {
                if (slot.state == SlotState::FREE)
                    return &slot;
            }
            return nullptr;
        }

        // socket -> frames -> engine request queue
        bool read_from_client(Slot& slot) {
            if (slot.state != SlotState::CONNECTED)
                return false;

            if (!slot.connection->receive()) {
                disconnect(slot, "closed by client");
                return true;
            }

            bool did_work = false;
            const bool ok = slot.connection->for_each_frame(
                [&](net::FrameType type, std::span<const char> payload) {
                    did_work = true;
                    return on_frame(slot, type, payload);
                });

            if (!ok) {
                disconnect(slot, "protocol error");
                return true;
            }
            return did_work;
        }

        net::FrameAction on_frame(Slot& slot, net::FrameType type, std::span<const char> payload) {
            if (type == net::FrameType::LOGIN && !slot.logged_in) {
                const auto login = net::read_payload<net::Login>(payload);
                slot.name = std::string(net::login_name(login));
                slot.logged_in = true;

                net::LoginAck ack = login_ack_;
                ack.participant_id = slot.session->participant_id;
                slot.connection->queue(ack);

                // book updates are only sent on change, so a late joiner
                // gets a snapshot of every book first
                for (const auto& [symbol, book] : latest_books_) {
                    slot.connection->queue(protocol::ServerMessage::make(book));
                }

                std::printf("[gateway] %s logged in as participant %u\n",
                    slot.name.c_str(), slot.session->participant_id);
                return net::FrameAction::CONSUMED;
            }

            if (type == net::FrameType::REQUEST && slot.logged_in) {
                const auto request = net::read_payload<protocol::ClientRequest>(payload);
                // engine queue full: leave the bytes in our buffer and stop reading.
                // the client's TCP window fills up and *it* slows down (backpressure)
                if (!slot.session->requests.push(request))
                    return net::FrameAction::STOP;

                if (request.action == matching::RequestAction::ADD) {
                    slot.pending_adds.insert(request.client_order_id);
                }
                return net::FrameAction::CONSUMED;
            }

            // request before login, double login, or a server-only frame type
            return net::FrameAction::PROTOCOL_ERROR;
        }

        // engine response queue -> track orders -> socket
        bool forward_to_client(Slot& slot) {
            bool did_work = false;
            const bool deliver = slot.state == SlotState::CONNECTED && slot.logged_in;

            protocol::ServerMessage message{};
            for (int count = 0; count < RESPONSE_BATCH && slot.session->responses.pop(message); ++count) {
                did_work = true;
                if (message.kind == protocol::MessageKind::EXECUTION) {
                    track(slot, message.payload.execution);
                }
                else if (message.kind == protocol::MessageKind::BOOK) {
                    remember_book(message.payload.book);
                }
                // free / draining / not logged in: market data is simply discarded
                if (deliver) {
                    slot.connection->queue(message);
                }
            }

            if (slot.state == SlotState::CONNECTED) {
                if (!slot.connection->flush()) {
                    disconnect(slot, "send failed");
                }
                else if (slot.connection->pending_output() > MAX_PENDING_OUTPUT) {
                    disconnect(slot, "too slow reading market data");
                }
            }
            return did_work;
        }

        // keep a live picture of which orders this client has in the book
        static void track(Slot& slot, const protocol::ExecutionReport& report) {
            using protocol::ExecType;
            const protocol::ClientOrderId id = report.client_order_id;

            switch (report.type) {
            case ExecType::ACCEPTED:
                slot.pending_adds.erase(id);
                slot.open_orders[id] = report.symbol;
                break;
            case ExecType::REJECTED:
                slot.pending_adds.erase(id);
                break;
            case ExecType::FILLED:
            case ExecType::CANCELED:
                // (a reprice emits CANCELED then ACCEPTED for the same id:
                //  erased here, re-added by the ACCEPTED right after)
                slot.open_orders.erase(id);
                slot.cancel_sent.erase(id);
                break;
            default:
                break;
            }
        }

        // every slot receives the same broadcast, keep whichever copy is newest
        void remember_book(const protocol::BookUpdate& book) {
            auto [it, inserted] = latest_books_.try_emplace(book.symbol, book);
            if (!inserted && book.sequence_number > it->second.sequence_number) {
                it->second = book;
            }
        }

        void disconnect(Slot& slot, const char* reason) {
            std::printf("[gateway] %s disconnected (%s), canceling %zu open order(s)\n",
                slot.name.empty() ? "client" : slot.name.c_str(), reason, slot.open_orders.size());

            slot.connection.reset();
            slot.state = SlotState::DRAINING;
            slot.drain_started = std::chrono::steady_clock::now();
            connected_.fetch_sub(1, std::memory_order_relaxed);
        }

        // cancel everything the client left behind; free the slot once it is all gone
        bool drain(Slot& slot) {
            bool did_work = false;

            for (const auto& [id, symbol] : slot.open_orders) {
                if (slot.cancel_sent.contains(id))
                    continue;
                const protocol::ClientRequest cancel{
                    .action = matching::RequestAction::CANCEL,
                    .symbol = symbol,
                    .client_order_id = id,
                };
                if (!slot.session->requests.push(cancel))
                    break;  // queue full, rest next round
                slot.cancel_sent.insert(id);
                did_work = true;
            }

            const bool clean = slot.open_orders.empty() && slot.pending_adds.empty();
            const bool timed_out = std::chrono::steady_clock::now() - slot.drain_started > DRAIN_TIMEOUT;

            if (clean || timed_out) {
                if (!clean) {
                    std::printf("[gateway] warning: slot %u freed with %zu order(s) unconfirmed\n",
                        slot.session->participant_id, slot.open_orders.size());
                }
                slot.pending_adds.clear();
                slot.open_orders.clear();
                slot.cancel_sent.clear();
                slot.state = SlotState::FREE;
                did_work = true;
            }
            return did_work;
        }

        std::vector<Slot> slots_;
        net::LoginAck login_ack_{};
        std::map<core::Symbol, protocol::BookUpdate, core::SymbolLess> latest_books_;
        net::socket_t listener_{ net::INVALID_SOCKET_VALUE };
        std::atomic<std::size_t> connected_{ 0 };
    };
}
