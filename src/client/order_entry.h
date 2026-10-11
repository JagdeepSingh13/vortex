#pragma once

#include <cstdint>

#include "exchange/protocol.h"

// order entry helpers shared by every client transport.
//
// CRTP: a transport derives from OrderEntry<Itself> and provides
//     void transmit(const protocol::ClientRequest&)
// and gets send / send_limit / send_cancel / send_modify for free.
// the bots are templates over the client type, so the same bot code runs
// in-process (LocalClient) or over the network (TcpClient) with no virtual calls.

namespace exchange::client {

    template <typename Derived>
    class OrderEntry {
    public:

        [[nodiscard]] protocol::ClientOrderId next_client_order_id() noexcept {
            return next_client_order_id_++;
        }

        void send(const protocol::ClientRequest& request) {
            ++requests_sent_;
            static_cast<Derived&>(*this).transmit(request);
        }

        protocol::ClientOrderId send_limit(const core::Symbol& symbol, core::Side side,
            core::Price price, core::Quantity qty,
            core::OrderType type = core::OrderType::LIMIT,
            core::Price trigger_price = 0, core::Quantity display_qty = 0)
        {
            const protocol::ClientOrderId id = next_client_order_id();
            send(protocol::ClientRequest{
                .action = matching::RequestAction::ADD,
                .side = side,
                .order_type = type,
                .symbol = symbol,
                .client_order_id = id,
                .price = price,
                .qty = qty,
                .display_qty = display_qty,
                .trigger_price = trigger_price,
                });
            return id;
        }

        void send_cancel(const core::Symbol& symbol, protocol::ClientOrderId id) {
            send(protocol::ClientRequest{
                .action = matching::RequestAction::CANCEL,
                .symbol = symbol,
                .client_order_id = id,
                });
        }

        void send_modify(const core::Symbol& symbol, protocol::ClientOrderId id,
            core::Quantity new_qty, core::Price new_price)
        {
            send(protocol::ClientRequest{
                .action = matching::RequestAction::MODIFY,
                .symbol = symbol,
                .client_order_id = id,
                .price = new_price,
                .qty = new_qty,
                });
        }

        [[nodiscard]] std::uint64_t requests_sent() const noexcept { return requests_sent_; }

    private:
        protocol::ClientOrderId next_client_order_id_{ 1 };
        std::uint64_t requests_sent_{ 0 };
    };
}
