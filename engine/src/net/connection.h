#pragma once

#include <algorithm>
#include <cstring>
#include <span>
#include <utility>
#include <vector>

#include "net/socket.h"
#include "net/wire.h"

// one non-blocking TCP connection with an input and an output buffer.
// used on both sides: the gateway keeps one per client, a client keeps one.
//
// input:  receive() appends whatever bytes arrived. for_each_frame() hands out
//         every *complete* frame; a half-arrived frame stays for next time.
// output: queue() appends a frame, flush() sends as much as the kernel takes.
//         nothing ever blocks; leftovers wait in the buffer.

namespace exchange::net {

    enum class FrameAction {
        CONSUMED,   // frame handled, move on
        STOP,       // can't take it right now, keep it and stop reading (backpressure)
        PROTOCOL_ERROR,  // peer broke the protocol
    };

    class Connection {
        static constexpr std::size_t READ_CHUNK = 64 * 1024;

    public:

        explicit Connection(socket_t socket) : socket_(socket) {}

        ~Connection() { close(); }

        Connection(const Connection&) = delete;
        Connection& operator=(const Connection&) = delete;

        Connection(Connection&& other) noexcept
            : socket_(std::exchange(other.socket_, INVALID_SOCKET_VALUE)),
            input_(std::move(other.input_)), output_(std::move(other.output_)),
            output_offset_(other.output_offset_)
        {
        }

        Connection& operator=(Connection&&) = delete;

        [[nodiscard]] bool is_open() const noexcept { return socket_ != INVALID_SOCKET_VALUE; }

        void close() noexcept {
            close_socket(socket_);
            socket_ = INVALID_SOCKET_VALUE;
        }

        // false = peer closed or connection broken
        bool receive() {
            if (!is_open())
                return false;

            while (true) {
                const std::size_t old_size = input_.size();
                input_.resize(old_size + READ_CHUNK);
                const long received = receive_some(socket_, input_.data() + old_size, READ_CHUNK);
                input_.resize(old_size + static_cast<std::size_t>(std::max(received, 0L)));

                if (received < 0)
                    return false;
                // drained everything the kernel had
                if (received < static_cast<long>(READ_CHUNK))
                    return true;
            }
        }

        // handler(FrameType, std::span<const char> payload) -> FrameAction
        // returns false if the peer sent garbage
        template <typename Handler>
        bool for_each_frame(Handler&& handler) {
            std::size_t offset = 0;
            bool ok = true;

            while (input_.size() - offset >= sizeof(FrameHeader)) {
                FrameHeader header{};
                std::memcpy(&header, input_.data() + offset, sizeof(header));

                if (header.size != expected_size(header.type)) {
                    ok = false;
                    break;
                }
                if (input_.size() - offset < sizeof(header) + header.size)
                    break;  // rest of this frame hasn't arrived yet

                const std::span<const char> payload(input_.data() + offset + sizeof(header), header.size);
                const FrameAction action = handler(header.type, payload);
                if (action == FrameAction::PROTOCOL_ERROR) {
                    ok = false;
                    break;
                }
                if (action == FrameAction::STOP)
                    break;

                offset += sizeof(header) + header.size;
            }

            input_.erase(input_.begin(), input_.begin() + static_cast<std::ptrdiff_t>(offset));
            return ok;
        }

        template <typename T>
        void queue(const T& value) {
            static_assert(std::is_trivially_copyable_v<T>);
            const FrameHeader header{
                .size = static_cast<std::uint16_t>(sizeof(T)),
                .type = FrameTypeOf<T>::value,
                .reserved = 0,
            };

            const std::size_t old_size = output_.size();
            output_.resize(old_size + sizeof(header) + sizeof(T));
            std::memcpy(output_.data() + old_size, &header, sizeof(header));
            std::memcpy(output_.data() + old_size + sizeof(header), &value, sizeof(T));
        }

        // false = connection broken
        bool flush() {
            if (!is_open())
                return false;

            while (output_offset_ < output_.size()) {
                const long sent = send_some(socket_, output_.data() + output_offset_,
                    output_.size() - output_offset_);
                if (sent < 0)
                    return false;
                if (sent == 0)
                    break;  // kernel buffer full, try again later
                output_offset_ += static_cast<std::size_t>(sent);
            }

            if (output_offset_ == output_.size()) {
                output_.clear();
                output_offset_ = 0;
            }
            return true;
        }

        [[nodiscard]] std::size_t pending_output() const noexcept {
            return output_.size() - output_offset_;
        }

    private:
        socket_t socket_{ INVALID_SOCKET_VALUE };
        std::vector<char> input_;
        std::vector<char> output_;
        std::size_t output_offset_{ 0 };
    };

    template <typename T>
    [[nodiscard]] T read_payload(std::span<const char> payload) noexcept {
        T value{};
        std::memcpy(&value, payload.data(), sizeof(T));
        return value;
    }
}
