#pragma once

#include <algorithm>
#include <array>
#include <cstdint>
#include <string_view>
#include <type_traits>

#include "core/types.h"
#include "exchange/protocol.h"

// what actually travels over TCP.
//
// TCP is a byte stream: one send() can arrive as two recv()s, or two sends
// as one recv. so every message is a frame:
//
//   [ size: u16 | type: u8 | reserved: u8 ][ payload: `size` bytes ]
//
// payloads are the raw bytes of our trivially copyable structs. that is only
// valid between programs built by the same compiler on the same kind of
// machine (same endianness + padding), which is fine for this project.
// a real exchange spells out every field explicitly (SBE, FIX, ITCH...).
//
// session flow:
//   client -> LOGIN(name)
//   server -> LOGIN_ACK(participant id, tradable symbols)
//   client -> REQUEST ...         (any number)
//   server -> MESSAGE ...         (exec reports + market data)

namespace exchange::net {

    enum class FrameType : std::uint8_t {
        LOGIN = 1,
        LOGIN_ACK = 2,
        REQUEST = 3,
        MESSAGE = 4,
    };

    struct FrameHeader {
        std::uint16_t size;
        FrameType type;
        std::uint8_t reserved;
    };

    inline constexpr std::size_t MAX_NAME_LENGTH = 16;
    inline constexpr std::size_t MAX_SYMBOLS = 16;

    struct Login {
        std::array<char, MAX_NAME_LENGTH> name;
    };

    struct LoginAck {
        core::ParticipantId participant_id;
        std::uint32_t symbol_count;
        std::array<core::Symbol, MAX_SYMBOLS> symbols;
    };

    static_assert(sizeof(FrameHeader) == 4);
    static_assert(std::is_trivially_copyable_v<Login>);
    static_assert(std::is_trivially_copyable_v<LoginAck>);

    // payload type <-> frame type, checked at compile time
    template <typename T> struct FrameTypeOf;
    template <> struct FrameTypeOf<Login> { static constexpr FrameType value = FrameType::LOGIN; };
    template <> struct FrameTypeOf<LoginAck> { static constexpr FrameType value = FrameType::LOGIN_ACK; };
    template <> struct FrameTypeOf<protocol::ClientRequest> { static constexpr FrameType value = FrameType::REQUEST; };
    template <> struct FrameTypeOf<protocol::ServerMessage> { static constexpr FrameType value = FrameType::MESSAGE; };

    // so a bad peer can't make us read a frame into the wrong struct
    [[nodiscard]] constexpr std::size_t expected_size(FrameType type) noexcept {
        switch (type) {
        case FrameType::LOGIN: return sizeof(Login);
        case FrameType::LOGIN_ACK: return sizeof(LoginAck);
        case FrameType::REQUEST: return sizeof(protocol::ClientRequest);
        case FrameType::MESSAGE: return sizeof(protocol::ServerMessage);
        }
        return 0;
    }

    [[nodiscard]] inline Login make_login(std::string_view name) noexcept {
        Login login{};
        const std::size_t length = std::min(name.size(), MAX_NAME_LENGTH - 1);
        for (std::size_t index = 0; index < length; ++index) {
            login.name[index] = name[index];
        }
        return login;
    }

    [[nodiscard]] inline std::string_view login_name(const Login& login) noexcept {
        std::size_t length = 0;
        while (length < MAX_NAME_LENGTH && login.name[length] != '\0') {
            ++length;
        }
        return std::string_view(login.name.data(), length);
    }
}
