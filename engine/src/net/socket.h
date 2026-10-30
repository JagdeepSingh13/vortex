#pragma once

#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <string>

// thin wrapper over the OS socket api so the rest of the code
// doesn't care whether it runs on Winsock or POSIX.
// every socket used by the exchange is non-blocking.

#ifdef _WIN32
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <winsock2.h>
#include <ws2tcpip.h>
#else
#include <arpa/inet.h>
#include <cerrno>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>
#endif

namespace exchange::net {

#ifdef _WIN32
    using socket_t = SOCKET;
    inline constexpr socket_t INVALID_SOCKET_VALUE = INVALID_SOCKET;
#else
    using socket_t = int;
    inline constexpr socket_t INVALID_SOCKET_VALUE = -1;
#endif

    // Winsock must be started once per process before any socket call
    class NetworkInit {
    public:
        NetworkInit() {
#ifdef _WIN32
            WSADATA data{};
            ok_ = WSAStartup(MAKEWORD(2, 2), &data) == 0;
#endif
        }

        ~NetworkInit() {
#ifdef _WIN32
            if (ok_)
                WSACleanup();
#endif
        }

        NetworkInit(const NetworkInit&) = delete;
        NetworkInit& operator=(const NetworkInit&) = delete;

        [[nodiscard]] bool ok() const noexcept { return ok_; }

    private:
        bool ok_{ true };
    };

    inline void close_socket(socket_t socket) noexcept {
        if (socket == INVALID_SOCKET_VALUE)
            return;
#ifdef _WIN32
        closesocket(socket);
#else
        ::close(socket);
#endif
    }

    inline bool set_non_blocking(socket_t socket) noexcept {
#ifdef _WIN32
        u_long mode = 1;
        return ioctlsocket(socket, FIONBIO, &mode) == 0;
#else
        const int flags = fcntl(socket, F_GETFL, 0);
        return flags >= 0 && fcntl(socket, F_SETFL, flags | O_NONBLOCK) == 0;
#endif
    }

    // disable Nagle: send small messages right away instead of batching them
    inline void set_no_delay(socket_t socket) noexcept {
        int enable = 1;
        setsockopt(socket, IPPROTO_TCP, TCP_NODELAY,
            reinterpret_cast<const char*>(&enable), sizeof(enable));
    }

    [[nodiscard]] inline bool last_error_would_block() noexcept {
#ifdef _WIN32
        const int error = WSAGetLastError();
        return error == WSAEWOULDBLOCK || error == WSAEINPROGRESS;
#else
        return errno == EAGAIN || errno == EWOULDBLOCK || errno == EINPROGRESS;
#endif
    }

    // listening socket on all interfaces, non-blocking
    [[nodiscard]] inline socket_t listen_on(std::uint16_t port) noexcept {
        const socket_t server = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
        if (server == INVALID_SOCKET_VALUE)
            return INVALID_SOCKET_VALUE;

        int reuse = 1;
        setsockopt(server, SOL_SOCKET, SO_REUSEADDR,
            reinterpret_cast<const char*>(&reuse), sizeof(reuse));

        sockaddr_in address{};
        address.sin_family = AF_INET;
        address.sin_addr.s_addr = htonl(INADDR_ANY);
        address.sin_port = htons(port);

        if (::bind(server, reinterpret_cast<const sockaddr*>(&address), sizeof(address)) != 0 ||
            ::listen(server, SOMAXCONN) != 0 || !set_non_blocking(server)) {
            close_socket(server);
            return INVALID_SOCKET_VALUE;
        }
        return server;
    }

    // returns a new client socket, or INVALID_SOCKET_VALUE if nobody is waiting
    [[nodiscard]] inline socket_t accept_client(socket_t server) noexcept {
        const socket_t client = ::accept(server, nullptr, nullptr);
        if (client == INVALID_SOCKET_VALUE)
            return INVALID_SOCKET_VALUE;

        set_non_blocking(client);
        set_no_delay(client);
        return client;
    }

    // blocking connect (resolves names like "localhost"), then switches to non-blocking
    [[nodiscard]] inline socket_t connect_to(const std::string& host, std::uint16_t port) noexcept {
        addrinfo hints{};
        hints.ai_family = AF_INET;
        hints.ai_socktype = SOCK_STREAM;
        hints.ai_protocol = IPPROTO_TCP;

        addrinfo* results = nullptr;
        const std::string service = std::to_string(port);
        if (getaddrinfo(host.c_str(), service.c_str(), &hints, &results) != 0)
            return INVALID_SOCKET_VALUE;

        socket_t client = INVALID_SOCKET_VALUE;
        for (addrinfo* entry = results; entry != nullptr; entry = entry->ai_next) {
            client = ::socket(entry->ai_family, entry->ai_socktype, entry->ai_protocol);
            if (client == INVALID_SOCKET_VALUE)
                continue;
            if (::connect(client, entry->ai_addr, static_cast<int>(entry->ai_addrlen)) == 0)
                break;
            close_socket(client);
            client = INVALID_SOCKET_VALUE;
        }
        freeaddrinfo(results);

        if (client != INVALID_SOCKET_VALUE) {
            set_non_blocking(client);
            set_no_delay(client);
        }
        return client;
    }

    // > 0: bytes moved, 0: would block (try later), < 0: connection closed or broken
    [[nodiscard]] inline long receive_some(socket_t socket, char* data, std::size_t size) noexcept {
        const auto received = ::recv(socket, data, static_cast<int>(size), 0);
        if (received > 0)
            return static_cast<long>(received);
        if (received == 0)
            return -1;  // orderly shutdown by the peer
        return last_error_would_block() ? 0 : -1;
    }

    [[nodiscard]] inline long send_some(socket_t socket, const char* data, std::size_t size) noexcept {
#ifdef _WIN32
        const int flags = 0;
#else
        const int flags = MSG_NOSIGNAL;  // no SIGPIPE if the peer is gone
#endif
        const auto sent = ::send(socket, data, static_cast<int>(size), flags);
        if (sent >= 0)
            return static_cast<long>(sent);
        return last_error_would_block() ? 0 : -1;
    }
}
