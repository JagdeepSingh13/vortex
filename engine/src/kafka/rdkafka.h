#pragma once

#include <cstdio>
#include <cstdlib>
#include <initializer_list>
#include <string>
#include <utility>

#include <librdkafka/rdkafka.h>

// thin RAII helpers over librdkafka's C API (no exceptions, explicit errors).

namespace exchange::kafka {

    [[noreturn]] inline void kafka_fatal(const std::string& what) {
        std::fprintf(stderr, "kafka: %s\n", what.c_str());
        std::exit(EXIT_FAILURE);
    }

    // builds a configuration from key/value pairs; any rejected setting is fatal
    inline rd_kafka_conf_t* make_conf(std::initializer_list<std::pair<const char*, std::string>> settings) {
        rd_kafka_conf_t* conf = rd_kafka_conf_new();
        char error[512];
        for (const auto& [key, value] : settings) {
            if (rd_kafka_conf_set(conf, key, value.c_str(), error, sizeof(error)) != RD_KAFKA_CONF_OK) {
                kafka_fatal(std::string("config ") + key + "=" + value + ": " + error);
            }
        }
        return conf;
    }

    // owns an rd_kafka_t; destroy() runs once, in the destructor
    class Client {
    public:
        Client(rd_kafka_type_t type, rd_kafka_conf_t* conf) {
            char error[512];
            handle_ = rd_kafka_new(type, conf, error, sizeof(error));  // takes ownership of conf
            if (handle_ == nullptr) {
                kafka_fatal(std::string("create client: ") + error);
            }
        }

        Client(const Client&) = delete;
        Client& operator=(const Client&) = delete;

        ~Client() {
            if (handle_ != nullptr) {
                rd_kafka_destroy(handle_);
            }
        }

        [[nodiscard]] rd_kafka_t* get() const noexcept { return handle_; }

    private:
        rd_kafka_t* handle_{ nullptr };
    };
}
