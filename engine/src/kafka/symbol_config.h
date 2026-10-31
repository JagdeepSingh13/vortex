#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "core/types.h"

// config/symbols.yaml -> what the Kafka edge needs to convert units (ADR 0004):
//   engine price  = price_ticks * price_per_tick     (price_per_tick = tick_size * engine scale)
//   engine qty    = qty_lots    * lot_size
// read once at startup, never on the matching thread.

namespace exchange::kafka {

    struct SymbolConfig {
        std::string name;
        core::Symbol symbol{};
        core::Price price_per_tick{ 0 };
        core::Quantity lot_size{ 0 };
    };

    // "0.01" * 10000 -> 100, exactly. decimal strings are parsed as integers so no binary
    // float is involved; nullopt if the result would not be a whole number
    [[nodiscard]] inline std::optional<std::int64_t> scale_decimal(std::string_view text, std::int64_t scale) {
        std::int64_t whole = 0;
        std::int64_t fraction = 0;
        std::int64_t fraction_scale = 1;
        bool after_point = false;
        for (const char c : text) {
            if (c == '.') {
                if (after_point) return std::nullopt;
                after_point = true;
            }
            else if (c >= '0' && c <= '9') {
                if (after_point) {
                    fraction = fraction * 10 + (c - '0');
                    fraction_scale *= 10;
                }
                else {
                    whole = whole * 10 + (c - '0');
                }
            }
            else {
                return std::nullopt;
            }
        }
        const std::int64_t scaled_fraction = fraction * scale;
        if (scaled_fraction % fraction_scale != 0) {
            return std::nullopt;  // e.g. tick 0.00001 with scale 10000: not a whole engine unit
        }
        return whole * scale + scaled_fraction / fraction_scale;
    }

    // reads and checks the file; on any problem prints why and exits.
    // implemented in symbol_config.cpp, the only file built with C++ exceptions
    // (yaml-cpp reports errors by throwing; the rest of the engine is -fno-exceptions)
    [[nodiscard]] std::vector<SymbolConfig> load_symbols(const std::string& path);
}
