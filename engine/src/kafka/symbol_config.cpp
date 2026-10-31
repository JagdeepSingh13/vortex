#include "kafka/symbol_config.h"

#include <cstdio>
#include <cstdlib>
#include <exception>

#include <yaml-cpp/yaml.h>

// built with -fexceptions (see CMakeLists.txt): yaml-cpp throws on missing keys and bad
// values. every exception is caught here and turned into a clear startup error, so no
// exception ever crosses into the -fno-exceptions code that includes the header.

namespace exchange::kafka {

    namespace {
        [[noreturn]] void fail(const std::string& message) {
            std::fprintf(stderr, "symbols config: %s\n", message.c_str());
            std::exit(EXIT_FAILURE);
        }
    }

    std::vector<SymbolConfig> load_symbols(const std::string& path) {
        try {
            const YAML::Node root = YAML::LoadFile(path);
            const auto scale = root["engine_price_scale"].as<std::int64_t>();
            if (scale != core::PRICE_SCALE) {
                fail("engine_price_scale " + std::to_string(scale) + " does not match the engine's PRICE_SCALE");
            }

            std::vector<SymbolConfig> symbols;
            for (const YAML::Node& entry : root["symbols"]) {
                SymbolConfig config;
                config.name = entry["symbol"].as<std::string>();
                if (config.name.empty() || config.name.size() > config.symbol.size()) {
                    fail("symbol '" + config.name + "' must be 1-8 characters");
                }
                config.symbol = core::make_symbol(config.name);

                const auto per_tick = scale_decimal(entry["tick_size"].as<std::string>(), scale);
                if (!per_tick || *per_tick <= 0) {
                    fail(config.name + ": tick_size is not a whole number of engine units");
                }
                config.price_per_tick = *per_tick;
                config.lot_size = entry["lot_size"].as<core::Quantity>();
                if (config.lot_size == 0) {
                    fail(config.name + ": lot_size must be at least 1");
                }
                symbols.push_back(config);
            }
            if (symbols.empty()) {
                fail("no symbols in " + path);
            }
            return symbols;
        }
        catch (const std::exception& error) {
            fail(path + ": " + error.what());
        }
    }
}
