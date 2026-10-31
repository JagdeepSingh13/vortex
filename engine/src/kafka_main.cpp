#include <array>
#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdio>
#include <cstdint>
#include <cstdlib>
#include <format>
#include <memory>
#include <random>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include <librdkafka/rdkafka.h>

#include "core/clock.h"
#include "core/spsc_queue.h"
#include "core/types.h"
#include "exchange/exchange_core.h"
#include "kafka/latency.h"
#include "kafka/messages.h"
#include "kafka/order_decoder.h"
#include "kafka/rdkafka.h"
#include "kafka/symbol_config.h"
#include "kafka/trade_encoder.h"
#include "matching/matching_engine.h"

// the engine as a Kafka service (ADR 0014):
//
//   Kafka "orders" -> [consumer] --SPSC--> [engine] --SPSC--> [publisher] -> Kafka "trades"
//                      parse JSON           match              format JSON
//                      ticks -> units       (no I/O, no         units -> ticks
//                                            allocation)        produce, key = symbol
//
// each SPSC queue has exactly one writer and one reader thread. the engine thread only
// pops, calls ExchangeCore::handle and pushes; everything slow happens around it.
//
// usage: vortex_engine [--bootstrap host:port] [--symbols file] [--orders topic] [--trades topic]
//                      [--warmup-seconds n]   latency samples skip the first n s (default 5)
//        env KAFKA_BOOTSTRAP overrides the default bootstrap (kafka:29092)

using namespace exchange;

namespace {

    // a few hundred thousand resting orders per book; ~45 MB per symbol
    using Engine = matching::MatchingEngine<131'072, 16'384, 8'192, 16'384>;
    using Core = gateway::ExchangeCore<Engine>;
    using InboundQueue = core::SPSCQueue<kafka::InboundRequest, 65'536>;
    using OutboundQueue = core::SPSCQueue<kafka::OutboundTrade, 65'536>;

    struct Options {
        std::string bootstrap = "kafka:29092";
        std::string symbols = "config/symbols.yaml";
        std::string orders_topic = "orders";
        std::string trades_topic = "trades";
        std::string group = "vortex-engine";
    };

    // each counter has one writer thread; main reads them for the stats line
    struct Stats {
        std::atomic<std::uint64_t> consumed{ 0 };
        std::atomic<std::uint64_t> decode_errors{ 0 };
        std::atomic<std::uint64_t> requests{ 0 };
        std::atomic<std::uint64_t> trades{ 0 };
        std::atomic<std::uint64_t> published{ 0 };
        std::atomic<std::uint64_t> delivered{ 0 };
        std::atomic<std::uint64_t> delivery_errors{ 0 };
        std::atomic<int> assigned_partitions{ 0 };
    };

    std::atomic<bool> g_stop{ false };
    std::atomic<bool> g_consumer_done{ false };
    std::atomic<bool> g_engine_done{ false };

    // latency samples only count for orders created after a warm-up window that starts at
    // the first order seen (clients connecting, idempotence ids, caches); 0 = keep everything.
    // set once by the consumer thread, read by the publisher thread
    std::atomic<core::Timestamp> g_measure_from_ns{ 0 };
    core::Timestamp g_warmup_ns = 5'000'000'000ULL;

    bool counts_for_latency(core::Timestamp order_time_ns) {
        const core::Timestamp from = g_measure_from_ns.load(std::memory_order_relaxed);
        return from != 0 && order_time_ns >= from;
    }

    void on_signal(int) { g_stop.store(true); }

    void relaxed_add(std::atomic<std::uint64_t>& counter) {
        counter.fetch_add(1, std::memory_order_relaxed);
    }

    std::string random_run_id() {
        std::random_device device;
        const std::uint64_t value = (static_cast<std::uint64_t>(device()) << 32) | device();
        return std::format("{:016x}", value);
    }

    // ---------------- consumer thread ----------------

    void on_rebalance(rd_kafka_t* client, rd_kafka_resp_err_t error,
        rd_kafka_topic_partition_list_t* partitions, void* opaque)
    {
        auto& stats = *static_cast<Stats*>(opaque);
        if (error == RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS) {
            rd_kafka_assign(client, partitions);
            stats.assigned_partitions.store(partitions->cnt);
            std::printf("consumer: assigned %d partition(s), reading new orders from the end\n", partitions->cnt);
        }
        else {
            rd_kafka_assign(client, nullptr);
            stats.assigned_partitions.store(0);
            std::printf("consumer: partitions revoked\n");
        }
        std::fflush(stdout);
    }

    void run_consumer(const Options& options, const std::vector<kafka::SymbolConfig>& symbols,
        InboundQueue& inbound, Stats& stats, kafka::LatencyRecorder& ingest)
    {
        rd_kafka_conf_t* conf = kafka::make_conf({
            { "bootstrap.servers", options.bootstrap },
            { "group.id", options.group },
            { "client.id", "vortex-engine-consumer" },
            // every engine start is a new trading session: no committed offsets, so it
            // starts at the end of `orders` (resting orders do not survive a restart)
            { "auto.offset.reset", "latest" },
            { "enable.auto.commit", "false" },
            });
        rd_kafka_conf_set_rebalance_cb(conf, on_rebalance);
        rd_kafka_conf_set_opaque(conf, &stats);

        kafka::Client client(RD_KAFKA_CONSUMER, conf);
        rd_kafka_poll_set_consumer(client.get());

        rd_kafka_topic_partition_list_t* topics = rd_kafka_topic_partition_list_new(1);
        rd_kafka_topic_partition_list_add(topics, options.orders_topic.c_str(), RD_KAFKA_PARTITION_UA);
        if (const auto error = rd_kafka_subscribe(client.get(), topics); error != RD_KAFKA_RESP_ERR_NO_ERROR) {
            kafka::kafka_fatal(std::string("subscribe: ") + rd_kafka_err2str(error));
        }
        rd_kafka_topic_partition_list_destroy(topics);

        kafka::OrderDecoder decoder(symbols);
        std::uint64_t reported_errors = 0;

        while (!g_stop.load(std::memory_order_relaxed)) {
            rd_kafka_message_t* message = rd_kafka_consumer_poll(client.get(), 100);
            if (message == nullptr) {
                continue;
            }
            if (message->err != RD_KAFKA_RESP_ERR_NO_ERROR) {
                if (message->err != RD_KAFKA_RESP_ERR__PARTITION_EOF) {
                    std::fprintf(stderr, "consumer: %s\n", rd_kafka_message_errstr(message));
                }
                rd_kafka_message_destroy(message);
                continue;
            }

            const core::Timestamp received = core::wall_clock_ns();
            kafka::DecodeError error = kafka::DecodeError::NONE;
            const std::string_view payload(static_cast<const char*>(message->payload), message->len);
            const auto request = decoder.decode(payload, received, error);
            rd_kafka_message_destroy(message);

            if (!request) {
                relaxed_add(stats.decode_errors);
                if (reported_errors++ < 5) {
                    std::fprintf(stderr, "consumer: skipped message (%s)\n", kafka::to_string(error));
                }
                continue;
            }

            // producer clock -> our clock: Kafka transit including the producer's batching
            if (g_measure_from_ns.load(std::memory_order_relaxed) == 0) {
                g_measure_from_ns.store(request->client_time_ns + g_warmup_ns);
            }
            if (counts_for_latency(request->client_time_ns)) {
                ingest.record(static_cast<std::int64_t>(received - request->client_time_ns) / 1000);
            }

            // queue full = the engine is behind: wait (backpressure), never drop an order
            while (!inbound.push(*request)) {
                if (g_stop.load(std::memory_order_relaxed)) {
                    break;
                }
                std::this_thread::yield();
            }
            relaxed_add(stats.consumed);
        }

        rd_kafka_consumer_close(client.get());
        g_consumer_done.store(true);
    }

    // ---------------- engine thread ----------------

    // what ExchangeCore talks to. only trades leave the engine in M5; private execution
    // reports and book updates have no Kafka topic yet, so they are ignored here
    struct KafkaSink {
        OutboundQueue& outbound;
        Stats& stats;
        // set by the engine loop before each request: every trade that request causes
        // (including triggered stops) carries it, for the end-to-end measurement
        core::Timestamp order_time_ns{ 0 };

        void send(core::ParticipantId, const protocol::ServerMessage&) {}
        void broadcast(const protocol::ServerMessage&) {}

        void trade(const matching::Trade& trade, core::Timestamp event_time_ns) {
            const kafka::OutboundTrade out{
                .trade = trade, .event_time_ns = event_time_ns, .order_time_ns = order_time_ns };
            // a trade is never dropped: if the publisher is behind, the engine waits
            while (!outbound.push(out)) {
                std::this_thread::yield();
            }
            relaxed_add(stats.trades);
        }
    };

    void run_engine(Core& core, InboundQueue& inbound, OutboundQueue& outbound, Stats& stats) {
        KafkaSink sink{ outbound, stats };
        kafka::InboundRequest request{};

        while (true) {
            if (inbound.pop(request)) {
                sink.order_time_ns = request.client_time_ns;
                core.handle(request.participant, request.request, sink);
                relaxed_add(stats.requests);
                continue;
            }
            // stop only once the consumer is gone and every order it handed over is matched
            if (g_consumer_done.load() && inbound.empty()) {
                break;
            }
            std::this_thread::yield();
        }
        g_engine_done.store(true);
    }

    // ---------------- publisher thread ----------------

    struct DeliveryContext {
        Stats* stats;
        kafka::LatencyRecorder* delivery;
        kafka::LatencyRecorder* end_to_end;
    };

    // called from rd_kafka_poll/flush, i.e. on the publisher thread itself
    void on_delivery(rd_kafka_t*, const rd_kafka_message_t* message, void* opaque) {
        auto& context = *static_cast<DeliveryContext*>(opaque);
        if (message->err != RD_KAFKA_RESP_ERR_NO_ERROR) {
            relaxed_add(context.stats->delivery_errors);
            std::fprintf(stderr, "publisher: delivery failed: %s\n", rd_kafka_err2str(message->err));
            return;
        }
        relaxed_add(context.stats->delivered);
        // the order's time travelled with the message as librdkafka's per-message opaque
        const auto order_time = static_cast<core::Timestamp>(reinterpret_cast<std::uintptr_t>(message->_private));
        if (counts_for_latency(order_time)) {
            // produce() -> broker acknowledged, measured by librdkafka
            context.delivery->record(rd_kafka_message_latency(message));
            // order created by its producer -> resulting trade acknowledged by the broker
            context.end_to_end->record(static_cast<std::int64_t>(core::wall_clock_ns() - order_time) / 1000);
        }
    }

    void run_publisher(const Options& options, const std::vector<kafka::SymbolConfig>& symbols,
        const std::string& run_id, OutboundQueue& outbound, Stats& stats,
        kafka::LatencyRecorder& queue_lag, kafka::LatencyRecorder& delivery, kafka::LatencyRecorder& end_to_end)
    {
        DeliveryContext context{ &stats, &delivery, &end_to_end };
        rd_kafka_conf_t* conf = kafka::make_conf({
            { "bootstrap.servers", options.bootstrap },
            { "client.id", "vortex-engine-publisher" },
            { "acks", "all" },
            { "enable.idempotence", "true" },
            { "linger.ms", "5" },
            { "compression.type", "lz4" },
            // same partition for a symbol as the Java client and the simulator (ADR 0013)
            { "partitioner", "murmur2_random" },
            });
        rd_kafka_conf_set_dr_msg_cb(conf, on_delivery);
        rd_kafka_conf_set_opaque(conf, &context);
        kafka::Client client(RD_KAFKA_PRODUCER, conf);

        kafka::TradeEncoder encoder(symbols, run_id);
        kafka::OutboundTrade trade{};

        while (true) {
            if (!outbound.pop(trade)) {
                if (g_engine_done.load() && outbound.empty()) {
                    break;
                }
                rd_kafka_poll(client.get(), 0);
                // idle: a short sleep instead of spinning keeps a laptop core free;
                // it adds at most ~50 us before an idle publisher sees the next trade
                std::this_thread::sleep_for(std::chrono::microseconds(50));
                continue;
            }

            // engine matched it -> publisher picks it up: time spent in the outbound queue
            if (counts_for_latency(trade.order_time_ns)) {
                queue_lag.record(static_cast<std::int64_t>(core::wall_clock_ns() - trade.event_time_ns) / 1000);
            }

            std::string_view symbol;
            const std::string_view json = encoder.encode(trade, symbol);
            if (json.empty()) {
                std::fprintf(stderr, "publisher: could not encode trade %llu\n",
                    static_cast<unsigned long long>(trade.trade.match_id));
                continue;
            }

            while (true) {
                const rd_kafka_resp_err_t error = rd_kafka_producev(client.get(),
                    RD_KAFKA_V_TOPIC(options.trades_topic.c_str()),
                    RD_KAFKA_V_KEY(symbol.data(), symbol.size()),
                    RD_KAFKA_V_VALUE(const_cast<char*>(json.data()), json.size()),
                    RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
                    RD_KAFKA_V_OPAQUE(reinterpret_cast<void*>(static_cast<std::uintptr_t>(trade.order_time_ns))),
                    RD_KAFKA_V_END);
                if (error == RD_KAFKA_RESP_ERR_NO_ERROR) {
                    break;
                }
                if (error != RD_KAFKA_RESP_ERR__QUEUE_FULL) {
                    kafka::kafka_fatal(std::string("produce: ") + rd_kafka_err2str(error));
                }
                rd_kafka_poll(client.get(), 10);  // local queue full: let deliveries drain
            }
            relaxed_add(stats.published);
            rd_kafka_poll(client.get(), 0);
        }

        if (rd_kafka_flush(client.get(), 30'000) != RD_KAFKA_RESP_ERR_NO_ERROR) {
            std::fprintf(stderr, "publisher: %d message(s) not delivered at shutdown\n",
                rd_kafka_outq_len(client.get()));
        }
    }

    Options parse_options(int argc, char** argv) {
        Options options;
        if (const char* bootstrap = std::getenv("KAFKA_BOOTSTRAP"); bootstrap != nullptr && *bootstrap != '\0') {
            options.bootstrap = bootstrap;
        }
        for (int index = 1; index + 1 < argc; index += 2) {
            const std::string_view flag = argv[index];
            if (flag == "--bootstrap") options.bootstrap = argv[index + 1];
            else if (flag == "--symbols") options.symbols = argv[index + 1];
            else if (flag == "--orders") options.orders_topic = argv[index + 1];
            else if (flag == "--trades") options.trades_topic = argv[index + 1];
            else if (flag == "--warmup-seconds") g_warmup_ns = std::strtoull(argv[index + 1], nullptr, 10) * 1'000'000'000ULL;
            else {
                std::fprintf(stderr, "unknown option %s\n", argv[index]);
                std::exit(EXIT_FAILURE);
            }
        }
        return options;
    }
}

int main(int argc, char** argv) {
    const Options options = parse_options(argc, argv);
    std::signal(SIGINT, on_signal);
    std::signal(SIGTERM, on_signal);

    const std::vector<kafka::SymbolConfig> symbols = kafka::load_symbols(options.symbols);
    std::vector<core::Symbol> engine_symbols;
    std::string names;
    for (const auto& symbol : symbols) {
        engine_symbols.push_back(symbol.symbol);
        names += (names.empty() ? "" : ",") + symbol.name;
    }

    const std::string run_id = random_run_id();
    auto core = std::make_unique<Core>(engine_symbols);
    auto inbound = std::make_unique<InboundQueue>();
    auto outbound = std::make_unique<OutboundQueue>();
    Stats stats;
    kafka::LatencyRecorder ingest;
    kafka::LatencyRecorder queue_lag;
    kafka::LatencyRecorder delivery;
    kafka::LatencyRecorder end_to_end;

    std::printf("vortex engine run %s: %s -> %s via %s, symbols %s, librdkafka %s\n",
        run_id.c_str(), options.orders_topic.c_str(), options.trades_topic.c_str(),
        options.bootstrap.c_str(), names.c_str(), rd_kafka_version_str());
    std::fflush(stdout);

    std::thread publisher([&] { run_publisher(options, symbols, run_id, *outbound, stats, queue_lag, delivery, end_to_end); });
    std::thread engine([&] { run_engine(*core, *inbound, *outbound, stats); });
    std::thread consumer([&] { run_consumer(options, symbols, *inbound, stats, ingest); });

    // stats every 5 s until Ctrl+C / docker stop
    auto next = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    std::uint64_t last_requests = 0;
    std::uint64_t last_trades = 0;
    while (!g_stop.load()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        if (std::chrono::steady_clock::now() < next) {
            continue;
        }
        next += std::chrono::seconds(5);
        const std::uint64_t requests = stats.requests.load();
        const std::uint64_t trades = stats.trades.load();
        std::printf("orders %llu (+%llu/s)  trades %llu (+%llu/s)  delivered %llu  decode errors %llu  "
            "delivery errors %llu  partitions %d\n",
            static_cast<unsigned long long>(requests), static_cast<unsigned long long>((requests - last_requests) / 5),
            static_cast<unsigned long long>(trades), static_cast<unsigned long long>((trades - last_trades) / 5),
            static_cast<unsigned long long>(stats.delivered.load()),
            static_cast<unsigned long long>(stats.decode_errors.load()),
            static_cast<unsigned long long>(stats.delivery_errors.load()),
            stats.assigned_partitions.load());
        std::fflush(stdout);
        last_requests = requests;
        last_trades = trades;
    }

    std::printf("stopping: consumer, then engine, then publisher (flush)...\n");
    std::fflush(stdout);
    consumer.join();
    engine.join();
    publisher.join();

    // one line each, parsed by the end-to-end benchmark
    std::printf("SUMMARY run=%s orders=%llu trades=%llu delivered=%llu decode_errors=%llu delivery_errors=%llu\n",
        run_id.c_str(), static_cast<unsigned long long>(stats.requests.load()),
        static_cast<unsigned long long>(stats.trades.load()), static_cast<unsigned long long>(stats.delivered.load()),
        static_cast<unsigned long long>(stats.decode_errors.load()),
        static_cast<unsigned long long>(stats.delivery_errors.load()));
    std::printf("SUMMARY latency samples exclude orders created in the first %llu s\n",
        static_cast<unsigned long long>(g_warmup_ns / 1'000'000'000ULL));
    std::printf("SUMMARY ingest (order created -> engine consumer) %s\n", ingest.summary().c_str());
    std::printf("SUMMARY outbound queue (matched -> publisher) %s\n", queue_lag.summary().c_str());
    std::printf("SUMMARY delivery (produce -> broker ack) %s\n", delivery.summary().c_str());
    std::printf("SUMMARY end-to-end (order created -> its trade acked by broker) %s\n", end_to_end.summary().c_str());
    return EXIT_SUCCESS;
}
