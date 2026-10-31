# 0014. The engine as a Kafka service

- Status: accepted
- Date: 2026-10-31
- Milestone: M5

## Context

The engine has to read `orders` from Kafka, match them, and publish `trades`, without breaking the hard rule that nothing new runs on the matching thread (no heap allocation, locks, logging or I/O). Engine changes must stay minimal (M2.5 deferred), and matching behaviour must not change without proof.

## Options

Thread layout:
1. **Three threads, two SPSC queues**: consumer (Kafka + JSON + unit conversion) → engine (matching only) → publisher (JSON + Kafka). Reuses `ExchangeCore` and `SPSCQueue`.
2. One thread doing everything: simplest, but Kafka I/O and JSON parsing would sit on the matching path.
3. Reuse the TCP-era `Session`/`EngineRunner`: they assume one queue per connected client; Kafka brings any number of participants through one stream.

Libraries (all from Ubuntu 24.04 packages, pinned):
- Kafka client: **librdkafka 2.3.0** (apt) vs building 2.16.0 from source (same as the Python simulator; slower image build).
- JSON: **simdjson 3.6.4** (fastest, on-demand API, error codes without exceptions) vs nlohmann/json 3.11.3 (easiest, allocates a tree per message) vs RapidJSON 1.1.0 (2016 release).
- Config: **yaml-cpp 0.8.0** to read `config/symbols.yaml` directly, rather than a hand-written parser or a second JSON copy of the config.

Trade contract: `trade.v1` needed `buy_participant_id` and `sell_participant_id`, since order ids are unique only per participant (ADR 0013). **Amend in place** (nothing produced or consumed it yet) vs bump to v2.

## Decision

- Option 1, in `engine/src/kafka_main.cpp` and `engine/src/kafka/`:
  - **consumer thread**: librdkafka consumer, simdjson decode, ticks/lots → engine units, push `InboundRequest`. Waits (never drops) if the queue is full.
  - **engine thread**: pop, `ExchangeCore::handle()`, push `OutboundTrade` through a new compile-time-optional `trade()` hook on the sink. No allocation, locks or I/O.
  - **publisher thread**: engine units → ticks, packed order id → (participant, order id), `trade.v1` JSON, produce with key = symbol, `acks=all`, idempotent, `linger.ms=5`, lz4, `partitioner=murmur2_random`.
  - Shutdown order: consumer stops → engine drains → publisher drains and flushes, so no accepted order's trade is lost.
- librdkafka 2.3.0, simdjson 3.6.4, yaml-cpp 0.8.0 (apt, pinned). yaml-cpp throws on errors; only `symbol_config.cpp` is compiled with `-fexceptions`, and it catches everything, so no exception reaches the `-fno-exceptions` engine.
- `aggressor_side` added to `matching::Trade` (one field, set in `emit_trade`).
- `trade.v1` amended in place with the two participant fields; Jagdeep approved.

## Proof that matching is unchanged

The golden replay test dumped its full output (300,000 requests, 889,167 lines) before and after the aggressor change. 65,034 lines changed, exactly the TRADE lines, and after removing ` aggr=...` the two dumps are byte-identical. The golden files were then regenerated.

## Consequences

- Each engine start is a **new trading session**: a fresh `engine_run_id`, no committed offsets, reading `orders` from the end. Resting orders do not survive a restart. Rebuilding the book by replaying `orders` (event sourcing) is possible later.
- Only trades leave the engine. Private execution reports (order accepted/rejected/filled) and book updates are dropped in the Kafka sink until a topic needs them (Order API, M8).
- Measurement lives where the data is: the engine records ingest, outbound queue, delivery and per-trade end-to-end latency (the order's creation time travels with the trade as librdkafka's per-message opaque). The trade checker verifies correctness after a run instead of measuring latency during it: running it alongside inflated p99 from ~50–140 ms to ~500–700 ms on this laptop.
- A known gap from M2 remains: the level pool aborts if a book needs more than 16,384 price levels; the simulator's prices stay near the mid, so it is not hit in practice.
