# Architecture

Status: event contracts are **v1** (M1); lakehouse table layouts are proposals until M6/M7.

## Data flow

```
simulator / Order API (Spring Boot) -> Kafka "orders" -> C++ matching engine -> Kafka "trades"
Kafka "trades" -> Redis live prices (read by the Order API)
Kafka "trades" -> Spark Structured Streaming -> Iceberg bronze/silver (S3) -> dbt gold -> dashboard
Airflow: dbt runs, data quality checks, Iceberg maintenance
```

| Component | Folder | Language | Role |
|-----------|--------|----------|------|
| Matching engine | `engine/` | C++20 | Price-time priority order books per symbol; emits trades |
| Simulator | `simulator/` | Python | Generates order flow |
| Order API | `api/` | Java / Spring Boot | Accepts orders; serves live prices and candles |
| Streaming | `streaming/` | PySpark | Kafka → Iceberg bronze and silver |
| Gold models | `dbt/` | SQL (dbt) | Candles, VWAP, daily stats |
| Orchestration | `airflow/` | Python | dbt runs, data quality, Iceberg maintenance |

## Engine

```
Kafka "orders" -> [consumer thread] --SPSC--> [engine thread] --SPSC--> [publisher thread] -> Kafka "trades"
                   simdjson decode,           ExchangeCore::handle       trade.v1 encode,
                   ticks -> engine units      MatchingEngine             engine units -> ticks
```

- `core/`: fixed-width types (`Price` = int64, `Quantity` = uint32, 8-byte `Symbol`), memory pool, SPSC queue, clocks.
- `matching/`: `MatchingEngine` routes by symbol to an `OrderBook`; books keep price levels with intrusive FIFO lists of pooled `Order`s. Supports LIMIT, MARKET, IOC, FOK, GTC, STOP, STOP_LIMIT, ICEBERG, POST_ONLY; cancel and modify. Events are a tagged union written into a preallocated buffer; no heap allocation on the matching path.
- `exchange/`: `ExchangeCore` validates requests, packs (participant, order id) into one 64-bit id, stamps UTC event time once per request (ADR 0009) and hands events to a sink; `protocol.h` holds the request and report structs.
- `kafka/` + `kafka_main.cpp`: the Kafka service (ADR 0014). Three threads, two SPSC queues, backpressure instead of drops, ordered shutdown. Units are converted only at the edges (ADR 0004). Each start is a new session with a fresh `engine_run_id`, reading `orders` from the end.
- The TCP gateway, bots and trader terminal from the original `cpp_exchange` were removed in M5 (ADR 0015).

Known gaps:
- Private execution reports and book updates are not published yet (no topic needs them before the Order API, M8).
- Resting orders do not survive an engine restart (no replay of `orders`).
- The level pool aborts if one book needs more than 16,384 price levels (not reached by the simulator).

## Kafka topics

| Topic | Key | Value | Producer | Consumers |
|-------|-----|-------|----------|-----------|
| `orders` | symbol | `order.v1` JSON | simulator, Order API | engine |
| `trades` | symbol | `trade.v1` JSON | engine | Redis updater (API), Spark bronze job |

Keying by symbol keeps per-symbol ordering within a partition. Both topics have 3 partitions (ADR 0012). All producers use Java-compatible murmur2 partitioning (librdkafka: `partitioner=murmur2_random`), so a symbol always lands on the same partition whoever sends it (ADR 0013): AAPL, MSFT → 0; GOOGL, NFLX → 1; AMZN, NVDA → 2.

## Units

`config/symbols.yaml` is the single source of truth (ADR 0004):

| Quantity | In events and bronze/silver | In gold and UI | Engine internal |
|---|---|---|---|
| Price | `price_ticks` (int) | `price_ticks * tick_size` (decimal) | `price_ticks * tick_size * 10000` (int64) |
| Quantity | `qty_lots` (int) | `qty_lots * lot_size` shares | `qty_lots * lot_size` (uint32) |
| Time | `*_time_ns` (int, UTC ns since epoch) | timestamp | steady clock for latency; epoch for `event_time_ns` (M2) |

## Event contracts (v1)

Defined as JSON Schema draft 2020-12 in `schemas/`; examples in `schemas/examples/`; tests in `schemas/tests/` (`make test-schemas`). All objects reject undeclared fields. Integers are strict: `18990.0` is invalid (ADR 0007).

Integer ranges:
- ids, prices, sequence numbers: 1 to 2^53 - 1, so JavaScript reads them exactly.
- quantities: 1 to 2^32 - 1 (engine `Quantity` is uint32).
- `*_time_ns`: from 10^18 (2001-09-09) to 2^63 - 1. This range rejects monotonic-clock values and milliseconds by mistake. It exceeds 2^53, so JavaScript consumers must read these fields as BigInt or string.

### `order.v1` ([schemas/order.v1.json](../schemas/order.v1.json))

| Field | Type | Required | Notes |
|-------|------|----------|-------|
| `schema_version` | const `1` | always | |
| `order_id` | 1..2^40-1 | always | unique per participant; for CANCEL/MODIFY the order to change |
| `symbol` | string `^[A-Z][A-Z0-9.]{0,7}$` | always | must exist in `config/symbols.yaml` |
| `action` | `ADD` / `CANCEL` / `MODIFY` | always | |
| `participant_id` | 1..2^24-1 | always | the engine identifies an order by (`participant_id`, `order_id`) |
| `client_time_ns` | epoch ns | always | producer clock, informational |
| `side` | `BUY` / `SELL` | ADD | |
| `order_type` | `LIMIT` `MARKET` `IOC` `FOK` `GTC` `STOP` `STOP_LIMIT` `ICEBERG` `POST_ONLY` | ADD | |
| `qty_lots` | 1..2^32-1 | ADD, MODIFY | |
| `price_ticks` | safe int | ADD (not MARKET, STOP), MODIFY | forbidden for MARKET and STOP |
| `trigger_price_ticks` | safe int | STOP, STOP_LIMIT | forbidden otherwise |
| `display_qty_lots` | 1..2^32-1 | ICEBERG | forbidden otherwise; must be ≤ `qty_lots` (engine check) |

CANCEL carries only the always-required fields. MODIFY carries those plus `price_ticks` and `qty_lots`. These rules mirror `OrderBook::validate_new_order` and `modify_order`. Business rules that depend on book state (MARKET into an empty book, FOK liquidity, POST_ONLY crossing, duplicate ids) are left to the engine, which emits a reject.

### `trade.v1` ([schemas/trade.v1.json](../schemas/trade.v1.json))

| Field | Type | Notes |
|-------|------|-------|
| `schema_version` | const `1` | |
| `engine_run_id` | string `^[0-9a-f]{16}$` | random per engine start (ADR 0005) |
| `match_id` | safe int | counted per symbol, restarts each run; unique key = (`engine_run_id`, `symbol`, `match_id`) |
| `symbol` | string | as above |
| `price_ticks` | safe int | |
| `qty_lots` | 1..2^32-1 | |
| `buy_participant_id`, `sell_participant_id` | 1..2^24-1 | who bought and who sold |
| `buy_order_id`, `sell_order_id` | 1..2^40-1 | the producers' order ids; (participant, order id) identifies an order |
| `aggressor_side` | `BUY` / `SELL` | side of the incoming order (ADR 0006) |
| `sequence_number` | safe int | engine event sequence, increasing within a run |
| `event_time_ns` | epoch ns | set by the engine |

All fields are required.

## Lakehouse tables (proposed)

- `bronze.trades`: every field of `trade.v1` as received, plus `kafka_partition`, `kafka_offset`, `ingest_time_ns`. Append-only.
- `silver.trades`: deduplicated on (`engine_run_id`, `symbol`, `match_id`), partitioned by `symbol` and `days(event_time)`. Integers only.
- `gold.*` (dbt): `candles_1m`, `vwap`, `daily_symbol_stats`. Decimal prices derived from `config/symbols.yaml`.

## Schema change log

- 2026-10-31 — `order.v1` and `trade.v1` created (M1).
- 2026-10-31 — `order.v1` amended in place, no version bump (M4, ADR 0013): `order_id` is unique **per participant** (was: per engine run) and limited to 2^40 − 1; `participant_id` limited to 2^24 − 1, matching the engine's 24/40-bit order id packing. Shape unchanged. Normally a schema change needs a version bump; this exception was approved by Jagdeep because no producer or consumer of `order.v1` existed yet.
- 2026-10-31 — `trade.v1` amended in place, no version bump (M5, ADR 0014): new required `buy_participant_id` and `sell_participant_id`; `buy_order_id`/`sell_order_id` limited to 2^40 − 1. Needed because order ids are unique only per participant since the M4 change. Approved by Jagdeep for the same reason: nothing produced or consumed `trade.v1` yet.
- 2026-10-31 — `trade.v1` description corrected (M5): the unique key is (`engine_run_id`, `symbol`, `match_id`), not (`engine_run_id`, `match_id`). `match_id` is counted per symbol's order book, so different symbols reuse the same numbers within a run. Found while writing the trade checker; no field or validation rule changed (ADR 0005 updated).
