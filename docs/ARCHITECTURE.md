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

## Engine internals (as imported from `cpp_exchange`)

- `core/`: fixed-width types (`Price` = int64, `Quantity` = uint32, 8-byte `Symbol`), memory pool, SPSC queue, clocks.
- `matching/`: `MatchingEngine` routes by symbol to an `OrderBook`; books keep price levels with intrusive FIFO lists of pooled `Order`s. Supports LIMIT, MARKET, IOC, FOK, GTC, STOP, STOP_LIMIT, ICEBERG, POST_ONLY; cancel and modify.
- `exchange/`, `net/`: TCP gateway and binary wire protocol (kept; not on the Kafka path).
- Events are a tagged union (`Event`) written into a preallocated buffer; no heap allocation on the matching path.

Known gaps to close (tracked in the roadmap):
- Timestamps come from `steady_clock` (monotonic, not epoch). `event_time_ns` must be UTC epoch nanoseconds → M2.
- Prices use a global `PRICE_SCALE = 10'000`; events use per-symbol ticks, so the engine converts at its Kafka edge (ADR 0004) → M5.
- `match_id` restarts at 1 each run; trades need `engine_run_id` (ADR 0005) → M5.
- The `Trade` event has no aggressor side; `trade.v1` requires it (ADR 0006) → M5.

## Kafka topics

| Topic | Key | Value | Producer | Consumers |
|-------|-----|-------|----------|-----------|
| `orders` | symbol | `order.v1` JSON | simulator, Order API | engine |
| `trades` | symbol | `trade.v1` JSON | engine | Redis updater (API), Spark bronze job |

Keying by symbol keeps per-symbol ordering within a partition.

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
| `order_id` | safe int | always | unique within an engine run; for CANCEL/MODIFY the order to change |
| `symbol` | string `^[A-Z][A-Z0-9.]{0,7}$` | always | must exist in `config/symbols.yaml` |
| `action` | `ADD` / `CANCEL` / `MODIFY` | always | |
| `participant_id` | safe int | always | |
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
| `match_id` | safe int | restarts each run; unique key = (`engine_run_id`, `match_id`) |
| `symbol` | string | as above |
| `price_ticks` | safe int | |
| `qty_lots` | 1..2^32-1 | |
| `buy_order_id`, `sell_order_id` | safe int | |
| `aggressor_side` | `BUY` / `SELL` | side of the incoming order (ADR 0006) |
| `sequence_number` | safe int | engine event sequence, increasing within a run |
| `event_time_ns` | epoch ns | set by the engine |

All fields are required.

## Lakehouse tables (proposed)

- `bronze.trades`: every field of `trade.v1` as received, plus `kafka_partition`, `kafka_offset`, `ingest_time_ns`. Append-only.
- `silver.trades`: deduplicated on (`engine_run_id`, `match_id`), partitioned by `symbol` and `days(event_time)`. Integers only.
- `gold.*` (dbt): `candles_1m`, `vwap`, `daily_symbol_stats`. Decimal prices derived from `config/symbols.yaml`.

## Schema change log

- 2026-10-31 — `order.v1` and `trade.v1` created (M1).
