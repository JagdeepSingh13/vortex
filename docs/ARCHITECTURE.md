# Architecture

Status: **draft, written in M0**. Event contracts and table layouts are proposals until M1 freezes them as versioned schemas in `schemas/`.

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
- Prices use a global `PRICE_SCALE = 10'000`; the platform contract is per-symbol ticks → M1.
- Trades carry no participant/aggressor side yet; decide in M1 whether the contract needs them.

## Kafka topics

| Topic | Key | Value | Producer | Consumers |
|-------|-----|-------|----------|-----------|
| `orders` | symbol | `order.v1` JSON | simulator, Order API | engine |
| `trades` | symbol | `trade.v1` JSON | engine | Redis updater (API), Spark bronze job |

Keying by symbol keeps per-symbol ordering within a partition.

## Event contracts (proposed)

All prices are integer ticks and all quantities integer lots. Times are integer nanoseconds since the Unix epoch, UTC.

`order.v1`
| Field | Type | Notes |
|-------|------|-------|
| `schema_version` | int | `1` |
| `order_id` | int64 | unique per producer |
| `symbol` | string | ≤ 8 chars, must exist in `config/symbols.yaml` |
| `action` | enum | `ADD`, `CANCEL`, `MODIFY` |
| `side` | enum | `BUY`, `SELL` |
| `order_type` | enum | engine `OrderType` names |
| `price_ticks` | int64 | 0 for MARKET |
| `qty_lots` | int64 | > 0 for ADD |
| `participant_id` | int64 | |
| `client_time_ns` | int64 | producer clock, informational |

`trade.v1`
| Field | Type | Notes |
|-------|------|-------|
| `schema_version` | int | `1` |
| `trade_id` | int64 | engine `match_id`, unique per engine run |
| `symbol` | string | |
| `price_ticks` | int64 | |
| `qty_lots` | int64 | |
| `buy_order_id` | int64 | |
| `sell_order_id` | int64 | |
| `sequence_number` | int64 | engine sequence, monotonically increasing |
| `event_time_ns` | int64 | set by the engine, UTC epoch ns |

## Lakehouse tables (proposed)

- `bronze.trades`: every field of `trade.v1` as received, plus `kafka_partition`, `kafka_offset`, `ingest_time_ns`. Append-only.
- `silver.trades`: deduplicated on `trade_id`, partitioned by `symbol` and `days(event_time)`. Integers only.
- `gold.*` (dbt): `candles_1m`, `vwap`, `daily_symbol_stats`. Decimal prices derived from `config/symbols.yaml`.

## Schema change log

- (none yet)
