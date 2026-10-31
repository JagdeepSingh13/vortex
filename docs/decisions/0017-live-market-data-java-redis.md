# 0017. Live market data: a Java service writes prices and candles to Redis

- Status: accepted
- Date: 2026-10-31
- Milestone: M8 (done before M7)

## Context

A trading screen needs the current price and the forming 1-minute candle within milliseconds. The lakehouse (bronze → silver → gold) is seconds to minutes behind, and the engine must do nothing but match (Jagdeep: "the engine should just match orders as fast as it can"). So live market data is derived downstream from the `trades` topic: the **hot path**, next to the lakehouse's **cold path**.

## Options

Where candles are computed:
1. **A Java service consuming `trades`**, writing to Redis, serving HTTP.
2. A streaming job (Spark or Flink) with event-time windows writing to Redis: more "correct" for late trades, another heavy JVM on a laptop already near its limit.
3. In the engine: rejected, it would put work on the matching path.
4. Lakehouse only: minutes late.

Store: **Redis 8** (open source again under AGPLv3 since 8.0) or Valkey 9 (BSD fork).

Candle update: **Lua script** (atomic, one round trip), Java read-modify-write, or the RedisTimeSeries module.

Viewer: **plain ANSI** escape codes, or a TUI library (JLine, Lanterna).

Scope: with or without order entry (`POST /orders`).

## Decision

- Spring Boot 4.1.1 on Java 25 (Spring for Apache Kafka 4.1.1, Spring Data Redis with Lettuce 7.5.2, Jackson 3.1.5; all managed by Boot), built in a pinned Maven container; runs in `eclipse-temurin:25.0.3_9-jre-alpine`. Redis 8.8.2, no persistence (everything can be rebuilt from Kafka), 128 MB, `volatile-lru`.
- Per trade, one Lua script (`apply_trade.lua`) checks for a duplicate (last engine run id + match id per symbol), sets the last price, updates the candle of the trade's minute (by engine event time: open, high, low, close, volume, trade count) and its index; candles expire after 24 h. Keys carry a `{SYMBOL}` hash tag (cluster-ready).
- One listener thread per partition, so each symbol has one writer. Kafka poll batches are written to Redis in one pipeline, over pooled connections. Offsets are committed after the batch is in Redis (at least once); duplicates are skipped by the script.
- Read API: `GET /symbols`, `GET /prices/{symbol}`, `GET /candles/{symbol}?minutes=N`, `GET /stats`. Prices are returned as integer ticks and as exact decimal strings (BigDecimal from `tick_size`), never JSON floats.
- Terminal viewer `CandleViewer` reads the HTTP API (only `CandleStore` knows the Redis layout), plain ANSI, redraw interval configurable (default 1 s). Candles are computed on every trade; only redrawing is throttled.
- **Market data only.** Order entry (`POST /orders`) is moved to a later milestone.

## Performance investigation (measured, Ryzen 5 5500U, Docker on WSL2, 2,000 orders/s)

The first version fell behind the trade stream. Each step was measured before changing anything:

| Version | Redis time | Trade → Redis p50 | Finding |
|---|---|---:|---|
| 1. one script call per trade, script from a classpath resource | 6.8 ms per call | ~19–28 s (backlog) | `RedisScript.of(Resource)` re-checks the resource on every call, synchronized, opening the jar entry |
| 2. script text loaded once | 2.1 ms per call | 0.19 s | kept up, but far above Redis's own 0.15 ms round trip |
| 3. pipeline per Kafka batch | 13.8 ms per batch of ~7 | 54.5 ms | Redis counters: one `HELLO` handshake per batch, i.e. a new connection for every pipeline |
| 4. + Lettuce connection pool | 3.9 ms per batch of ~5 | 14.8 ms (incl. warm-up) | 4 handshakes in total instead of ~3,500 |

Redis's own benchmark on the same network: ~5,400–6,700 sequential requests/s vs ~260,000–283,000/s pipelined; server-side time of the script ~120–136 µs per trade. Steady state with this design (`benchmarks/results/2026-10-31-api-ab02ecf.md`): p50 11–12 ms at 2,000 orders/s and 13–21 ms at 5,000 orders/s, keeping up at both.

## Consequences

- The live view is best effort: a trade arriving late updates the past candle it belongs to; the authoritative candles are gold (M9), which can be compared with Redis.
- Redis holds 24 h of candles; anything older comes from the lakehouse.
- A new consumer group starts from the latest trades; history is not replayed into Redis.
- The viewer needs the API running (`make api-run`).
- New dependency: `commons-pool2` (managed by Spring Boot), required for pooled Lettuce connections.
