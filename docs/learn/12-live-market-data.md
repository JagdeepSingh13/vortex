# 12. Live market data: Java, Kafka and Redis

M8 turns the stream of trades into live prices and live 1-minute candles that update within milliseconds, and shows them in the terminal. This guide covers the hot path / cold path idea, the Spring Boot service, Redis and its Lua scripts, and how measuring turned a service that fell behind into one that keeps up easily.

```
engine ─▶ Kafka "trades" ─▶ api (Spring Boot, Java 25)              ─▶ GET /prices, /candles ─▶ make candles
                              per trade: Lua script in Redis 8 │
                              last price + current 1-min candle ▼
                                                             Redis
```

## Hot path and cold path

| | Hot path (M8) | Cold path (M6 → M7 → M9) |
|---|---|---|
| Freshness | milliseconds | seconds to minutes |
| Holds | last price, last 24 h of candles | complete history |
| Correctness | best effort | authoritative, rebuildable from bronze |
| Built from | the same `trades` topic | the same `trades` topic |

Both read the same Kafka topic independently (two consumer groups). The engine knows about neither: it matches and publishes trades, and **all market data is derived downstream**. That keeps matching fast, and a problem in Redis or the API can never slow the engine down. Trades simply wait in Kafka until the service catches up.

The live candle is **computed on every trade** (open, high, low, close, volume), which costs microseconds. Only **redrawing** is throttled (the viewer's `REFRESH`), which is also how real charting systems work: compute per tick, conflate what you send or draw.

## The service

| Class | Job |
|---|---|
| `TradeListener` | `@KafkaListener` on `trades`, batches of messages, 3 threads (one per partition, so one writer per symbol) |
| `CandleStore` | the only class that knows the Redis keys; runs the Lua script, pipelined |
| `MarketDataController` | `GET /symbols`, `/prices/{s}`, `/candles/{s}?minutes=N`, `/stats` |
| `SymbolRegistry` | tick sizes from `config/symbols.yaml`; ticks → exact decimal strings with `BigDecimal` |
| `IngestStats` | counts and latency (engine event time → Redis updated) |
| `CandleViewer` | the terminal chart: calls the API, draws with ANSI escape codes |

Prices travel as integer ticks everywhere; the API adds a decimal **string** (`"190.05"`) for humans. Never a JSON float.

## Redis keys

```
trade:last:{AAPL}            last applied trade (engine run id, match id): the duplicate check
price:{AAPL}                 last price, quantity, event time
candle:{AAPL}:1m:1793466840  one candle (minute = epoch seconds), expires after 24 h
candles:{AAPL}:1m            sorted set of minutes, for "the last N candles"
```

The `{AAPL}` part is a **hash tag**: in a Redis Cluster, only the part in braces decides which node holds a key, so all of a symbol's keys stay together. That's required for one script to touch them all.

## Why a Lua script

Updating a candle is a read-modify-write: read high and low, compare, write. Done as separate commands, another client could read a half-updated candle in between. A **Lua script runs inside Redis as one atomic step**: nothing else runs until it finishes. It also turns about 10 commands into one network round trip.

The script also makes the update **idempotent**. Kafka delivers at least once, so after a restart the service may read a few trades again. The script remembers the last match id per symbol and engine run and returns 0 for anything already applied, so a trade counts once, the same idea as bronze's exactly-once.

## Making it keep up: a measurement story

The first version **fell behind**: at 2,000 orders/s (~920 trades/s) it applied about 800 trades/s, so latency grew to 20+ seconds. Each fix below was found by measuring first, not by guessing:

1. **Measure the call.** Timing `store.apply()` showed **6.8 ms per trade**, while Redis's own benchmark on the same network showed a 0.15 ms round trip. So the time was being lost on our side, not in the network.
2. **Cause 1: script loading.** `RedisScript.of(ClassPathResource)` re-checks on every call whether the script file changed, in a synchronized block that opens the jar entry. Loading the text once took the time to **2.1 ms** and the service kept up.
3. **Pipelining.** Sending a whole Kafka batch's script calls in one round trip. Redis's benchmark: ~6,000 requests/s one at a time vs ~270,000/s pipelined. But the time per batch was 13.8 ms…
4. **Cause 2: connections.** Redis's own counters (`INFO commandstats`, `INFO stats`) showed **3,514 `HELLO` handshakes for 3,510 batches**: Spring was opening a brand-new connection for every pipeline. Enabling Lettuce's **connection pool** cut that to 4 handshakes, and per-batch time to 3.9 ms.

Result in steady state: see the table below. The lesson for interviews: **the bottleneck was never Redis** (its script takes ~0.12 ms server-side). It was two hidden costs in the client library, found by comparing what the code measured with what Redis measured.

## Results

`make api-bench`, Ryzen 5 5500U, everything in Docker on WSL2, 60 s measured × 2 runs per rate, after a 60 s JVM warm-up. Latency is engine event time → trade in Redis:

| Orders/s | Trades/s | Avg batch | Redis ms/batch | p50 | p90 | p99 |
|---:|---:|---:|---:|---:|---:|---:|
| 2,000 | ~950 | 4 | 2.5–2.9 | 10.9–12.0 ms | 23–34 ms | 59–117 ms |
| 5,000 | ~2,400 | 9–12 | 3.6–5.5 | 13.0–21.1 ms | 34–109 ms | 109–699 ms |

Two more lessons came from the benchmark itself:
- **Warm up a JVM before measuring it.** Measured 10 s after a restart, the tails were seconds long. Against an already-warm service the same load gave p99 ~75 ms. The JIT compiler needs real traffic before the hot code is fast.
- **Check the environment's logs, not just your numbers.** One bad run lined up with Kafka's broker timing out on a heartbeat to *itself*. That means the whole Docker VM froze for seconds, which has nothing to do with our code.

Source: `benchmarks/results/2026-10-31-api-ab02ecf.md`.

## Commands

```bash
make up && make topics
make api-run                     # terminal 1: the market data service on :8090
make engine-run                  # terminal 2
make sim RATE=2000               # terminal 3
make candles SYMBOL=AAPL         # terminal 4: live candles (REFRESH=5 to redraw every 5 s)
curl localhost:8090/prices/AAPL
curl "localhost:8090/candles/AAPL?minutes=10"
make test-api                    # unit + Testcontainers tests (real Kafka and Redis)
make api-bench                   # trade -> Redis latency, writes a result file
```

## Check yourself

1. Why does the engine not compute candles itself?
2. What is the difference between computing a candle per trade and redrawing it every 5 s?
3. Why must the candle update be atomic, and how does a Lua script make it so?
4. A restart makes the service read 200 trades again. Why are they not counted twice?
5. What does `{AAPL}` in a key name do?
6. Why is pipelining ~45× faster than one request at a time, even though Redis itself is just as fast?
7. Which two hidden costs made the first version slow, and how were they found?
8. Why are prices returned as strings like `"190.05"` and not as numbers?
9. Why did the first benchmark show multi-second tails, and why was rerunning until the numbers looked good not the fix?

## Go deeper

- Redis: Lua scripting and `EVALSHA`, pipelining, hash tags in Redis Cluster
- Spring for Apache Kafka: batch listeners, ack modes, consumer concurrency
- Lettuce: shared vs dedicated connections, pooling
- Lambda architecture (speed layer vs batch layer) and Kappa architecture
- Testcontainers: real dependencies in tests
