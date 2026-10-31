# 10. The engine on Kafka

M5 turned the matching engine into a Kafka service: orders in, trades out. This guide explains the thread design, the libraries, every conversion on the way, and what measuring it taught us.

```
Kafka "orders"                                                              Kafka "trades"
     │                                                                            ▲
[consumer thread] ──SPSC──▶ [engine thread] ──SPSC──▶ [publisher thread] ─────────┘
 librdkafka consumer          ExchangeCore::handle      encode trade.v1 JSON
 simdjson: JSON → request     → MatchingEngine          engine units → ticks
 ticks → engine units         → trades to the queue     librdkafka producer
```

## Why three threads

Hard rule: **nothing new on the matching thread**. No heap allocation, no locks, no logging, no I/O. Kafka calls do network I/O and allocate, and JSON parsing allocates and is comparatively slow. So all of that runs on two separate threads, and the engine thread does only this:

```cpp
while (true) {
    if (inbound.pop(request)) {                     // lock-free SPSC pop
        sink.order_time_ns = request.client_time_ns;
        core.handle(request.participant, request.request, sink);   // match
        continue;                                   // trades go to the sink → outbound.push
    }
    if (consumer is done && inbound is empty) break;
    std::this_thread::yield();
}
```

Each queue has **exactly one writer and one reader thread**, which is what lets it be lock-free (guide 3). The messages are fixed-size, trivially copyable structs (`InboundRequest`, `OutboundTrade`), so a push is a plain memory copy.

### Backpressure, not data loss

If a queue is full, the writer **waits**: the consumer waits for the engine, and the engine waits for the publisher. Nothing is ever dropped. If the publisher (Kafka) falls behind, the engine slows down, then the consumer, and Kafka simply keeps the unread orders. That is **backpressure**: a slow stage slows its producers instead of losing data.

### Shutdown in order

```
Ctrl+C → consumer stops reading → engine finishes every order already queued
       → publisher sends every trade already queued and flushes Kafka → exit
```

Every order the consumer accepted gets matched, and every resulting trade reaches Kafka.

## What the engine reuses

Almost everything that matters already existed:

| Piece | From | Role in M5 |
|---|---|---|
| `MatchingEngine`, `OrderBook` | M0 (your engine) | matching, unchanged |
| `ExchangeCore` | M0, + event time in M2 | validation, order-id packing, routes events to a sink |
| `SPSCQueue` | M0 | the two queues |
| `trade()` hook on the sink | **new, 4 lines** | hands the complete trade to the Kafka sink |
| `aggressor_side` in `Trade` | **new, 2 lines** | which side crossed the spread |

The hook uses a C++20 `requires` check, so sinks without `trade()` (the old TCP ones) still compile:

```cpp
if constexpr (requires { sink.trade(trade, request_time_ns_); }) {
    sink.trade(trade, request_time_ns_);
}
```

## Proving matching did not change

Adding a field to `Trade` touches the matching code, so the hard rule applies: prove old and new behaviour agree. The golden replay test can now dump its full output (`VORTEX_GOLDEN_DUMP`):

1. dump all 889,167 lines of the 300,000-request replay **before** the change,
2. make the change and dump again,
3. 65,034 lines changed, which is exactly the number of TRADE lines,
4. remove ` aggr=BUY|SELL` from the new dump: **byte-identical** to the old one.

The only difference is the new field. Everything else, every fill and every price, is identical.

## Conversions at the edge

The engine thinks in its own units; the platform speaks ticks and lots (guide 5). The conversion happens once on the way in and once on the way out, never inside matching:

| | In (consumer) | Out (publisher) |
|---|---|---|
| Price | `price_ticks × price_per_tick` (AAPL: ×100) | `price ÷ price_per_tick` |
| Quantity | `qty_lots × lot_size` | `qty ÷ lot_size` |
| Order id | `(participant_id, order_id)` → packed 64-bit id (24 + 40 bits) | packed id → `buy_participant_id`, `buy_order_id`, … |

`price_per_tick` comes from `config/symbols.yaml` (`tick_size` "0.01" × 10,000 = 100), computed from the decimal **string** with integer arithmetic, so no float is ever involved. Overflow is checked with `__builtin_mul_overflow`.

## The libraries

| Library | Why this one | Detail worth knowing |
|---|---|---|
| **librdkafka 2.3.0** | The standard C client (the Python simulator wraps it too) | Asynchronous: `produce()` only queues; delivery results come back through a callback during `rd_kafka_poll()` |
| **simdjson 3.6.4** | Fastest JSON parser; on-demand API reads fields in one pass | Needs `SIMDJSON_PADDING` spare bytes after the input, so each message is copied into a reused, padded buffer. `get_int64()` rejects `18990.0`, the same strict-integer rule as M1 |
| **yaml-cpp 0.8.0** | Reads `symbols.yaml` directly, one source of truth | It **throws** on errors, and the engine is built with `-fno-exceptions`. So only `symbol_config.cpp` is compiled with `-fexceptions`, and it catches everything |

### A trick: carrying data through librdkafka

To measure end-to-end latency per trade, the publisher needs to know, when Kafka acknowledges a message, when the order that caused it was created. librdkafka lets you attach a `void*` "opaque" to each message and hands it back in the delivery callback. A 64-bit timestamp fits in a pointer-sized value, so it travels with the message without any lookup table:

```cpp
RD_KAFKA_V_OPAQUE(reinterpret_cast<void*>(static_cast<std::uintptr_t>(trade.order_time_ns)))
// ... later, in the delivery callback:
auto order_time = static_cast<Timestamp>(reinterpret_cast<std::uintptr_t>(message->_private));
```

## Contract bugs found along the way

1. **Order ids are per participant** (M4), so a trade must say *whose* order filled: `trade.v1` gained `buy_participant_id` and `sell_participant_id` (amended in place, approved).
2. **The trade key was wrong.** ADR 0005 said `(engine_run_id, match_id)` is unique. But `match_id` is counted **per order book**, meaning per symbol, so AAPL's trade 1 and MSFT's trade 1 share it. Silver deduplication on that key would have silently deleted real trades. The correct key is `(engine_run_id, symbol, match_id)`, and the checker now verifies it never repeats.

## Measuring it, and the observer effect

The first measurement said a trade reached a consumer **6.3 seconds** after the engine stamped it. Kafka's own message timestamps showed the trades were in Kafka within milliseconds, so the 6 seconds belonged to the **checker**: it validated every trade with the Python schema validator, joined late, and then barely kept up. It was measuring itself.

Fix 1: measure where the information lives. The engine knows which order caused each trade, so the publisher records **order created → trade acknowledged by Kafka** for every trade.

Fix 2: even without measuring, running the checker **alongside** the benchmark competed for CPU on the laptop and inflated p99 from about 50–140 ms to 500–700 ms. So the benchmark now has two phases: **measure** (only the simulator, Kafka and the engine run), then **verify** (the checker reads the finished run back from Kafka). This is the **observer effect**: measuring a system changes it, so keep the measuring cheap and the heavy checking separate.

Fix 3: latency samples skip orders created in the first 5 seconds of a run (clients connecting, producer ids, cold caches).

## Results

From [benchmarks/results/2026-10-31-e2e-33a4945.md](../../benchmarks/results/2026-10-31-e2e-33a4945.md): Ryzen 5 5500U, everything in Docker on WSL2, 30 s per run, 2 runs per rate. End to end = order created by the simulator → its trade acknowledged by Kafka, in milliseconds:

| Target rate | Actually sent | E2E p50 | E2E p90 | E2E p99 (two runs) |
|---:|---:|---:|---:|---:|
| 1,000/s | 1,000/s | 16–24 | 78–155 | 223 / 515 |
| 5,000/s | 5,000/s | 11.6–12.2 | 15–17 | 42 / 39 |
| 10,000/s | 10,000/s | 10.9–12.0 | 15–20 | 32 / 266 |
| 20,000/s | 11,750–17,465/s | 10.5–13.7 | 14–34 | 27 / 104 |
| 30,000/s | 15,893–17,934/s | 10.6–10.9 | 15–17 | 107 / 298 |

Every one of the ~1.36 million trades was checked afterwards (unique key, per-symbol order, partition; the first 20,000 of each run also against the full schema): **zero problems**.

What the numbers say:

- **p50 is about 11 ms and flat** from 5,000 to ~18,000 orders/s. The system is not saturating.
- **The engine is not the bottleneck.** Matching takes ~0.35 µs (M2), and the engine thread plus its queue add ~0.2 ms. Almost all of the 11 ms is Kafka: two network hops, two broker writes with `acks=all`, and up to 5 ms of deliberate batching on each producer.
- **The load generator is the limit.** At 20k and 30k targets, the Python simulator, sharing the laptop with Kafka and the engine, could only send 11.7k–17.9k/s. Because latency starts when the simulator actually builds an order, its falling behind is not counted (coordinated omission in the generator), so those rows are optimistic.
- **p99 varies 3–8× between identical runs** (10k/s: 32 vs 266 ms). That is the laptop VM. Quote p50 and the best-run p99, and say so.
- **1,000/s is slower than 5,000/s.** Not investigated. A plausible cause is CPU power saving at low load (cores sleep deeper and wake slower). It is a hypothesis, not a finding.

A per-stage breakdown from a 2,000/s trial run (ms): order → engine ≈ 6 (p50) / 30 (p99); engine + outbound queue ≈ 0.2 / 3.5; publisher → Kafka ack ≈ 7 / 25.

## Commands

```bash
make up && make topics
make engine-run                  # terminal 1: the engine (Ctrl+C stops it cleanly, prints summaries)
make sim RATE=2000               # terminal 2: orders in
make kafka-tail TOPIC=trades     # terminal 3: trades out
make trades-check DURATION=30    # validate live trades
make e2e-bench                   # the full benchmark (~10 minutes)
```

## Check yourself

1. Why can't the consumer thread call `ExchangeCore::handle` itself?
2. What happens when Kafka is slow to accept trades? Is anything lost?
3. Why does shutdown stop the consumer first and the publisher last?
4. How was it proven that adding `aggressor_side` changed nothing else?
5. Why is `(engine_run_id, match_id)` not a unique key, and what would have gone wrong in silver?
6. Why does simdjson need padding after the message?
7. How does a trade's delivery callback know when its order was created?
8. The first measurement said 6.3 s. What was really being measured?
9. Matching takes 0.35 µs but end to end is ~13 ms. Where does the time go?

## Go deeper

- librdkafka `INTRODUCTION.md` (threads, delivery reports, idempotence)
- simdjson on-demand API and why SIMD makes JSON parsing fast
- Backpressure in streaming systems
- The observer effect in benchmarking; Brendan Gregg, *Systems Performance*
- Event sourcing: rebuilding state by replaying a log (a possible future for restart recovery)
