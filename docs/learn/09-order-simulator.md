# 9. The order simulator

`make sim` pretends to be a market full of traders and sends their orders into Kafka. This guide explains how it models a market, how it talks to Kafka safely, and the partitioning bug it uncovered.

```
OrderFlow (market.py)          __main__.py                       Kafka
seeded random process   ──▶   pace + encode JSON + produce  ──▶  orders (3 partitions, key = symbol)
pure Python, testable          confluent-kafka (librdkafka)
```

## Modelling a market in a few rules

Real order flow comes from two kinds of participants, and the simulator has both:

| Who | Participant ids | What they do |
|---|---|---|
| **Market makers** (one per symbol) | base+1 … base+6 | Keep up to 5 buy and 5 sell quotes around the mid price. When full, cancel the **oldest** quote to make room for a fresh one near the current mid. This is how real makers keep quotes fresh as the price moves. |
| **Noise traders** (10) | base+101 … base+110 | Send random orders of every type. IOC, limit and market orders priced at or through the opposite side cross the spread and create trades. Post-only and iceberg orders add resting liquidity. Now and then they cancel or modify their own resting orders. |

The **mid price** of each symbol takes a random walk: on about 5% of steps it moves one tick up or down. Prices are always integer ticks around `reference_price` from `config/symbols.yaml` (AAPL starts at 190.00 = 19000 ticks).

Each step does one thing, chosen by weight:

```
50%  market maker: add a quote, or cancel its oldest if it already has 10
35%  noise trader: new order  (IOC 35%, LIMIT 25%, MARKET 10%, POST_ONLY 8%, FOK 6%,
                               ICEBERG 6%, GTC 4%, STOP 3%, STOP_LIMIT 3%)
10%  noise trader: cancel one of its resting orders
 5%  noise trader: modify one (never a stop: the engine rejects modifying stops)
```

The measured mix over 500,000 orders: 35% cancels, 34% limit (mostly maker quotes), 12% IOC, 5% modifies, and the rest spread over the other types. Cancel-heavy flow is realistic: on real exchanges most orders are cancelled, not filled.

### Open loop: cancels race fills

The simulator never reads `trades`, so it does not know which of its orders already filled. A cancel may target an order that is gone, and the engine answers `ORDER_NOT_FOUND`. That is not a bug: real clients see the same race when their cancel and a fill cross on the wire.

## Deterministic and testable

`OrderFlow` contains no Kafka code and takes its clock as a parameter, so a test can run it with a fixed time and seed:

- same seed → same stream (useful for reproducing a problem),
- `make test-sim` generates 10,000 orders and validates every one against `order.v1` with the M1 validator, so a float price or missing field fails the test,
- it also checks every symbol, action and order type appears, and that no participant reuses an order id.

That test first took **137 seconds**: the validator re-read the schema file and `symbols.yaml` and rebuilt itself on every call. Caching those (`functools.lru_cache`) brought it to **4.4 seconds**. A classic profiling lesson: the cost was not the work itself, it was repeating setup inside a hot loop.

## Order ids: unique per participant

Several producers send orders: the simulator now, the Order API later. If ids had to be unique across all of them, they would need to coordinate. Instead, each participant numbers its own orders from 1, and the engine identifies an order by the **pair** (`participant_id`, `order_id`). The engine's gateway already packs that pair into one 64-bit number: 24 bits participant, 40 bits order id. That is why `order.v1` now limits them to 2^24 − 1 and 2^40 − 1.

Each simulator run picks a **random participant range** (for example 4316001…4316110). Restarting the simulator therefore never reuses a (participant, order id) pair that might still be resting in the book from the previous run.

## Talking to Kafka safely

```python
Producer({
    "acks": "all",                    # done only when the broker has written it
    "enable.idempotence": True,       # retries never duplicate or reorder within a partition
    "linger.ms": 5, "compression.type": "lz4",   # batching: ~3x throughput in M3
    "partitioner": "murmur2_random",  # same partition for a key as the Java client
})
```

### Idempotent producer

When a send times out, the client retries. Without idempotence, if the first attempt actually arrived, the retry writes a **duplicate**, and a retried earlier batch can land **after** a later one. With idempotence, the producer gets an id from the broker and numbers each batch. The broker drops duplicates and rejects out-of-order batches. For orders this matters: a duplicated ADD is a second order, and a reordered cancel can arrive before its ADD.

(On a freshly started broker you may see `Failed to acquire idempotence PID ... Coordinator load in progress: retrying`. The broker is still loading its internal state; the client retries and continues.)

### `produce()` is asynchronous

`producer.produce()` only puts the message in a local queue and returns immediately. librdkafka's background thread batches and sends. The result arrives later through the **delivery callback** (`on_delivery`), called from `producer.poll()`. That is why the loop calls `poll(0)` regularly, why `flush()` at the end waits for everything outstanding, and why a full local queue raises `BufferError` (the simulator then polls and retries).

### Open-loop pacing

`--rate 2000` means: by time *t* seconds, 2000 × *t* orders should have been sent. Each loop iteration sends however many are **due**, then sleeps briefly. The schedule does not wait for anything downstream. This is the open-loop style from the benchmarking guide (no coordinated omission). A slow broker shows up as growing delivery lag, not as a lower send rate.

## The partitioner bug

The first real run printed:

```
partitions={0: 3307, 1: 6687}        ← partition 2 empty, NFLX on 0
```

The M3 check had shown AAPL and MSFT on partition 0, and murmur2, Kafka's hash, puts NFLX on 1 and AMZN/NVDA on 2. So where did that come from?

**Different clients hash keys differently:**

| Client | Default partitioner | Hash |
|---|---|---|
| Java client, Kafka CLI tools | murmur2 | murmur2(key) % partitions |
| librdkafka (Python confluent-kafka, C/C++, Go, .NET, …) | `consistent_random` | **CRC32**(key) % partitions |

Our hard rule is "key = symbol, so per-symbol ordering holds". That only works if **every producer maps the same key to the same partition**. The Order API (Java, M8) and the simulator (librdkafka) would split AAPL across two partitions, and the engine could see a cancel before the order it cancels.

Fix: `partitioner=murmur2_random` in every librdkafka producer. After the fix:

```
1960 Partition:0  AAPL      2017 Partition:1  GOOGL      1960 Partition:2  AMZN
2036 Partition:0  MSFT      2001 Partition:1  NFLX       2026 Partition:2  NVDA
```

which also cleared the M3 hot partition: 500,000 orders split 33.4% / 33.4% / 33.3%. The rule is now in `CLAUDE.md`, so the engine's producer in M5 uses it too.

## Measured send rate

`make sim-bench` sends 500,000 orders as fast as possible. Six runs on the Ryzen 5 5500U (Docker on WSL2) gave **32k–46k orders/s**, 0 errors, all delivered. Kafka's own perf tool reached ~80k msg/s with default settings in M3, so the simulator is **Python-bound**: building each order and encoding it as JSON costs more than sending it. For our needs (feeding the engine and the pipeline) that is plenty, and `--rate` usually runs it far lower.

## Commands

```bash
make up && make topics
make sim                          # 1000 orders/s until Ctrl+C
make sim RATE=5000 DURATION=30    # 5000/s for 30 s
make sim COUNT=10000 SEED=7       # exactly 10,000 orders, another seed
make kafka-tail TOPIC=orders      # watch them arrive (another terminal)
make kafka-offsets                # messages per partition
make sim-bench                    # max send rate, writes a result file
make test-sim                     # the validity test
```

## Check yourself

1. Why does the market maker cancel its oldest quote rather than a random one?
2. Why is it fine that some cancels are rejected?
3. What would go wrong without `enable.idempotence` when a send is retried?
4. Why must every producer use the same partitioner? What exactly would break?
5. Why are order ids unique per participant instead of globally?
6. Why does a restarted simulator pick new participant ids?
7. Why was the test 30× slower before caching, and what does that teach about hot loops?
8. The simulator reaches ~40k orders/s, Kafka's own tool ~80k. Where does the difference go?

## Go deeper

- librdkafka `CONFIGURATION.md`: `partitioner`, `enable.idempotence`, `linger.ms`
- Kafka idempotent and transactional producers (KIP-98)
- Market microstructure: market makers, the spread, order-to-trade ratios
- Python profiling: `cProfile`, why `json.dumps` and object creation dominate
