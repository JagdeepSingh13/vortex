# 13. One order, end to end: how every part works and how data moves

Guides 9–12 explain each component on its own. This guide follows **one order** through the whole system, from the moment the simulator invents it to the moment its trade shows up as a candle in your terminal. At every hop it shows what the code does, what form the data is in, and why it is built that way. File links point at the real code, so you can read along.

```
 ┌──────────────┐  order.v1 JSON   ┌───────────────────┐           ┌──────────────────────────────────┐
 │  simulator   │ ───────────────▶ │ Kafka  "orders"   │ ────────▶ │  C++ engine (3 threads)          │
 │  (Python)    │  key = symbol    │ 3 partitions      │           │  consumer ─▶ engine ─▶ publisher │
 └──────────────┘                  └───────────────────┘           └───────────────┬──────────────────┘
                                                                                  │ trade.v1 JSON, key = symbol
                                                                                  ▼
                                   ┌───────────────────────────────────────────────────────────────┐
                                   │ Kafka  "trades"  (3 partitions)                              │
                                   └───────┬───────────────────────────────────────────┬───────────┘
                         hot path (ms)     │                                           │   cold path (s)
                                           ▼                                           ▼
                           ┌───────────────────────────┐              ┌───────────────────────────────┐
                           │ Java api (Spring Boot)    │              │ Spark bronze job (every 10 s) │
                           │ 3 listener threads        │              │ → Iceberg table on SeaweedFS  │
                           └─────────────┬─────────────┘              └───────────────────────────────┘
                                         │ EVALSHA apply_trade.lua (pipelined)
                                         ▼
                           ┌───────────────────────────┐  HTTP GET   ┌───────────────────────────────┐
                           │ Redis: price + candles    │ ◀────────── │ api endpoints ◀── make candles │
                           └───────────────────────────┘             └───────────────────────────────┘
```

| Component | Language | Runs as | Threads | Keeps state in |
|---|---|---|---|---|
| Simulator | Python 3.12 | container `vortex/python-tools:m4` | 1 (+ librdkafka's own) | memory: mid prices, its live orders |
| Kafka | Java (broker) | container `apache/kafka:4.3.1` | many | disk: the `orders` and `trades` logs |
| Engine | C++20 | container `vortex/engine-build:m5` | 3 + main | memory only: the order books |
| api | Java 25 | container `eclipse-temurin:25` | 3 listeners + HTTP | nothing (all in Redis) |
| Redis | C | container `redis:8.8.2` | 1 for commands | memory only (no persistence) |
| Bronze job | Python on Spark | container (profile `stream`) | Spark tasks | Iceberg table + checkpoint |

---

## Step 0. One config file all components share

Every component reads [`config/symbols.yaml`](../../config/symbols.yaml): six symbols, with a tick size and a lot size for each. This is the single source of truth for units.

There are three ways to write a price. Here is AAPL at $190.01:

| Unit | Value | Where it is used |
|---|---|---|
| Currency | `190.01` | only for humans: API strings, the viewer, gold tables later |
| **Ticks** | `19001` (= 190.01 / 0.01) | every message on Kafka, Redis, bronze |
| Engine units | `1900100` (= ticks × 100, since 0.01 × the engine's `PRICE_SCALE` 10,000 = 100 per tick) | inside the C++ engine only |

Quantities work the same way: **lots** on the wire, and lots × `lot_size` inside the engine.

**Why integers everywhere:** `0.1 + 0.2 != 0.3` in binary floating point. An exchange that adds up prices as floats ends up with candles that are off by a fraction of a cent, so floats are banned until the very last step (guide 5).

---

## Step 1. The simulator invents an order

Code: [`simulator/vortex_sim/market.py`](../../simulator/vortex_sim/market.py) (what to send) and [`__main__.py`](../../simulator/vortex_sim/__main__.py) (how to send it).

### 1a. Who is "trading"

`OrderFlow` is a small model of a market, driven by a seeded random generator, so the same seed always gives the same orders:

- **One market maker per symbol.** It keeps up to 5 buy and 5 sell limit orders ("quotes") 1–5 ticks around the mid price. When it has 10, it cancels its oldest quote before placing a new one. This provides the resting liquidity that others trade against.
- **10 noise traders.** They send random orders of all nine types: IOC 35 %, LIMIT 25 %, MARKET 10 %, POST_ONLY 8 %, FOK 6 %, ICEBERG 6 %, GTC 4 %, STOP 3 %, STOP_LIMIT 3 %. Most are priced at or through the opposite side, so they **cross the spread and cause trades**.
- **A random walk.** Each step has a 5 % chance to move the symbol's mid price one tick up or down. That walk is why the candles move.

Each call to `next()` ([market.py:72](../../simulator/vortex_sim/market.py:72)) picks a symbol, then a step: maker quote 50 %, new noise order 35 %, noise cancel 10 %, noise modify 5 %.

### 1b. The message

An ADD that a noise trader might send (an illustrative example, with the real field names):

```json
{"schema_version":1,"order_id":4211,"symbol":"AAPL","action":"ADD","participant_id":5576105,
 "client_time_ns":1793473502405836314,"side":"BUY","order_type":"IOC","qty_lots":30,"price_ticks":19002}
```

- `order_id` counts up **per participant** (`Counter` in `_add`). Two traders may both have an order 1, as on a real exchange. The engine makes these globally unique in Step 3.
- `participant_id` = a random base per run + 1..110, so a restarted simulator never reuses the ids of orders that are still resting in the book.
- `client_time_ns` is the simulator's wall clock when it created the order. The engine uses it to measure latency.
- CANCEL and MODIFY messages carry the same `order_id` they refer to. The simulator never sees trades (**open loop**), so it sometimes cancels an order that has already filled, and the engine rejects that, just as real clients' cancels race real fills.

### 1c. Sending it

The producer settings ([`__main__.py` `make_producer`](../../simulator/vortex_sim/__main__.py)), and what each one does:

| Setting | Effect |
|---|---|
| `key = symbol` | Kafka hashes the key to choose a partition: **all AAPL orders go to the same partition, in order** |
| `partitioner = murmur2_random` | the same hash function the Java client uses. librdkafka's default (CRC32) would send AAPL to a *different* partition than Java would; this bug was found in M4 (ADR 0013) |
| `acks = all` | the broker confirms only after the message is written |
| `enable.idempotence` | if a send is retried after a timeout, the broker discards the duplicate and keeps the order |
| `linger.ms = 5`, `lz4` | waits up to 5 ms to fill a batch, then compresses it: ~3× more throughput for a few ms of latency (measured in M3) |

**Open-loop pacing** ([`__main__.py:101`](../../simulator/vortex_sim/__main__.py:101)): at `--rate 1000`, the loop computes how many orders *should* have been sent by now (`elapsed × rate`) and sends exactly that many. It never waits for the system to respond. A closed loop (send, wait for the answer, send again) would slow down whenever the system slows down, and that would hide the slowness (coordinated omission, guide 6).

`produce()` only puts the message in librdkafka's local queue. A background thread batches and sends it, and `on_delivery` is called later, from `poll()`, with the partition the message landed on, or an error. That is where the `delivered=… partitions={0: …}` line comes from.

---

## Step 2. Kafka stores it

Kafka is a **log**: an append-only file per partition, where each message gets the next **offset** (0, 1, 2, …). Our settings ([`Makefile`](../../Makefile), [`docker-compose.yml`](../../infra/docker/docker-compose.yml)):

- Topics `orders` and `trades`, each with **3 partitions**, replication 1 (a single broker), retention 7 days. Auto-creation of topics is off, so a typo cannot create a topic.
- murmur2(symbol) mod 3 puts AAPL and MSFT on partition 0, GOOGL and NFLX on 1, AMZN and NVDA on 2. The symbols were chosen so that each partition gets two.
- **KRaft mode**: one process is both the broker and the controller (which holds the cluster metadata). There is no ZooKeeper.
- Three **listeners**: `kafka:29092` for containers on the Docker network (everything in this project), `localhost:9092` for programs on your Windows host, and `:9093` for the controller.

Ordering is guaranteed **within a partition**. Because the key is the symbol, "AAPL order 7 before AAPL order 8" always holds, while different partitions are processed in parallel.

**Consumer groups:** each reader has a group name, and Kafka remembers how far that group has read (its committed offset):

| Group | Reads | Starts at |
|---|---|---|
| `vortex-engine` | `orders` | always the end (never commits, see Step 3) |
| `vortex-api-market-data` | `trades` | its last commit (the end, the first time) |
| bronze job | `trades` | its own Spark checkpoint, not a Kafka commit |

The api and bronze read the *same* trades independently, and neither one slows the other down.

---

## Step 3. The engine

Code: [`engine/src/kafka_main.cpp`](../../engine/src/kafka_main.cpp). The engine is three threads joined by two lock-free queues:

```
 Kafka "orders"                                                                         Kafka "trades"
      │                                                                                       ▲
 [consumer thread] ──InboundQueue (65,536)──▶ [engine thread] ──OutboundQueue (65,536)──▶ [publisher thread]
  poll, parse JSON,                            match, nothing else:                       JSON, produce,
  ticks → engine units                         no I/O, no malloc, no locks                engine units → ticks
```

**Why three threads:** matching takes ~0.35 µs per order (M2 benchmark), while parsing JSON and talking to Kafka are much slower. If one thread did everything, matching would wait on network calls. Splitting the work lets the matching thread run without stalls, and the slow work happens on the other two (guide 10).

### 3a. Consumer thread: bytes → `InboundRequest`

[`run_consumer`, kafka_main.cpp:120](../../engine/src/kafka_main.cpp:120):

1. `rd_kafka_consumer_poll(100 ms)` returns one message, or nothing.
2. It stamps `received = wall_clock_ns()`.
3. `decoder.decode(...)` ([order_decoder.h:58](../../engine/src/kafka/order_decoder.h:58)):
   - It copies the bytes into a reused buffer with **padding**: simdjson reads in 32/64-byte SIMD blocks and may read past the end, so the buffer must have spare bytes.
   - It walks the JSON object once, with simdjson's **on-demand** API. Each known key fills a field and sets a bit in `seen_` (a bitmask that records which fields were present).
   - Integers are strict: `get_int64()` rejects `19002.0`, so a float price is a decode error.
   - `check_and_convert()` checks that the fields required for this action are present (ADD needs side, type and qty; MODIFY needs price and qty). It then converts **ticks → engine units** with `__builtin_mul_overflow`, so a huge value is rejected instead of silently wrapping around.
4. If decoding fails, the message is counted in `decode_errors` and skipped. A bad message never stops the engine.
5. It records the **ingest latency**: `received − client_time_ns`, i.e. the time from the simulator creating the order to the engine reading it.
6. It pushes the request onto `InboundQueue`. If the queue is full (the engine is behind), it **waits and retries** (backpressure). It never drops an order.

The engine never commits offsets and starts at the **end** of `orders` each time (`auto.offset.reset=latest`). Each engine start is a new trading session with a new `engine_run_id`, and resting orders do not survive a restart.

### 3b. The SPSC queue

[`core/spsc_queue.h`](../../engine/src/core/spsc_queue.h). SPSC means single producer, single consumer: exactly one thread pushes and exactly one pops. Because of that, it needs no lock:

- It is a fixed array of 65,536 slots (a power of two, so `index & (N-1)` replaces a slow `%`) with two counters: `tail` (written only by the pusher) and `head` (written only by the popper).
- `push`: if `tail − head == N` the queue is full; otherwise it copies the item into `buffer[tail & MASK]` and then does `tail.store(tail+1, release)`.
- `pop`: if `head == tail` the queue is empty; otherwise it copies out `buffer[head & MASK]` and does `head.store(head+1, release)`.
- **Release/acquire** ordering guarantees that the popper sees the item's bytes before it sees the new `tail`. Without it, the CPU could reorder the writes and the popper would read a half-written slot.
- Each counter sits in its own 128-byte block (`alignas(128)`). If `head` and `tail` shared a cache line, the two cores would keep taking it from each other (**false sharing**), and every push and pop would get slower.
- Items must be *trivially copyable* ([`messages.h`](../../engine/src/kafka/messages.h)): a push is a plain memory copy, with no allocation.

### 3c. Engine thread: `ExchangeCore::handle`

[`run_engine`, kafka_main.cpp:222](../../engine/src/kafka_main.cpp:222) pops a request and calls [`ExchangeCore::handle`, exchange_core.h:43](../../engine/src/exchange/exchange_core.h:43), which:

1. Reads **two clocks**:
   - `clock_.next()` is a *logical* counter (1, 2, 3, …). It is what the book uses for time priority, so replaying the same orders always gives identical results (the golden replay test depends on this).
   - `wall_clock_ns()`, read **once per request**, becomes `event_time_ns` on everything this request causes. It never influences matching (ADR 0009).
2. Rejects unknown symbols and out-of-range ids.
3. Builds the **exchange order id** with `make_order_id(participant, client_order_id)` = `participant << 40 | order_id` (24 bits + 40 bits, [protocol.h](../../engine/src/exchange/protocol.h)):
   - The ids of two traders can never collide.
   - The owner of any order can be read straight out of its id, with no lookup table.
   - A trader can only cancel its own orders, because a cancel builds the id from the trader's own participant number.
4. Calls `engine_->process_order(order)`. [`MatchingEngine`](../../engine/src/matching/matching_engine.h) looks up the symbol's `OrderBook` in a map (the books are created at start-up for the six symbols) and dispatches ADD, CANCEL or MODIFY to it.
5. Gets back a `span<const Event>`: a view into the book's event buffer listing everything that happened (ACCEPTED, TRADE, FILLED, RESTED, …). It translates each event **immediately**, because the buffer is reused by the next request.

### 3d. The order book: data structures

[`matching/order_book.h`](../../engine/src/matching/order_book.h). There is one book per symbol:

```
 bids_  std::map<price, Level*, greater>   best (highest) first      asks_  std::map<price, Level*, less>  best (lowest) first
   190.00 → Level{ total 250 } : [order A 100] ⇄ [order B 150]          190.01 → Level{ total 100 } : [maker 100]
   189.99 → Level{ total  80 } : [order C  80]                          190.02 → Level{ total 300 } : [...] ⇄ [...]

 orders_      unordered_map<order id, OrderRecord>    find any live order in O(1), for cancel/modify
 stop_buys_   multimap<trigger price, Order*>          stops wait here, outside the book
 stop_sells_  multimap<trigger price, Order*>
 best_bid_ / best_ask_                                 cached pointers to the best levels
```

- A **Level** ([level.h](../../engine/src/matching/level.h)) is one price, holding a **doubly linked list** of orders in arrival order. A new order goes to the tail, and matching takes from the head: this is **price-time priority** (FIFO). Removing an order from the middle (a cancel) is O(1) through its `prev`/`next` pointers.
- An **Order** ([order.h](../../engine/src/matching/order.h)) is 64-byte aligned, at most 128 bytes, and knows its level (`parent_level`). That makes a cancel O(1): find the order in `orders_`, unlink it, done.
- **No heap allocation while matching:** Orders and Levels come from `MemoryPool`s ([memory_pool.h](../../engine/src/core/memory_pool.h)), pre-allocated arrays whose free slots are chained in a free list, so allocate and free are a few pointer moves each. The maps use `std::pmr` memory resources carved from one big buffer allocated at start-up. Capacity per book (from `kafka_main.cpp`): 131,072 orders, 16,384 price levels, 16,384 stops, ~45 MB.

### 3e. The order book: what happens to our IOC buy

Our order: IOC BUY 30 lots @ 190.02. The book has a maker's SELL 100 @ 190.01, and nothing else at or below 190.02.

1. `add_order` ([order_book.h:121](../../engine/src/matching/order_book.h:121)) calls `validate_new_order` (line 419), which checks qty > 0, the id is not a duplicate, there is a price where one is needed, POST_ONLY would not cross, FOK has enough liquidity and MARKET has a non-empty other side. If any check fails, the order is rejected with a reason code.
2. It takes an `Order` from the pool and indexes it in `orders_`.
3. `execute_inbound_order` (line 1011) emits ACCEPTED and calls `match`.
4. `match` (line 931): the best ask 190.01 ≤ 190.02, so the order is **marketable** (it can trade). `match_against_levels` (line 893) walks the asks from the best price outwards, and stops at the first level the buyer's limit doesn't reach.
5. `match_fifo_at_level` (line 812) takes the oldest order at that level (the maker) and calls `execute_trade_slice` (line 780):
   - qty = min(30, 100) = 30.
   - **The price is the resting order's price, 190.01, not 190.02.** The order that was waiting sets the price, and the aggressor gets the better price.
   - It emits a `Trade` event with `match_id` = the next number **for this symbol** (match ids are per book), both order ids, and `aggressor_side = BUY`.
   - The maker has 70 left (PARTIALLY_FILLED) and stays first in the queue.
6. After matching, `trigger_stop_orders` (line 843) checks whether the new last price sets off any stops:
   - A buy stop at or below the last price becomes a MARKET (or LIMIT) order and matches immediately.
   - A sell stop at or above the last price does the same.
   - This repeats in a loop, because one stop's trades can trigger the next (a **stop cascade**).
7. Back in `execute_inbound_order`: our order is fully filled, so it emits FILLED and `retire_order` returns it to the pool and erases it from `orders_`. If it had quantity left:
   - LIMIT, GTC, POST_ONLY and ICEBERG **rest** in the book: `insert_order` puts them at the tail of their level.
   - IOC, FOK and MARKET orders have their remainder **canceled**.

Other order types, in brief:
- **ICEBERG** shows only `display_qty`. When the visible part is used up, `replenish_iceberg` (line 672) shows the next slice and moves the order to the **back** of the queue, so it loses its time priority.
- **CANCEL** finds the order, unlinks it from its level, and frees the level if it is now empty.
- **MODIFY** depends on the change:
   - Same price and a smaller quantity: it is reduced in place and **keeps its place** in the queue.
   - Any other change: it is canceled and replaced, so it **goes to the back**. That is fair, because changing the price is like sending a new order.

### 3f. From the book back to Kafka

For each `TRADE` event, `ExchangeCore::publish_trade` calls `KafkaSink::trade` ([kafka_main.cpp:201](../../engine/src/kafka_main.cpp:201)). That builds an `OutboundTrade` with three parts: the trade, its `event_time_ns`, and the order's `client_time_ns`, which travels along for the end-to-end measurement. It then pushes it onto `OutboundQueue`, waiting if the queue is full.

The private messages (fills for each trader, order acknowledgements) and the book updates are handed to the sink too, but they have no Kafka topic yet, so the Kafka sink ignores them.

### 3g. Publisher thread: `OutboundTrade` → trade.v1

[`run_publisher`, kafka_main.cpp:269](../../engine/src/kafka_main.cpp:269):

1. It pops a trade. If the queue is empty, it calls `rd_kafka_poll` (which runs the delivery callbacks) and sleeps 50 µs. That keeps a laptop core free, at the cost of ≤ 50 µs of delay.
2. [`TradeEncoder::encode`](../../engine/src/kafka/trade_encoder.h) reverses the conversion: engine units → ticks and lots, and `owner_of` / `client_order_id_of` split each packed id back into (participant, order id). Every number goes through `std::format` as an integer. An illustrative message, with the real format:

```json
{"schema_version":1,"engine_run_id":"b6f5d018aba65f34","match_id":31194,"symbol":"AAPL",
 "price_ticks":19001,"qty_lots":30,"buy_participant_id":5576105,"buy_order_id":4211,
 "sell_participant_id":5576001,"sell_order_id":873,"aggressor_side":"BUY",
 "sequence_number":96002,"event_time_ns":1793473502405911020}
```

3. `rd_kafka_producev` sends it to `trades` with **key = symbol**, using the same producer settings as the simulator (acks=all, idempotent, linger 5 ms, lz4, murmur2). The order's time rides along as the message's *opaque* pointer, so that when the broker acknowledges the message, `on_delivery` can record the **end-to-end** latency (order created → trade stored in Kafka).

**Identity of a trade:** `(engine_run_id, symbol, match_id)`. Match ids restart at 1 for each run and each symbol, so all three parts are needed (a bug found and fixed in M5).

**Shutdown order** (Ctrl+C): the consumer stops first, then the engine finishes everything already queued, then the publisher flushes. Every order the engine accepted gets its trades delivered before the process exits, and then it prints the `SUMMARY` lines.

---

## Step 4. The Java service reads the trade

Code: [`api/src/main/java/dev/vortex/api/market/`](../../api/src/main/java/dev/vortex/api/market/), settings in [`application.yaml`](../../api/src/main/resources/application.yaml).

### 4a. Start-up

Spring Boot creates the objects (beans) and connects them:
- `SymbolRegistry` loads the tick sizes.
- `CandleStore` reads the Lua script text **once** and sends it to Redis with `SCRIPT LOAD`, which returns a SHA-1 hash. From then on each call sends only the 40-character hash, not the whole script.
- The Kafka listener container starts **3 consumer threads** (`concurrency: 3`), one per partition of `trades`.

### 4b. A batch of trades

[`TradeListener.onTrades`, line 44](../../api/src/main/java/dev/vortex/api/market/TradeListener.java:44) receives everything one `poll()` returned for its partition: a **batch**, typically 4–12 trades at our rates.

1. **Parse.** Jackson turns each JSON value into a `TradeEvent` record and ignores fields it doesn't need, like the participant ids. An unreadable message or unknown symbol is counted as `rejected` and skipped. It never blocks the partition (bronze keeps the raw bytes anyway).
2. **Write.** `store.applyAll(trades)` ([CandleStore.java:75](../../api/src/main/java/dev/vortex/api/market/CandleStore.java:75)):
   - For every trade it builds 4 key names and 7 arguments, including the trade's **minute**, computed from the engine's event time: `floorDiv(event_time_ns, 60·10⁹) × 60`. The candle a trade belongs to is decided by **when the engine matched it**, not by when Java happened to read it.
   - `executePipelined` sends all of the batch's `EVALSHA` calls in **one network round trip** and reads all the replies together.
   - The connection comes from a **pool** (commons-pool2), so there is no new TCP + HELLO handshake for every batch. That handshake was the second big slowdown found in M8 (guide 12).
3. **Record.** Each reply is `1` (applied) or `0` (duplicate). `IngestStats` records each applied trade's latency: `now − event_time_ns`, i.e. engine match → Redis updated.
4. **Commit.** When the method returns, Spring commits the batch's offsets (`ack-mode: batch`). This is *after* Redis has the data, so a crash can only cause a re-read, never a loss: **at least once**.

If Redis restarted and lost the script, the call fails with NOSCRIPT. `applyAll` then loads the script again and retries the batch. That is safe, because trades already applied come back as duplicates.

**Why one thread per partition:** each symbol lives in exactly one partition, so it has exactly one writer. Two threads can never race to update the same AAPL candle out of order.

---

## Step 5. Redis applies the trade

The script: [`api/src/main/resources/redis/apply_trade.lua`](../../api/src/main/resources/redis/apply_trade.lua). Redis runs commands one at a time on a single thread, and a Lua script runs **as one command**. Nothing else can run in the middle of it, so nobody ever reads a half-updated candle.

The keys for AAPL after our trade (illustrative values):

```
trade:last:{AAPL}               hash  run=b6f5d018aba65f34  match=31194
price:{AAPL}                    hash  price_ticks=19001 qty_lots=30 event_time_ns=… engine_run_id=… match_id=31194
candle:{AAPL}:1m:1793473500     hash  minute=1793473500 open=19000 high=19003 low=18998 close=19001
                                      volume_lots=13581 trades=520                      (expires after 24 h)
candles:{AAPL}:1m               zset  1793473440, 1793473500, …   (which minutes exist, sorted)
```

What the script does, step by step:

1. **Duplicate check.** It reads `run` and `match` from `trade:last:{AAPL}`. If the run is the same and the stored match ≥ this trade's match, it returns `0` and changes nothing. This works because within one engine run, match ids for a symbol only ever increase. A new run (an engine restart) starts again from match 1, which is why the run id is part of the check.
2. **Last price.** It overwrites `price:{AAPL}`.
3. **Candle.**
   - If the minute's hash does not exist yet, this is the **first trade of the minute**, so open = high = low = close = this price.
   - Otherwise it raises high or lowers low if needed and always sets close.
   - Then `HINCRBY` adds the volume and the trade count, and `EXPIRE` keeps the candle for 24 h.
4. **Index.** `ZADD` adds the minute to a sorted set and `ZREMRANGEBYSCORE` trims minutes older than 24 h. That makes "the last N candles" one cheap query instead of scanning every key.

**`{AAPL}` is a hash tag.** In a Redis Cluster, only the part inside the braces decides which server stores a key. All of a symbol's keys therefore land on the same server, which a multi-key script requires. We run a single Redis, but the design is ready for a cluster.

**Redis settings** ([compose file](../../infra/docker/docker-compose.yml)): `--save ""` and `--appendonly no` turn persistence off, because everything can be rebuilt from Kafka and the lakehouse; there is a 128 MB memory cap; and `volatile-lru` evicts the least recently used keys *that have an expiry* if memory runs out.

---

## Step 6. Reading it back: HTTP and the terminal viewer

[`MarketDataController`](../../api/src/main/java/dev/vortex/api/market/MarketDataController.java):

| Request | Redis work | Answer |
|---|---|---|
| `GET /prices/AAPL` | `HGETALL price:{AAPL}` | `"price":"190.01"` plus `price_ticks`; 404 for an unknown symbol, 204 if no trade yet |
| `GET /candles/AAPL?minutes=30` | `ZREVRANGE` on the index, then `HGETALL` per candle | up to 30 candles, oldest first |
| `GET /stats` | none | counters and latency percentiles of the ingest |

Ticks become a decimal **string** with `BigDecimal` (`19001 × 0.01 = "190.01"`, exact). Two values deliberately stay away from JSON numbers:
- **Prices** are strings, because a JSON number is read as a float by JavaScript and many other clients.
- **`event_time_ns`** is also a string, because ~1.79 × 10¹⁸ is larger than 2⁵³, the largest integer a double can hold exactly.

**The viewer** ([`CandleViewer`](../../api/src/main/java/dev/vortex/api/viewer/CandleViewer.java), `make candles`) is a plain HTTP client. Every `REFRESH` seconds it:
1. calls `GET /candles/AAPL?minutes=40`;
2. finds the overall high and low and splits that range into 20 rows;
3. for each candle and row, prints `█` if the row is inside open..close (the body), `│` if it is only inside high..low (the wick), otherwise a blank;
4. colours the candle green if close ≥ open, red otherwise;
5. clears the screen with the ANSI code `ESC[H ESC[2J` and prints the frame.

It never talks to Redis directly, so the key layout is known only to `CandleStore`.

---

## Step 7. Meanwhile, the cold path

The same trade is also read by the bronze job ([`streaming/bronze_trades.py`](../../streaming/bronze_trades.py), guide 11):
- Every 10 s, Spark reads the new offsets of `trades`.
- It writes them as one Iceberg commit (Parquet files on SeaweedFS), with both typed integer columns and the raw message.
- It records the batch in its checkpoint, which together with the Iceberg commit makes it **exactly once** (proven in M6 with a kill -9 test).

This copy is complete and permanent. Silver (M7) and gold (M9) will build authoritative candles from it, and they can be compared with Redis's live ones.

---

## The journey in time

Measured figures only, Ryzen 5 5500U, everything in Docker on WSL2:

| Hop | Measured | Source |
|---|---|---|
| Matching one order in the book | ~0.35 µs | M2 engine benchmark |
| Order created → its trade acknowledged by Kafka, at 5k–18k orders/s | p50 ≈ 11 ms | `benchmarks/results/2026-10-31-e2e-33a4945.md` |
| Engine event time → trade in Redis, at 2,000 orders/s | p50 11–12 ms, p99 59–117 ms | `benchmarks/results/2026-10-31-api-ab02ecf.md` |
| Engine event time → trade in Redis, at 5,000 orders/s | p50 13–21 ms, p99 109–699 ms | same |
| Trade → bronze row, 1,000 orders/s, 10 s trigger | p50 5.0 s, p99 9.9 s | `benchmarks/results/2026-10-31-bronze-59c40ec.md` |

The matching itself is a tiny fraction of the total. Almost all of the time goes to deliberate batching (5 ms linger on each producer) and the Kafka hops, which buy throughput and durability.

---

## When something breaks

| What fails | What happens | Why it is OK |
|---|---|---|
| A message is malformed | engine: `decode_errors`++, skipped; api: `rejected`++, skipped | one bad message never blocks a partition |
| Engine restarts | new `engine_run_id`, starts at the end of `orders`, the old book is gone | each run is a trading session; the run id keeps trade ids unique |
| api restarts | resumes from its committed offsets and re-reads at most one batch | the Lua script returns 0 for trades already applied |
| Redis loses its data | live view is empty until new trades arrive; history is gone | Redis is a cache; the lakehouse has everything |
| Engine falls behind | `InboundQueue` fills, consumer waits; orders wait in Kafka | backpressure instead of dropping orders |
| api falls behind | trades wait in Kafka (lag grows), latency rises | nothing is lost; this is what happened before the M8 fixes |
| Bronze job is killed | restarts from its checkpoint | exactly once, proven by the M6 crash test |

---

## Check yourself

1. Write AAPL $190.01 in currency, ticks and engine units. Which components use which form?
2. Why is the Kafka key the symbol, and what went wrong with librdkafka's default partitioner?
3. The simulator is "open loop". What would a closed loop hide in a benchmark?
4. Which three jobs do the engine's three threads do, and why isn't it one thread?
5. How does the SPSC queue work without a lock? What do `release` and `acquire` guarantee, and why are `head` and `tail` 128 bytes apart?
6. Why does the book use a logical clock for priority but stamp trades with wall-clock time?
7. How does `participant << 40 | order_id` let the engine route fills without a lookup table?
8. An IOC buy for 30 @ 190.02 meets a resting sell for 100 @ 190.01. What is the trade price, what happens to each order, and why?
9. When does a MODIFY keep the order's place in the queue, and when does it lose it?
10. Why is a trade identified by `(engine_run_id, symbol, match_id)` and not just `match_id`?
11. Java commits offsets after writing to Redis. What would go wrong if it committed before? And how are the re-reads that the current order causes handled?
12. Why does a trade's candle depend on `event_time_ns` and not on when Java read it?
13. What does `{AAPL}` in a Redis key do, and why does the script need it in a cluster?
14. Why are prices and `event_time_ns` strings in the HTTP API?
15. Where does most of the ~11 ms from order to Redis go, and what would you change to make it smaller? What would that cost?

## Go deeper

- Order book design: price-time priority, intrusive linked lists, why `std::map` vs a flat price array
- Lock-free programming: C++ memory model (acquire/release), false sharing, the LMAX Disruptor
- simdjson on-demand parsing; librdkafka internals (batching, delivery reports)
- Kafka: partitioning, consumer group protocol, at-least-once vs exactly-once
- Redis: single-threaded command model, Lua scripting, pipelining, Cluster hash slots
- Spring for Apache Kafka: listener containers, batch listeners, ack modes
