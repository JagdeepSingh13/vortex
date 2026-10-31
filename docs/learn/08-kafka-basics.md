# 8. Kafka basics

Kafka is the pipe between every part of this platform. This guide explains the parts we use, how our local setup works, and what our own measurements say.

```
simulator / API ──▶ [ orders ] ──▶ engine ──▶ [ trades ] ──▶ Redis updater
                                                     └─────▶ Spark → Iceberg
```

## The core idea: an append-only log

A Kafka **topic** is a log: messages are only ever appended at the end, never changed or deleted one by one. Each message gets a number, its **offset**, and old messages are dropped only by **retention** (ours: 7 days).

```
topic "trades", partition 0
offset:   0     1     2     3     4     5   ...  ← new messages appended here
        [ m ] [ m ] [ m ] [ m ] [ m ] [ m ]
                      ▲                 ▲
              consumer A at 2    consumer B at 5
```

Consumers do not remove what they read. Each one just remembers its own position (offset). That is why one `trades` topic can feed both the Redis updater and the Spark job: they read the same log independently, at their own speed. If Spark crashes, it restarts from its last saved offset.

## Partitions: how Kafka scales, and what it costs

One log on one machine has a limit, so a topic is split into **partitions**: separate logs that can live on different brokers and be read by different consumers in parallel. Ours have 3.

```
topic "orders"
  partition 0: [AAPL][MSFT][AAPL][AAPL][MSFT] ...
  partition 1: [ ... other symbols ... ]
  partition 2: [ ... other symbols ... ]
```

The catch: **order is guaranteed only within a partition**, not across a topic.

### Why our key is the symbol (hard rule)

When a producer sends a message with a **key**, Kafka picks the partition as `hash(key) % partitions` (murmur2 hash). Same key → same partition → same order. With key = symbol:

- every AAPL order lands in one partition, in the order it was sent,
- so the engine sees AAPL's orders in sequence. Order matters for matching: a cancel must not overtake the add it cancels.

`make kafka-check` proves this on our broker: it sends 12 orders and checks each symbol stayed in one partition with its order preserved.

### A real finding: a hot partition

The check printed:

```
AAPL -> Partition:0
MSFT -> Partition:0
```

Both symbols hash to partition 0, so partitions 1 and 2 are empty and one partition does all the work. This is called a **hot partition** (key skew). It does not break correctness, but it limits parallelism: at most one consumer in a group gets any data. It evens out with more symbols. In real systems with a few huge keys, people split keys further (for example by symbol and venue) or choose partitioning by hand.

## Producers, consumers and consumer groups

- A **producer** sends messages. `acks` says when a send counts as done: `acks=all` waits until the broker has written it (all in-sync replicas; here, the one copy). We use `acks=all`.
- A **consumer** reads from partitions and commits its offset.
- A **consumer group** is a set of consumers sharing the work. Each partition goes to exactly one consumer in the group. Two different groups each get every message.

```
group "bronze-writer":  consumer 1 ← partitions 0,1     consumer 2 ← partition 2
group "redis-updater":  consumer 1 ← partitions 0,1,2   (gets every message too)
```

**Lag** is how far a group is behind: last offset in the partition minus the group's committed offset. `make kafka-groups` shows it per partition. Growing lag means a consumer cannot keep up.

## KRaft: no more ZooKeeper

Kafka needs a **controller** to store cluster metadata: which topics exist, which broker leads each partition. For 12 years that lived in a separate system, ZooKeeper. **KRaft** (Kafka Raft) moved it into Kafka itself, using the Raft consensus algorithm, and Kafka 4.0 removed ZooKeeper completely. Our single node is both:

```yaml
KAFKA_PROCESS_ROLES: broker,controller
```

In production you would run 3 or 5 controllers so the cluster survives a node failing (Raft needs a majority).

## Listeners: the classic setup bug

A client connects to the broker, and the broker replies "for partition 0, talk to me at **this address**" (the **advertised listener**). If that address is not reachable from where the client runs, it connects once and then fails. Our broker advertises two addresses:

| Listener | Address | Used by |
|---|---|---|
| INTERNAL | `kafka:29092` | other containers on the compose network (engine in M5, Spark in M6) |
| EXTERNAL | `localhost:9092` | programs running on Windows |
| CONTROLLER | `:9093` | KRaft only, never clients |

A container asking for `localhost:9092` would be talking to itself, which is why containers must use `kafka:29092`.

## Our setup

| | |
|---|---|
| Image | `apache/kafka:4.3.1`, pinned by digest |
| Nodes | 1 (broker + controller), heap 512 MB, ~280 MiB idle |
| Topics | `orders`, `trades`: 3 partitions, replication factor 1, 7-day retention |
| Auto topic creation | off: a typo in a topic name fails instead of creating a new topic |
| Data | Docker named volume `kafka-data`, kept by `make down`, deleted by `make reset` |

Replication factor 1 means one copy of the data. Fine on a laptop; a real cluster uses 3 brokers and RF 3, so losing one broker loses nothing.

## Commands

```bash
make up                     # start Kafka, wait until healthy
make topics                 # create orders + trades (safe to repeat), then describe them
make kafka-check            # round-trip test on a temporary topic

make kafka-topics           # partitions, leaders, config
make kafka-tail TOPIC=trades              # watch new messages live (Ctrl+C to stop)
make kafka-tail TOPIC=orders FROM_START=1 # everything since the beginning
make kafka-send TOPIC=orders              # type KEY|VALUE lines, e.g. AAPL|{...}
make kafka-groups           # consumer groups, offsets, lag
make kafka-offsets          # messages written per partition
make kafka-perf             # benchmark (3 runs per scenario, ~5 minutes)

make up-ui                  # optional web UI at http://localhost:8080
make down                   # stop (data kept);  make reset: stop and delete data
```

## What our measurements say

From [benchmarks/results/2026-10-31-kafka-98ba5d3.md](../../benchmarks/results/2026-10-31-kafka-98ba5d3.md): Kafka's own perf tools inside the broker container, real order JSON (138–248 bytes), `acks=all`, 3 runs each, Ryzen 5 5500U in Docker on WSL2.

| Scenario | Median | Range |
|---|---:|---|
| Producer, client defaults | ~80k msg/s | 74k–103k |
| Producer, `linger.ms=10`, `batch.size=64KB`, lz4 | ~235k msg/s | 175k–384k |
| Producer at a fixed 20k msg/s | p50 1 ms, p99 13 ms | one run spiked to 576 ms p99 |
| Consumer | ~303k msg/s | 237k–423k |

### Why batching makes it ~3× faster

Every request to the broker has a fixed cost: a network round trip, a write, an acknowledgement. Sending one 200-byte message per request wastes most of that cost. A producer **batch** groups many messages per partition into one request:

- `linger.ms`: how long to wait for more messages before sending a batch (adds a little latency),
- `batch.size`: the maximum batch size in bytes,
- `compression.type=lz4`: compresses the whole batch. Our JSON repeats the same field names in every message, so it compresses well, and fewer bytes means less work.

That is the classic **throughput vs latency trade-off**: waiting a few milliseconds to build bigger batches raises throughput a lot.

### Why the max-throughput latency numbers look terrible

In the max-throughput runs, p50 latency is around 1.5 seconds. That is not Kafka being slow per message. The perf tool sends as fast as it can, faster than the broker accepts, so messages pile up in the producer's buffer and "latency" mostly measures time spent waiting in that queue. The **paced** runs (fixed 20k msg/s, below saturation) show the real per-message time: **about 1 ms at p50**.

### Engine vs Kafka

| | Per message | Throughput |
|---|---|---|
| Matching engine (M2) | ~0.35 µs p50 | ~2M requests/s on one core |
| Kafka producer → broker | ~1 ms p50 (paced) | ~80k–235k msg/s |

On this laptop Kafka is about **3,000× slower per message** and **roughly 10× lower in throughput** than the matching code. End to end, the bottleneck will be Kafka, I/O and serialization, not matching. That is normal: real exchanges keep matching in memory and put persistence and distribution around it, which is exactly this design.

### Noise

Runs varied up to 2× (tuned: 175k–384k). The machine is a laptop VM under Windows, and the perf clients share CPUs with the broker. Conclusions only count when they hold across all runs: even the worst tuned run (175k) beat the best default run (103k), so the batching win is real.

## Check yourself

1. Two consumer groups read `trades`. Does each get every message? Why?
2. Why does per-symbol ordering need the key to be the symbol?
3. What is a hot partition, and why do we have one?
4. A container connects to `localhost:9092` and fails. Why, and what should it use?
5. What does `acks=all` wait for? What does it mean with replication factor 1?
6. Why does a higher `linger.ms` increase throughput but also latency?
7. Why is p50 latency 1.5 s in the max-throughput run but 1 ms in the paced run?
8. What did KRaft replace, and why does production run 3 controllers?

## Go deeper

- Kafka documentation: "Design" section (log, replication, producers)
- Martin Kleppmann, *Designing Data-Intensive Applications*, chapter 11 (stream processing)
- The Raft consensus algorithm (raft.github.io)
- Delivery semantics and idempotent producers (`enable.idempotence`)
- Partition count trade-offs: more partitions, more parallelism, more overhead
