# Interview notes

One entry per completed milestone, newest last. Keep each entry short enough to say out loud in two minutes.

Format:

```
## MN — Title (YYYY-MM-DD)
- What I built: one or two sentences.
- Why it is designed this way: the key decision(s), with ADR links.
- Trade-off I can defend: what I gave up and why that was acceptable.
- Numbers: only measured figures, with machine and conditions. "None yet" is fine.
- Likely questions: 2-3 questions an interviewer could ask, with short answers.
- Study further: concepts to read up on.
```

## M0 — Repository bootstrap (2026-10-30)
- What I built: a monorepo for the platform, with my existing C++ matching engine imported into `engine/` and a one-command build (`make engine && make test`) that runs inside a pinned Linux container.
- Why it is designed this way: git subtree keeps the engine's history in one repo ([ADR 0001](decisions/0001-import-engine-with-git-subtree.md)); a container build makes results independent of my Windows machine and avoids MinGW pain with librdkafka later ([ADR 0002](decisions/0002-build-engine-in-linux-container.md)).
- Trade-off I can defend: builds need Docker and are slower the first time; in exchange anyone can reproduce them from a clean clone.
- Numbers: none yet.
- Likely questions:
  - *Why a monorepo?* The services share event schemas and symbol config; one repo keeps them versioned together.
  - *Subtree vs submodule?* Submodules pin a commit in another repo and complicate cloning and CI; subtree copies the history in.
  - *Why pin the image by digest?* A tag like `24.04` can move; a digest cannot, so the build is repeatable.
- Study further: git subtree internals, Docker image digests and layer caching, CMake build types (Release vs Debug).

## M1 — Event contracts and symbol config (2026-10-31)
- What I built: versioned JSON Schemas for the `orders` and `trades` Kafka messages, a symbol registry (`config/symbols.yaml`) with tick and lot sizes, and 49 contract tests that run in a pinned Python container.
- Why it is designed this way: events carry integer ticks and lots and the engine converts to its internal fixed point at the edge ([ADR 0004](decisions/0004-prices-in-ticks-quantities-in-lots.md)); trades are identified by (engine_run_id, match_id) so duplicates can be removed after restarts ([ADR 0005](decisions/0005-trade-identity-run-id-plus-match-id.md)); trades include the aggressor side for order-flow analytics ([ADR 0006](decisions/0006-aggressor-side-in-trades.md)).
- Trade-off I can defend: the engine needs a conversion layer (ticks ↔ engine units) instead of using ticks natively, but matching code stays untouched and proven.
- Numbers: none measured (contract milestone).
- Likely questions:
  - *Why integers for prices?* Binary floats cannot represent most decimals exactly (0.1 + 0.2 ≠ 0.3); two equal prices might not compare equal.
  - *Your validator rejects 18990.0. Isn't that a valid JSON integer?* By the JSON Schema spec it is; I made the validator strict because a float on the wire means a producer is computing prices in floating point.
  - *How do you guarantee no duplicate trades downstream?* Delivery is at-least-once, so processing is idempotent on the unique key (engine_run_id, match_id).
  - *Why are some ids capped at 2^53?* JavaScript and many JSON parsers use doubles; above 2^53 integers silently lose digits. Nanosecond timestamps exceed it, so consumers must read them as BigInt or strings.
- Study further: IEEE 754, fixed-point arithmetic, JSON Schema conditionals, idempotency and delivery semantics, schema evolution.

## M2 — Engine benchmark, event time and tests (2026-10-31)
- What I built: a latency benchmark for the C++ matching engine that times every request with `rdtscp` and reports p50–p99.9 per request type plus throughput; UTC event time on every outgoing message; a golden replay test that pins matching behaviour across 300,000 requests.
- Why it is designed this way: percentiles per request type, not averages, because the tail is what traders feel ([ADR 0010](decisions/0010-latency-benchmark-method.md)); the order book keeps a deterministic logical clock and wall time is read once per request outside it, so replays stay identical ([ADR 0009](decisions/0009-event-time-read-once-per-request.md)); a small test suite where the golden test is the safety net for optimisation ([ADR 0011](decisions/0011-minimal-engine-test-suite.md)).
- Trade-off I can defend: the benchmark is closed-loop, so it measures service time, not queueing under load (coordinated omission); end-to-end open-loop measurement comes with the Kafka path.
- Numbers (Ryzen 5 5500U laptop, Docker on WSL2, GCC 13.3 -O3, pinned to one core, 2 M requests after 200 k warm-up, five runs): p50 331–371 ns, p99 1.9–2.1 µs, p99.9 ≈ 8.4 µs per request; 1.8–2.3 M requests/s single-threaded; trades are the slow path (p99 5.2 µs); one wall-clock read ≈ 50 ns. Sources: `benchmarks/results/2026-10-30-2bcee71.md`, `benchmarks/results/2026-10-30-bf9943f.md`.
- Likely questions:
  - *Why not report the average?* Latency has a long tail; the mean (504 ns) describes almost no real request. p99 says what 1 in 100 requests experiences.
  - *How do you know the timer isn't distorting the numbers?* I measure two back-to-back rdtscp reads (~40 ns) and subtract it; throughput is measured in a separate run with no timers.
  - *Your max latency is 4 ms. Is the engine slow?* No: max values are OS and VM preemption (Docker inside WSL2 under Windows); p99.9 is 8 µs. Production would isolate cores and run on bare metal.
  - *How would you make it faster without breaking it?* Profile, change one data structure at a time, and require the golden replay test to pass unchanged plus a before/after benchmark.
- Study further: rdtsc/rdtscp and invariant TSC, coordinated omission, CPU caches, vDSO clock_gettime, golden/snapshot testing, ThreadSanitizer.

## M3 — Local Kafka (2026-10-31)
- What I built: a single-node Kafka 4.3 cluster in KRaft mode (no ZooKeeper) with `orders` and `trades` topics keyed by symbol, one-command start/stop, a round-trip check that proves per-symbol ordering, CLI shortcuts, and a repeatable Kafka benchmark.
- Why it is designed this way: official `apache/kafka` image pinned by digest, 3 partitions, separate internal and external listeners, CLI first with an opt-in UI to save memory on a 3.7 GB VM ([ADR 0012](decisions/0012-local-kafka-single-node-kraft.md)).
- Trade-off I can defend: replication factor 1 and a single controller, so no fault tolerance; acceptable locally, production needs 3 brokers and RF 3.
- Numbers (Ryzen 5 5500U, Docker on WSL2, Kafka perf tools inside the broker container, real 138–248-byte order JSON, acks=all, 3 runs each): producer ~80k msg/s with defaults, ~235k msg/s with linger.ms=10 + 64 KB batches + lz4 (worst tuned run beat best default run); p50 1 ms / p99 13 ms at a paced 20k msg/s; consumer ~303k msg/s. Source: `benchmarks/results/2026-10-31-kafka-98ba5d3.md`.
- Likely questions:
  - *How do you keep orders for one symbol in sequence?* The message key is the symbol; Kafka hashes the key to pick the partition, and order is guaranteed within a partition.
  - *What problem did you find with 3 partitions?* Both symbols hash to the same partition, a hot partition; ordering is fine but there is no parallelism until more symbols exist.
  - *Why is batching so much faster?* Each broker request has fixed overhead; batching and compressing many small messages per request amortises it, at the cost of a few ms of latency.
  - *Where is the bottleneck end to end?* Kafka: ~1 ms and ~10^5 msg/s versus the engine's ~0.35 µs and ~2×10^6 requests/s on the same laptop.
- Study further: Kafka log design, consumer group rebalancing, Raft, producer batching and idempotence, partitioning strategies.

## M4 — Order simulator (2026-10-31)
- What I built: a Python order simulator that models market makers and noise traders over six symbols, sends every order type plus cancels and modifies as `order.v1` JSON to Kafka, paced open-loop at a chosen rate, with an idempotent producer.
- Why it is designed this way: confluent-kafka (librdkafka, the same client as the engine), per-participant order ids matching the engine's 24/40-bit packing, six symbols chosen to spread 2 per partition, pure order-flow model separated from Kafka so it is testable and seeded ([ADR 0013](decisions/0013-order-simulator.md)).
- Trade-off I can defend: open loop, so some cancels target already-filled orders and get rejected, which is realistic; and I amended `order.v1` in place instead of bumping the version because nothing had used it yet, with explicit approval logged.
- Numbers (Ryzen 5 5500U, Docker on WSL2, Python 3.12, librdkafka 2.16.0, acks=all, idempotent, lz4): 32k–46k orders/s max send rate over six 500k-order runs, 0 errors; partitions 33.4/33.4/33.3%. Sources: `benchmarks/results/2026-10-31-sim-*.md`.
- Likely questions:
  - *What bug did you find?* librdkafka hashes keys with CRC32 by default, the Java client with murmur2, so the same symbol landed on different partitions depending on the client. That breaks per-symbol ordering across producers; I set `partitioner=murmur2_random` everywhere and made it a project rule.
  - *Why an idempotent producer?* Retries after a timeout could otherwise duplicate an order or let a cancel overtake its add.
  - *Why is the simulator slower than Kafka's own perf tool?* It is Python-bound: building and JSON-encoding each order costs more than sending it.
  - *How do you know every generated order is valid?* A test generates 10,000 orders and validates each against the JSON Schema; caching the validator cut that test from 137 s to 4 s.
- Study further: librdkafka partitioners, idempotent producers (KIP-98), asynchronous produce and delivery callbacks, open-loop load generation, market making basics.
