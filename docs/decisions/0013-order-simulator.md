# 0013. Order simulator

- Status: accepted
- Date: 2026-10-31
- Milestone: M4

## Context

Until the Order API exists (M8), something has to feed realistic `order.v1` messages into Kafka: every order type, cancels and modifies, prices moving over time, at a controllable rate. It must also respect the contracts: integer ticks and lots, key = symbol.

## Options

Client library:
1. **confluent-kafka**: Python wrapper over librdkafka, the C library the engine will also use (M5). Fastest Python option, idempotent producer.
2. kafka-python: pure Python, slower, patchy maintenance history.
3. aiokafka: asyncio; unnecessary complexity for a generator.

Symbols: keep AAPL + MSFT (both hash to partition 0 of 3), **6 symbols chosen to spread 2 per partition**, or 8 (3/3/2).

Order id uniqueness across producers (simulator now, API in M8):
1. **Unique per participant**; the engine identifies an order by (`participant_id`, `order_id`), as its TCP gateway already does by packing 24 + 40 bits.
2. Globally unique: needs coordination between producers.
3. Random 53-bit ids: rare collisions, ids carry no meaning.

Runtime: **container on the compose network** (pinned Python 3.12 image) or host Python.

## Decision

- confluent-kafka 2.16.0 (2.16.1 was under two weeks old), in the pinned `python-tools` image; `make sim`.
- Symbols AAPL, MSFT, GOOGL, NFLX, AMZN, NVDA, with a `reference_price` per symbol for the simulator's starting mid.
- Order ids unique per participant. `order.v1` was **amended in place** without a version bump: `order_id` ≤ 2^40 − 1, `participant_id` ≤ 2^24 − 1, uniqueness per participant. This bends the "schema change needs a version bump" rule; Jagdeep approved it explicitly on 2026-10-31 because no producer or consumer of `order.v1` existed yet. Logged in `docs/ARCHITECTURE.md`.
- Producer: `acks=all`, `enable.idempotence=true`, `linger.ms=5`, lz4, and **`partitioner=murmur2_random`** (see below).
- Each run picks a random participant id range (or `--participant-base`), so restarting the simulator never reuses a (participant, order id) pair still resting in the book.
- Open-loop pacing (`--rate`), seeded generator (`--seed`).

## The partitioner finding

The first run against the broker put NFLX on partition 0 and left partition 2 empty, although Kafka's hash predicted 1 and a 2/2/2 spread. Cause: **librdkafka's default partitioner (`consistent_random`) hashes keys with CRC32; the Java client and the Kafka CLI tools use murmur2.** The same key lands on different partitions depending on which client sent it. With the Java API (M8) and the simulator both sending AAPL orders, AAPL would be split across two partitions and per-symbol ordering would break. Fix: `partitioner=murmur2_random` in every librdkafka producer (simulator now, engine in M5). Verified: 12,000 orders landed exactly AAPL/MSFT → 0, GOOGL/NFLX → 1, AMZN/NVDA → 2. Added to the hard rules in `CLAUDE.md`.

## Consequences

- Measured max send rate: 32k–46k orders/s across six 500k-order runs (Python-bound: building and JSON-encoding each order), 0 errors, partitions within 0.1% of an even split. See `benchmarks/results/2026-10-31-sim-*.md`.
- Open loop: the simulator never sees trades, so some cancels and modifies target orders that already filled; the engine rejects those, as it would for a real client whose cancel races a fill.
- One test (`make test-sim`) validates 10,000 generated orders against `order.v1`, checks every symbol, action and order type appears, ids are unique per participant, and the stream is deterministic for a seed.
