# 0012. Local Kafka: single-node KRaft, 3 partitions, CLI first

- Status: accepted
- Date: 2026-10-31
- Milestone: M3

## Context

Every component talks through Kafka (`orders`, `trades`). Locally it must run on a laptop whose WSL2 VM has about 3.7 GB of memory, start with one command, and leave room for Spark later.

## Options

Image:
1. **`apache/kafka`** (official Apache image): plain Kafka, KRaft-native, small config surface.
2. `confluentinc/cp-kafka`: Confluent Platform build; more tooling around it, more to learn.
3. `bitnami/kafka`: popular in tutorials, but Bitnami's free image catalogue changed in 2025.

Partitions per topic: 1 (total order, no parallelism), **3**, or 6.

Visibility: **Kafka CLI tools** (inside the broker container, no extra memory) vs a web UI (`kafbat/kafka-ui`, ~300–400 MB).

## Decision

- `apache/kafka:4.3.1`, pinned by digest (4.3.2 and 4.4.0 were under two weeks old). One node is both broker and controller (KRaft; Kafka 4.x has no ZooKeeper). Heap fixed at 512 MB.
- Three listeners: `INTERNAL kafka:29092` for containers, `EXTERNAL localhost:9092` for the host, `CONTROLLER :9093` for KRaft.
- `orders` and `trades`: 3 partitions, replication factor 1, 7-day retention, key = symbol. Auto topic creation off.
- CLI first, wrapped in `make` targets (`kafka-topics`, `kafka-tail`, `kafka-send`, `kafka-groups`, `kafka-offsets`). Kafka UI (`kafbat/kafka-ui:v1.5.0`) is opt-in via `make up-ui` (compose profile `ui`).

## Consequences

- Replication factor 1 means no fault tolerance: losing the container's volume loses the data. Fine for local work; a real deployment needs 3 brokers and RF 3.
- **Hot partition today:** with murmur2 hashing, both `AAPL` and `MSFT` map to partition 0 of 3, so partitions 1 and 2 stay empty until more symbols exist. Per-symbol ordering is unaffected.
- Measured on this machine (see `benchmarks/results/2026-10-31-kafka-98ba5d3.md`): roughly 80k msg/s producer with defaults, 235k with batching + lz4, p50 1 ms at a paced 20k msg/s. Kafka, not the engine (~2M requests/s), will limit end-to-end throughput.
- Memory: about 280 MiB idle, about 620 MiB after a benchmark.
