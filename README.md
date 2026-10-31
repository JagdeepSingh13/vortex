# vortex

A real-time market data platform: a C++ matching engine streams trades through Kafka into an Apache Iceberg lakehouse, a Spring Boot API serves live prices, and dbt models plus a dashboard turn the data into analytics.

```
simulator / Order API -> Kafka "orders" -> C++ matching engine -> Kafka "trades"
Kafka "trades" -> Redis live prices
Kafka "trades" -> Spark Structured Streaming -> Iceberg bronze/silver -> dbt gold -> dashboard
```

- Design: [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)
- Plan: [docs/ROADMAP.md](docs/ROADMAP.md) (plain-language version: [docs/MILESTONES.md](docs/MILESTONES.md))
- Decisions: [docs/decisions/](docs/decisions/)
- Learning guides: [docs/learn/](docs/learn/) (how each part works, in plain language)

## Quick start

Requirements: Docker (Desktop or Engine), GNU Make, Git. The engine builds inside a pinned Linux container, so no local C++ toolchain is needed.

```bash
make engine   # build the engine (Release) in the build container
make test     # run unit tests
make run-sim  # run the engine with simulated traders and a live dashboard
```

Local infrastructure (Kafka):

```bash
make up && make topics   # start Kafka (KRaft, single node) and create orders/trades
make kafka-check         # round-trip check
make kafka-tail TOPIC=orders
make down
```

Run `make help` to list all targets.
