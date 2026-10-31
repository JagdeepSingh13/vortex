# Real-time market data platform

Portfolio project by Jagdeep Singh (third-year B.E. IT student). A C++ matching engine streams trades through Kafka into an Apache Iceberg lakehouse; a Spring Boot API serves live prices; dbt models and a dashboard turn the data into analytics. It builds on two of his earlier projects (C++ matching engine, AWS/Iceberg/dbt/Airflow pipeline) and adds a new Spring Boot API, forming one original system he will present in internship and placement interviews.

## How to work with Jagdeep (read this first)

This project exists for learning and for interviews. Jagdeep must understand and be able to defend every part of it.

- Work one milestone at a time from `docs/ROADMAP.md`. Start each milestone with a short plan and wait for his go-ahead before writing code.
- For any significant design choice, present 2-3 options with trade-offs and let him decide. Record the decision as an ADR in `docs/decisions/` using `docs/decisions/0000-template.md`.
- At the end of each step, explain in plain language what changed and why, and name any concept worth studying further.
- For C++ hot-path code (ring buffers, the Kafka publisher, matching logic), ask whether he wants to write it himself with you reviewing before you write it.
- When a milestone is done, append an entry to `docs/INTERVIEW_NOTES.md` (format is in that file), and add or update a guide in `docs/learn/` for any new concept it introduced (plain language, diagrams, "check yourself" questions).
- Never invent numbers. Only report latency, throughput or volume figures you actually measured, together with the machine and conditions.
- If a request conflicts with the hard rules below, say so and ask before proceeding.

## Current status

- Milestone: M7 (silver tables). M0 done 2026-10-30, M1–M6 done 2026-10-31, M8 (live prices and candles, done before M7) done 2026-11-01. M2.5 (engine optimisation) deferred: avoid engine code changes beyond what a milestone strictly needs. Update this line whenever a milestone completes.

## Architecture in one screen

```
simulator / Order API (Spring Boot) -> Kafka "orders" -> C++ matching engine -> Kafka "trades"
Kafka "trades" -> Redis live prices (read by the Order API)
Kafka "trades" -> Spark Structured Streaming -> Iceberg bronze/silver (S3) -> dbt gold -> dashboard
Airflow: dbt runs, data quality checks, Iceberg maintenance
```

Full design, event contracts and table layouts: `docs/ARCHITECTURE.md`. Read it before starting any milestone.

## Repository layout

```
engine/          C++ matching engine (imported from Jagdeep's existing repo) + Kafka I/O
simulator/       Python order generator
api/             Spring Boot service (orders in, live prices and candles out)
streaming/       PySpark Structured Streaming jobs
dbt/             dbt project for gold models
airflow/         DAGs
schemas/         Event schemas (JSON Schema first; Avro later)
config/          Shared config, e.g. symbols.yaml (tick size, lot size)
infra/docker/    docker-compose and service config for local runs
infra/terraform/ AWS infrastructure (from M10)
benchmarks/      Benchmark harness output; results/ holds dated result files
docs/            Architecture, roadmap, ADRs, interview notes
```

## Tech stack and constraints

- C++20 with CMake for the engine, built in a pinned Linux container (`infra/docker/engine-build.Dockerfile`); librdkafka for Kafka.
- Java 25 (LTS), Spring Boot (current stable release), Spring for Apache Kafka, Redis.
- Python 3.12 for the simulator and tools; dbt for gold models. Known deviation: PySpark jobs run on the Spark image's Python 3.10 (decided 2026-10-31, ADR 0016); revisit when the streaming jobs grow more Python logic.
- Kafka in KRaft mode (no ZooKeeper). Iceberg tables on S3-compatible storage. Airflow for orchestration.
- Local first: everything runs with docker-compose on a laptop. AWS only from M10, via Terraform.
- Pin exact versions everywhere (images, Maven, pip, CMake dependencies). Check compatibility before pinning: the Iceberg Spark runtime artifact must match the Spark minor version and Scala version.
- Prefer boring, well-documented choices. Explain why before adding any new dependency.

## Hard rules

- Prices and quantities are integers (ticks and lots) in the engine, in events and in bronze and silver tables. Never floats. Convert to decimals only in gold models or the UI.
- The Kafka message key is the symbol for both `orders` and `trades`, so per-symbol ordering holds.
- Every producer must map keys to partitions like the Java client (murmur2). librdkafka clients (Python simulator, C++ engine) set `partitioner=murmur2_random`; its default hashes with CRC32 and would split a symbol across partitions (ADR 0013).
- Event time is `event_time_ns` (UTC nanoseconds since the Unix epoch), set by the engine. Keep ingestion time in a separate column.
- Nothing new on the matching thread: no heap allocation, locks, logging or I/O. Anything leaving the engine goes through a lock-free queue to another thread.
- Do not change matching behaviour without a test proving the old and new behaviour agree.
- Schemas live in `schemas/`. Changing one requires a version bump and a note in `docs/ARCHITECTURE.md`.
- No secrets in git. Use `.env` (gitignored) and keep `.env.example` current.
- Never run `terraform apply`, `terraform destroy`, or anything else that creates paid cloud resources without Jagdeep's explicit approval in the conversation.

## Commands

Make targets are the single entry point. Create each target when a milestone first needs it and keep this list accurate.

- `make help`: list available targets
- `make up` / `make down`: start or stop local infrastructure (`make reset` also deletes data; `make up-ui` adds Kafka UI on :8080)
- `make topics`: create Kafka topics
- `make kafka-check`: Kafka round trip on a temporary topic
- `make kafka-topics` / `kafka-tail` / `kafka-send` / `kafka-groups` / `kafka-offsets`: Kafka CLI shortcuts (`TOPIC=`, `FROM_START=1`)
- `make kafka-perf`: Kafka producer/consumer benchmark, dated file in `benchmarks/results/`
- `make engine`: build the engine in Release mode
- `make engine-run`: run the engine on Kafka, orders in and trades out (Ctrl+C stops cleanly and prints latency summaries)
- `make trades-check`: validate live trades (schema, unique key, per-symbol order, partition)
- `make e2e-bench`: simulator -> engine -> Kafka latency at several rates, dated file in `benchmarks/results/`
- `make test`: engine, contract and simulator tests (`make test-engine`, `make test-schemas`, `make test-sim` run each part)
- `make test-all`: `make test` plus the slow sanitizer builds (`make test-asan`, `make test-tsan`)
- `make bench`: run the engine benchmark and write a dated file to `benchmarks/results/` (`BENCH_REQUESTS`, `BENCH_CPU` override defaults)
- `make sim`: run the order simulator (`RATE=`, `DURATION=`, `COUNT=`, `SEED=`); `make sim-bench` measures its max send rate
- `make test-sim`: simulator produces only valid order.v1 messages (part of `make test`)
- `make stream-bronze`: start the bronze ingestion job (Kafka trades -> Iceberg `vortex.bronze.trades`; restarts resume from the checkpoint)
- `make bronze-check`: reconcile Kafka with bronze (no loss, no duplicates, no gaps)
- `make bronze-restart-test`: kill -9 the bronze job twice mid-stream, then reconcile
- `make bronze-bench`: bronze ingest lag and files per trigger interval, dated file in `benchmarks/results/`
- `make api-build` / `make test-api`: build the Java market data service / run its unit and Testcontainers tests
- `make api-run`: run the market data service on :8090 (Kafka trades -> Redis prices and 1-min candles)
- `make candles`: live candles in the terminal (`SYMBOL=`, `REFRESH=` seconds; needs `make api-run`)
- `make api-bench`: trade -> Redis latency, dated file in `benchmarks/results/`
- `make dbt`: run and test dbt models

## Testing expectations

- C++: kept deliberately small (ADR 0011): the golden replay test guards matching behaviour (it must pass unchanged after any engine change, including optimisations), and a multi-threaded test covers each lock-free queue. Run `make test-all` (sanitizers) before merging engine changes. Benchmarks are separate from tests.
- Java: unit tests for business logic; Testcontainers for Kafka and Redis integration tests.
- Pipelines: reconciliation (trades produced equals rows in bronze), no duplicates after a job restart, tests with deliberately late events for candles.
- dbt: not_null and unique tests on keys; accepted ranges on prices and quantities.

## Git

- Small commits with conventional messages, e.g. `feat(engine): add SPSC trade queue`, `test(streaming): add late-event test`.
- One branch per milestone (e.g. `m3-trade-publisher`), merged when its acceptance criteria in `docs/ROADMAP.md` pass.
