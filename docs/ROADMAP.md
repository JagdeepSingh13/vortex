# Roadmap

One milestone at a time, one branch per milestone (`mN-short-name`). A milestone is done when every acceptance criterion below passes on a clean clone. Status: **draft, written in M0, revise as we learn**.

| # | Milestone | Branch |
|---|-----------|--------|
| M0 | Repository bootstrap | `m0-bootstrap` |
| M1 | Event contracts and symbol config | `m1-contracts` |
| M2 | Engine test suite and benchmark harness | `m2-engine-tests` |
| M2.5 | Engine performance optimisation (**deferred**, optional) | `m2.5-engine-perf` |
| M3 | Local Kafka (KRaft) | `m3-local-kafka` |
| M4 | Order simulator | `m4-simulator` |
| M5 | Engine Kafka I/O (orders in, trades out) | `m5-engine-kafka` |
| M6 | Bronze ingestion into Iceberg | `m6-bronze` |
| M7 | Silver tables | `m7-silver` |
| M8 | Order API and live prices (Spring Boot + Redis) | `m8-api` |
| M9 | dbt gold models and Airflow | `m9-gold` |
| M10 | AWS via Terraform | `m10-aws` |
| M11 | Dashboard, CI and presentation polish | `m11-polish` |

## M0 — Repository bootstrap
- Folder layout from `CLAUDE.md`, `.gitignore`, `.env.example`, README.
- `cpp_exchange` imported into `engine/` with its history (ADR 0001).
- Engine builds in a pinned Linux container (ADR 0002); toolchain versions recorded (ADR 0003).
- **Acceptance:** on a clean clone, `make engine && make test` succeeds and `engine_smoke_test` passes.

## M1 — Event contracts and symbol config
- `schemas/order.v1.json` and `schemas/trade.v1.json` (JSON Schema draft 2020-12), documented in `docs/ARCHITECTURE.md`.
- `config/symbols.yaml`: symbol, tick size, lot size, display decimals, engine price scale.
- Decisions: ticks and lots in events (ADR 0004), trade identity (ADR 0005), aggressor side (ADR 0006), contract tests in a Python container (ADR 0007).
- **Acceptance:** `make test` passes on a clean clone: every valid example in `schemas/examples/` passes, every invalid example fails for exactly its stated reason, and `symbols.yaml` converts exactly to engine units.

## M2 — Engine test suite and benchmark harness
- GoogleTest (ADR 0008) with a deliberately small suite (ADR 0011): golden replay test for matching behaviour, multi-threaded SPSC queue test; ASan/UBSan and TSan builds in `make test-all`.
- Engine stamps wall-clock `event_time_ns` (UTC epoch) once per request; the logical clock stays for deterministic matching (ADR 0009).
- `make bench` writes a dated file to `benchmarks/results/` with machine and conditions (ADR 0010).
- **Acceptance:** `make test-all` passes on a clean clone; one benchmark result file committed with real measurements.

## M2.5 — Engine performance optimisation
**Status: deferred (2026-10-31).** The engine is already large and complex, so engine code stays as it is for now; pick this up after the pipeline works end to end, if there is time. Numbered 2.5 so later milestone numbers, which ADRs refer to, stay the same.

Findings so far (no code changed):
- `perf` works inside a privileged Docker container on WSL2, including hardware counters (cycles, instructions, cache misses, branch misses).
- In a short 200k-request benchmark run, 38% of `perf record` samples were the kernel zero-filling pages (`clear_page_rep`) while the engine's large pools were touched for the first time: a startup cost, not the matching path. Profiling must sample only after warm-up.

- Profile the engine on the benchmark workload and record where time goes.
- Optimise one hot spot at a time; each change is its own commit with a before/after `make bench` result and, if it is a significant design choice, an ADR.
- Hard constraint: the golden replay test passes **unchanged** after every change (matching behaviour identical).
- **Acceptance:** a profile write-up in `benchmarks/`, at least one optimisation with a measured, repeatable improvement beyond run-to-run noise (latency percentiles ±6%, throughput ±13% on this machine), `make test-all` green on a clean clone.

## M3 — Local Kafka (KRaft)
- `infra/docker/docker-compose.yml` with a single-broker Kafka in KRaft mode, pinned image.
- `make up`, `make down`, `make topics` (`orders`, `trades`, partitions keyed by symbol).
- `make kafka-check` (round trip on a temporary topic, per-key partition check), CLI shortcuts, optional Kafka UI (`make up-ui`), `make kafka-perf` (ADR 0012).
- **Acceptance:** on a clean clone, `make up && make topics && make kafka-check` passes; one Kafka benchmark result committed.

## M4 — Order simulator
- Python simulator (`simulator/`) producing valid `order.v1` events keyed by symbol; configurable rate and symbols.
- `make sim`.
- **Acceptance:** produced messages validate against `order.v1`; key equals symbol.

## M5 — Engine Kafka I/O
- Engine consumes `orders`, publishes `trades` via librdkafka.
- Matching thread hands trades to a publisher thread through a lock-free SPSC queue; no allocation, locks, logging or I/O on the matching thread.
- **Acceptance:** with the simulator running, every trade on `trades` validates against `trade.v1`; matching results equal the in-process run for the same input (test).

## M6 — Bronze ingestion into Iceberg
- MinIO (S3-compatible) + Iceberg REST catalog in docker-compose.
- PySpark Structured Streaming job `trades` → `bronze.trades`, with `ingest_time_ns` separate from `event_time_ns`.
- `make stream-bronze`.
- **Acceptance:** reconciliation test (trades produced = bronze rows); restart the job and confirm no duplicates.

## M7 — Silver tables
- `silver.trades`: deduplicated on `trade_id`, typed, partitioned by symbol and day.
- **Acceptance:** dedup test; deliberately late events land in the correct partition.

## M8 — Order API and live prices
- New Spring Boot service in `api/`: POST orders to Kafka `orders`; consume `trades` into Redis last price; GET live prices.
- **Acceptance:** unit tests for business logic; Testcontainers integration tests for Kafka and Redis.

## M9 — dbt gold models and Airflow
- dbt models: 1-minute candles (OHLCV), VWAP, per-symbol daily stats; integers converted to decimals here.
- Airflow DAGs: scheduled dbt runs, data quality checks, Iceberg maintenance (compaction, snapshot expiry).
- `make dbt`.
- **Acceptance:** dbt tests (not_null, unique, accepted ranges) pass; late-event candle test passes.

## M10 — AWS via Terraform
- `infra/terraform/`: S3 bucket(s) and catalog for the lakehouse; the rest stays local unless we decide otherwise (ADR).
- **Acceptance:** `terraform plan` clean. `apply` only with Jagdeep's explicit approval; cost estimate written first.

## M11 — Dashboard, CI and presentation polish
- Dashboard over gold models; GitHub Actions running `make test`.
- Final architecture diagram, demo script, measured numbers in README.
- **Acceptance:** CI green on `main`; demo runs end to end from a clean clone.
