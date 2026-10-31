# 0016. Bronze ingestion: Spark Structured Streaming into Iceberg on SeaweedFS

- Status: accepted
- Date: 2026-10-31
- Milestone: M6

## Context

Every trade on Kafka must land in an Iceberg table, the bronze layer of the lakehouse, with no trade lost and none duplicated, even if the job crashes. It must run locally next to Kafka on a laptop whose WSL2 VM has about 3.7 GB of memory, with every version pinned and compatible.

## Options

Object storage (S3-compatible):
1. **SeaweedFS**: Apache-2.0, maintained, one container (`weed server -s3`).
2. Garage: very light, AGPL, needs a bootstrap step (layout, keys).
3. MinIO built from source: the community edition stopped publishing images and is now source-only and unmaintained.

Catalog:
1. **Iceberg REST fixture**: the reference REST catalog; one container; its pointers in SQLite on a volume. The REST protocol is what Spark, PyIceberg, DuckDB and Trino speak, so later tools (dbt in M9) can read the same tables. Labelled a test fixture.
2. Apache Polaris: production REST catalog, heavier, OAuth bootstrap.
3. JDBC catalog inside Spark: no service, but only JVM engines could read the tables.

Bronze row shape: **parsed columns + raw message + Kafka metadata**, raw only, or parsed only.

## Decision

- Spark **4.1.3** (`apache/spark:4.1.3-scala2.13-java21-python3-ubuntu`, pinned by digest), the newest Spark that has an Iceberg runtime (Iceberg 1.12.0 supports up to 4.1) and is at least two weeks old. Jars added to the image, each pinned by SHA-256: `spark-sql-kafka-0-10_2.13:4.1.3`, `spark-token-provider-kafka-0-10_2.13:4.1.3`, `kafka-clients:3.9.1` and `commons-pool2:2.12.1` (versions from the connector's POM), `iceberg-spark-runtime-4.1_2.13:1.12.0`, `iceberg-aws-bundle:1.12.0`.
- SeaweedFS 4.48 and the Iceberg REST fixture 1.10.1, both pinned by digest. S3 credentials come from `.env` (gitignored); `make up` creates it from `.env.example`.
- Table `vortex.bronze.trades`: every `trade.v1` field as a typed column (64-bit integers, never floats), plus `raw_value`, `kafka_partition`, `kafka_offset`, `kafka_timestamp`, `parse_ok` and `ingest_time_ns`. Partitioned by `days(event_ts)`, where `event_ts` is `event_time_ns` at microsecond precision. Iceberg format v2, zstd Parquet.
- One Structured Streaming query, Kafka source from the earliest offset, Iceberg sink in append mode, 10-second processing-time trigger, at most 200,000 offsets per batch, `failOnDataLoss=true`, checkpoint on a Docker volume that `make reset` deletes together with the table.

## Why there is no loss and no duplication

- The **checkpoint** records, before a batch runs, which Kafka offsets it covers (write-ahead log), and after it commits, that it is done.
- The **Iceberg sink** stores the query id and batch id in the snapshot it commits. When a batch is replayed after a crash, it sees that batch id is already committed and skips it.
- So a batch is either fully committed once, or not at all and replayed from the same offsets. Proven by `make bronze-restart-test`: the job was killed with `SIGKILL` twice while processing, and afterwards all 364,305 Kafka offsets were in bronze exactly once (no duplicates, no gaps).

## Consequences

- Python in the Spark image is 3.10, not the 3.12 named in `CLAUDE.md`, and 3.10 reaches end of life in October 2026. The job code is a thin PySpark driver; changing it means adding the deadsnakes PPA to the image. Open question for Jagdeep.
- Freshness is bounded by the trigger: a row waits up to one trigger interval for its batch to start, then for the batch to commit. Shorter triggers give fresher data and more, smaller files; compaction belongs to table maintenance (M9).
- Each micro-batch has a fixed Iceberg commit cost of a few seconds on this machine, and the first batch after a start is slow (JVM and catalog warm-up, 25–30 s measured).
- Bronze keeps duplicates that exist in Kafka itself (none are expected from an idempotent producer, but bronze does not deduplicate); silver deduplicates on (`engine_run_id`, `symbol`, `match_id`) in M7.
- The REST fixture is not production software; in M10 the catalog becomes AWS Glue or a managed REST catalog.
