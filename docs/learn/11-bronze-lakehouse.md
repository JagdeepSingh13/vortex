# 11. Bronze: streaming trades into an Iceberg lakehouse

M6 copies every trade from Kafka into a table that analytics tools can query: the **bronze** layer of the lakehouse. This guide explains the pieces (Spark Structured Streaming, Iceberg, the catalog, S3 storage) and how "no trade lost, none duplicated" is guaranteed and proven.

```
Kafka "trades" ─▶ Spark Structured Streaming ─▶ Iceberg table vortex.bronze.trades
 3 partitions      micro-batch every 10 s         ├─ metadata pointer: REST catalog
                   checkpoint: offsets done       └─ files: s3://warehouse/ on SeaweedFS
```

## Why a lakehouse at all

Kafka keeps trades for 7 days and can only be read in order. Analytics needs years of history, SQL queries, and reads by several tools. A **lakehouse** stores data as plain files (Parquet) in cheap object storage (S3), with a **table format** (Iceberg) on top that gives database features: a schema, atomic commits, time travel and safe concurrent writers.

The **medallion** layers:

| Layer | Content | Milestone |
|---|---|---|
| **Bronze** | Every message as received, plus metadata. Never edited. | M6 |
| Silver | Cleaned, typed, deduplicated, validated | M7 |
| Gold | Business tables: candles, VWAP, daily stats | M9 |

Bronze is the safety net: if a bug in silver or gold corrupts data, it can be rebuilt from bronze.

## The pieces

| Piece | What it does | Ours |
|---|---|---|
| **Object storage** | Stores files by key; S3 API | SeaweedFS 4.48 (MinIO stopped publishing images) |
| **Parquet** | Columnar file format: values of one column stored together, compressed | zstd compression |
| **Iceberg** | Table format: tracks which Parquet files make up the table, schema, partitions, snapshots | 1.12.0, format v2 |
| **Catalog** | Maps a table name to its current metadata file; the commit point | Iceberg REST fixture 1.10.1 |
| **Spark Structured Streaming** | Reads Kafka continuously, writes micro-batches | Spark 4.1.3, local mode |

### How an Iceberg commit works

```
catalog: vortex.bronze.trades ──▶ metadata/00009.json   (current)
                                      │ snapshot 9 ──▶ manifest list ──▶ manifests ──▶ data/*.parquet
                                      │ snapshot 8 ──▶ ...                (older snapshots: time travel)
```

A writer writes new Parquet files, then new manifests, then a new metadata file, and finally asks the catalog to **swap the pointer** from the old metadata file to the new one. That swap is atomic: readers see either the old table or the new one, never half a write. If the writer crashes before the swap, its files are simply never referenced (**orphan files**, cleaned up later by maintenance).

## What a bronze row holds

```
typed trade.v1 fields   engine_run_id, symbol, match_id, price_ticks, qty_lots, ... (BIGINT, never floats)
the original message    raw_value      (the JSON exactly as received)
where it came from      kafka_partition, kafka_offset, kafka_timestamp
did it parse            parse_ok       (false: raw_value kept, typed columns may be null)
two separate times      event_time_ns  (engine), ingest_time_ns (this job), as the hard rule requires
partitioning            event_ts       (event time at µs, table partitioned by day)
```

`(kafka_partition, kafka_offset)` identifies each Kafka message, which is what makes "no duplicates" checkable.

## Exactly once, step by step

Kafka delivers at least once; a crash can make Spark read the same messages twice. The combination of two mechanisms turns that into **exactly-once** in the table:

1. Before running batch *N*, Spark writes "batch N = offsets A..B" to its **checkpoint** (a write-ahead log on a volume).
2. It reads A..B from Kafka, writes Parquet files, and commits to Iceberg. The **Iceberg snapshot records the query id and batch id N**.
3. It writes "batch N done" to the checkpoint.

After a crash:

| Crash between | On restart | Result |
|---|---|---|
| 1 and 2 | replays batch N from the same offsets A..B | written once |
| 2 and 3 | replays batch N, the sink sees N already committed, skips it | written once |
| during 2 (files written, no commit) | files are orphans; batch N replays and writes new files | written once, plus orphan files |

### The crash test

`make bronze-restart-test` runs the engine and simulator for 180 s at 3,000 orders/s and kills the bronze job with `SIGKILL` (no clean shutdown, like a crash or an out-of-memory kill) twice, while it is processing, restarting it each time. Then it reconciles every Kafka partition against the table:

```
partition     kafka    bronze  distinct  unparsed  result
        0    121997    121997    121997         0  OK
        1    121643    121643    121643         0  OK
        2    120665    120665    120665         0  OK
RESULT OK
```

364,305 trades, each exactly once. One honest caveat: storage held exactly as many Parquet files as the table referenced, so neither kill happened to land in the middle of writing files (the third row of the table above). The checkpoint replay was proven; the orphan-file case is covered by Iceberg's design but not by this run.

An earlier version of the test killed the job during Spark's 30-second startup, before it had processed anything, so it passed without testing the dangerous moment. Now it waits for a committed batch before each kill. A test that cannot fail is not a test.

## Reconciliation

`make bronze-check` reads the earliest and latest offset of each Kafka partition (with Spark, in batch mode) and checks bronze against them:

- rows == distinct offsets → no duplicates
- first offset == Kafka's earliest → nothing missing at the start
- last offset == Kafka's latest → caught up
- rows == last − first + 1 → no gaps

## Freshness, batches and small files

From [benchmarks/results/2026-10-31-bronze-59c40ec.md](../../benchmarks/results/2026-10-31-bronze-59c40ec.md), 60 s runs after a warm-up batch, two runs each, times in seconds:

| Trigger | Orders/s (trades/s) | Batch time median | Lag p50 | Lag p99 |
|---|---|---:|---:|---:|
| 10 s | 1,000 (~460) | 3.0–4.0 | 5.0 | 9.9 |
| 10 s | 3,000 (~1,430) | 10.1–10.4 | 5.0–6.7 | 12.6–17.7 |
| 2 s | 3,000 (~1,430) | 13.2–16.2 | 7.9–11.2 | 18.3–29.7 |

(lag = engine event → start of the batch that wrote the row; the row is readable once that batch commits)

Two regimes:

- **Light load: the trigger sets freshness.** At 1,000 orders/s a batch takes 3–4 s and the job then waits for the next trigger. A row waits on average half an interval, so p50 ≈ 5 s and p99 ≈ 10 s for a 10 s trigger.
- **Heavy load: processing sets freshness.** At 3,000 orders/s a batch takes 10–16 s, longer than the trigger, so batches run back to back. A 2 s trigger did **not** help; it was worse. The job was busy anyway, and each batch got bigger and slower. Lowering the trigger only helps when there is spare capacity.

So **bronze is the slowest stage of the pipeline**: about 1,500–2,000 trades/s on this laptop (Spark local mode, 2 cores, 1 GB), while the engine produces trades far faster. Where the time goes, from the stream's own log: almost all of it is `addBatch`, writing Parquet plus the Iceberg commit, with a fixed cost of a few seconds per batch. The first batch after a start takes 25–30 s (JVM and catalog warm-up).

The cost side: every batch writes at least one file per partition it touches, so short triggers at light load produce many small files, and many small files make reads slow. That is why table maintenance (compaction, snapshot expiry, orphan cleanup) is part of M9. After all the M6 runs the table had 65 data files for 32.9 MB, about 0.5 MB per file, which is very small for Parquet.

## Running it

```bash
make up && make topics       # Kafka, SeaweedFS, catalog (creates .env and the bucket)
make stream-bronze           # the streaming job (Ctrl+C stops; a restart resumes from the checkpoint)
make engine-run              # another terminal
make sim RATE=2000           # another terminal
make bronze-check            # reconcile Kafka with bronze
make bronze-restart-test     # the crash test (~5 min)
make bronze-bench            # freshness and files per trigger interval (~15 min)
```

## Check yourself

1. Why keep `raw_value` when every field is already parsed?
2. What makes an Iceberg commit atomic?
3. Walk through a crash after the Iceberg commit but before the checkpoint says "done". Why is nothing duplicated?
4. What is an orphan file, and is it dangerous?
5. Why must the checkpoint and the table be deleted together?
6. Why does a 2-second trigger write more files than a 10-second one?
7. The first test version passed while killing Spark during startup. Why was that a bad test?
8. Why is `ingest_time_ns` a separate column from `event_time_ns`?

## Go deeper

- Iceberg spec: snapshots, manifests, and the commit protocol (iceberg.apache.org)
- Spark Structured Streaming programming guide: checkpoints, triggers, fault tolerance
- Parquet: row groups and column chunks; why columnar storage compresses well
- The small-files problem and compaction
