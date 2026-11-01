# 0018. Silver trades: incremental batch MERGE from bronze, with a rejects table

- Status: accepted
- Date: 2026-11-01
- Milestone: M7

## Context

Bronze (ADR 0016) holds every Kafka message exactly once, as received, including messages that do not parse. Gold (M9) needs one clean table where each trade appears exactly once, with trustworthy types and values, laid out so that "one symbol, one day" reads few files.

Duplicates can still reach bronze even though the bronze job is exactly once per Kafka offset: the same trade can sit at two offsets (a producer retry without idempotence, a replayed topic, a manual re-send). A trade is identified by `(engine_run_id, symbol, match_id)` (trade.v1, fixed in M5).

Late data: `event_time_ns` is set by the engine, but a trade can reach bronze long after that time (the bronze job was down, Kafka was behind). Silver must put it into the right event-time day even if that day was processed before.

Constraints: a laptop where a second always-on JVM is expensive (M8 showed the Docker VM stalling), and gold will be scheduled by Airflow in M9.

## Options

How silver is built:
1. **Incremental batch MERGE**: a job reads only the bronze rows appended since its last run (Iceberg incremental read between two snapshot ids), removes duplicates inside that batch, and runs `MERGE INTO silver ... WHEN NOT MATCHED THEN INSERT`. Pros: reads only new data; idempotent (a rerun inserts nothing twice); uses no resources between runs; Airflow can schedule it. Cons: fresh only to the schedule (minutes); needs to remember how far it got.
2. **Streaming MERGE**: an always-on Spark stream over bronze, MERGE per micro-batch. Pros: seconds fresh. Cons: another ~1 GB JVM running all the time.
3. **Rebuild affected days**: recompute whole day partitions from bronze and overwrite them. Pros: simplest logic, no state. Cons: every run rewrites all trades of the day, so cost grows with the day's volume.

Bad rows: a **rejects table** with the reason and Kafka position, or only counting them in a check.

## Decision

Option 1, and a rejects table (Jagdeep, 2026-11-01).

- `vortex.silver.trades`: the trade.v1 fields as integers, `event_ts` (event time at µs), lineage back to bronze (`bronze_kafka_partition`, `bronze_kafka_offset`, `bronze_ingest_time_ns`) and `silver_time_ns` (the run that wrote the row). Partitioned by `symbol` and `days(event_ts)`; files sorted by `event_time_ns`. No raw JSON (bronze keeps it).
- A bronze row is valid when it parsed (`parse_ok`), its symbol is in `config/symbols.yaml`, price, quantity and match id are at least 1 and the aggressor side is BUY or SELL. Anything else goes to `vortex.silver.trades_rejected` with a reason, its Kafka position and the raw message.
- Each run: read bronze between the last processed snapshot and the current one; split valid and rejected rows; keep the first row per key (lowest Kafka partition and offset); MERGE insert-only into silver on the key (plus `event_ts`, equal for equal keys, which lets the MERGE skip other days' files); MERGE the rejects on their Kafka position; then append one row to `vortex.silver.trades_progress` with the bronze snapshot id and the counts.
- No progress yet, or the last snapshot was expired: read all of bronze as of the current snapshot. MERGE makes this safe.
- The three commits (silver, rejects, progress) are not one transaction. A crash between them only means the next run reads the same bronze rows again, and the MERGEs insert nothing twice.
- Symbols are read from `config/symbols.yaml` with a small line parser: the Spark image has no YAML library and the names are the only thing needed, so no new dependency.

## Consequences

- Silver is as fresh as the schedule: `make silver` runs it on demand now, Airflow every few minutes in M9.
- A MERGE that inserts only adds new files and never rewrites existing ones, but it still joins the new rows against silver to find existing keys; the benchmark measures how that cost grows with silver's size.
- Rejected rows are visible and countable; `make silver-check` reconciles bronze, silver and rejects as of the snapshot silver last processed.
- When bronze maintenance (compaction, snapshot expiry) arrives in M9, compaction snapshots are skipped by the incremental read, and an expired snapshot falls back to a full, idempotent read.
- The incremental read returns rows added by appends only. If bronze rows were ever deleted or rewritten with new values, silver would not notice; bronze is append-only by design.
