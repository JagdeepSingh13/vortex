# 14. Silver: one clean trades table (dedup, MERGE, late data)

M7 turns bronze ("everything, exactly as received") into **silver**: one table where every trade appears exactly once, with trusted values, laid out for fast reads. Gold (M9) will read only silver. This guide explains how a silver run works, why it is safe to run again, and how late trades land in the right place.

```
vortex.bronze.trades ──make silver──▶ vortex.silver.trades            each trade once, by symbol and day
  every Kafka message                 vortex.silver.trades_rejected   bad rows, with a reason
  (appended every 10 s)               vortex.silver.trades_progress   how far each run got
```

## What changes from bronze to silver

| | Bronze | Silver |
|---|---|---|
| Rows | every Kafka message, duplicates included | each trade once, by key `(engine_run_id, symbol, match_id)` |
| Bad messages | kept, `parse_ok = false` | moved to `trades_rejected` with a reason |
| Checks | none (it must accept anything) | parsed, known symbol, price, quantity and match id ≥ 1, side BUY or SELL |
| Columns | trade fields + raw JSON + Kafka metadata | trade fields + **lineage** (`bronze_kafka_partition`, `bronze_kafka_offset`, `bronze_ingest_time_ns`) + `silver_time_ns` |
| Layout | by day | by **symbol and day**, sorted by time inside each file |
| Units | integers | still integers: decimals only in gold (hard rule) |

**Lineage** means every silver row says exactly which bronze row it came from, so any number in gold can be traced back to the raw Kafka message.

## Why duplicates can exist at all

Bronze is exactly once *per Kafka offset* (M6). But the same trade can sit at **two offsets**: a producer retry without idempotence, a topic replayed by hand, a test that re-sends. Kafka would see two messages; the business sees one trade. So silver deduplicates on the trade's identity, not on its Kafka position.

## One silver run, step by step

[`streaming/silver_trades.py`](../../streaming/silver_trades.py), function `run`:

```
 trades_progress: "last run processed bronze up to snapshot 41"
 bronze now at snapshot 47
                │
 1. read only what was appended in snapshots 42..47   (Iceberg incremental read)
 2. classify: valid trade, or reject + reason
 3. keep one row per key: the lowest (Kafka partition, offset)
 4. MERGE INTO silver.trades ON key  WHEN NOT MATCHED THEN INSERT
 5. MERGE INTO silver.trades_rejected ON (partition, offset)  WHEN NOT MATCHED THEN INSERT
 6. append to trades_progress: "processed up to 47", plus counts and seconds
```

### Step 1: reading only what is new (Iceberg snapshots)

Every commit to an Iceberg table creates a **snapshot**: a numbered version of the table. The bronze stream commits every 10 s, so bronze has a snapshot for every micro-batch. Iceberg can return **only the rows added between two snapshots**:

```python
spark.read.format("iceberg")
     .option("start-snapshot-id", "41")   # exclusive
     .option("end-snapshot-id",   "47")   # inclusive
     .load("vortex.bronze.trades")
```

It reads just the new files, no matter how big bronze grows. The first run (no progress yet) reads all of bronze as of the current snapshot (`versionAsOf`). If the remembered snapshot was expired by maintenance, the run falls back to a full read, which is safe because of step 4.

### Step 3: dedup inside the batch

```python
Window.partitionBy("engine_run_id", "symbol", "match_id").orderBy("bronze_kafka_partition", "bronze_kafka_offset")
→ row_number() == 1
```

If the same trade arrives twice in one batch, the copy that reached Kafka first wins. Which copy wins does not matter for the data (they are equal); a fixed rule makes runs **deterministic**.

### Step 4: MERGE, the duplicate check against everything before

Dedup inside the batch is not enough: the same trade may already be in silver from an earlier run. `MERGE INTO` compares the batch with silver:

```sql
MERGE INTO vortex.silver.trades AS s
USING silver_batch_trades AS b
ON  s.engine_run_id = b.engine_run_id AND s.symbol = b.symbol AND s.match_id = b.match_id
AND s.event_ts = b.event_ts
WHEN NOT MATCHED THEN INSERT *
```

- **Insert-only** (no `WHEN MATCHED`): Spark rewrites it as "anti-join the batch with silver, append what is left". Existing files are **never rewritten**; the test proves every silver commit is an `append`.
- `event_ts` is part of the condition although the key alone decides. Equal keys always have equal event times, and the extra condition lets the join read only silver's files for the days in the batch.
- This is what makes a run **idempotent**: running it twice on the same rows inserts nothing the second time.

### Steps 5 and 6: three commits, no transaction, still safe

Silver, rejects and progress are three separate Iceberg commits. If the job crashes after the silver MERGE but before the progress row, the next run thinks those bronze rows are new and reads them again. Both MERGEs then insert nothing for them. **Safe to rerun** is easier to build than a transaction across three tables, and it is tested (a full rerun over everything inserts 0 rows).

## Late data

A trade's **event time** is when the engine matched it. It can reach bronze much later: the bronze job was down, Kafka was behind. Silver is partitioned by `days(event_ts)`, so the MERGE simply writes the late trade into **its own day's partition**, even if that day was processed long ago:

```
run at 10:00 today   → silver: today/AAPL ← trades 1..1000
late trade arrives   → bronze: match 1001, event time yesterday 23:59
run at 10:05         → silver: yesterday/AAPL ← match 1001      (not "today", where it arrived)
```

Gold (M9) then has to recompute yesterday's candles. That is why silver keeps `silver_time_ns`: gold can find "which days received rows since my last run".

The real pipeline rarely produces late trades on purpose, so the test **injects one**: a bronze row with yesterday's event time, appended after today was processed. It must land in yesterday's partition (checked with Iceberg's `_partition` metadata column).

## Rejects instead of drops

A row that fails a check is not thrown away. It goes to `silver.trades_rejected` with its Kafka position, the raw message and a reason:

| Reason | Example |
|---|---|
| `unparseable_or_missing_field` | not JSON, or a required field missing (bronze `parse_ok = false`) |
| `wrong_schema_version` | `schema_version` is not 1 |
| `unknown_symbol` | a symbol that is not in `config/symbols.yaml` |
| `match_id_not_positive`, `price_not_positive`, `qty_not_positive` | values below 1 |
| `bad_aggressor_side` | not BUY or SELL |

Silent drops hide problems; a rejects table makes them **countable and auditable**.

## Proving it: `make silver-check` and `make test-silver`

[`silver_check.py`](../../streaming/silver_check.py) compares silver with bronze **as of the snapshot silver last processed** (time travel), so a bronze job that keeps writing does not make it fail:

- every valid bronze trade key is in silver (nothing lost), and nothing is in silver that bronze does not have (nothing invented)
- each key once (no duplicates); no nulls
- every rejected bronze row is in `trades_rejected`
- match ids per engine run and symbol have **no gaps**: the engine numbers trades 1, 2, 3, … per symbol, so a hole means a trade went missing

[`test_silver.py`](../../streaming/test_silver.py) runs the real job on hand-made bronze rows in separate test tables: duplicates, bad rows, an empty run, an incremental run, a late trade, a full rerun, an unknown symbol, the reconciliation, and "every commit is an append".

## Results

*(filled from the benchmark at the end of M7)*

## Commands

```bash
make stream-bronze        # terminal 1: Kafka trades -> bronze (with engine and simulator running)
make silver               # one silver run: only the new bronze rows
make silver-check         # reconcile bronze, silver and rejects
make test-silver          # the tests, on their own tables
make silver-bench         # run time vs new rows and silver size, replaying bronze
```

## Check yourself

1. Bronze is already exactly once. Why does silver still need to deduplicate?
2. What is an Iceberg snapshot, and how does silver read only the rows added since its last run?
3. Why is the MERGE insert-only, and what does that mean for the files in silver?
4. The job crashes after writing silver but before writing progress. What happens on the next run, and why is it correct?
5. A trade from yesterday arrives today. Where does it end up in silver, and why?
6. Why keep rejected rows instead of dropping them?
7. Why does `silver-check` compare against bronze *as of a snapshot* instead of bronze now?
8. Why do gaps in match ids mean a trade is missing?
9. Why is `event_ts` in the MERGE condition when the key alone identifies a trade?

## Go deeper

- Iceberg: snapshots, incremental reads, `MERGE INTO` (copy-on-write vs merge-on-read), metadata tables (`snapshots`, `files`, `partitions`, `refs`)
- Idempotent data pipelines; exactly-once vs effectively-once
- Late data and event time vs processing time; how gold recomputes affected windows
- Data lineage and data quality checks
