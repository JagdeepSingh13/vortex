"""Silver: Iceberg `vortex.bronze.trades` -> `vortex.silver.trades`, each trade exactly once (M7, ADR 0018).

One run (make silver; Airflow every few minutes from M9):

  1. find the bronze snapshots this run covers: from the one the last run processed
     (silver.trades_progress) to bronze's current snapshot
  2. read only the rows appended in between (Iceberg incremental read); first run, or the
     last snapshot was expired: read all of bronze as of the current snapshot
  3. split them: valid trades, and rejects with a reason (did not parse, unknown symbol, ...)
  4. keep one row per trade key (engine_run_id, symbol, match_id): the lowest Kafka position
  5. MERGE INTO silver.trades ... WHEN NOT MATCHED THEN INSERT: a key already in silver is skipped
  6. MERGE the rejects on their Kafka position, then append one progress row

Steps 5 and 6 are separate commits. A crash in between only makes the next run read the same
bronze rows again, and both MERGEs skip what is already there: rerunning is always safe.

Integers stay integers (hard rule). event_time_ns is the engine's; silver_time_ns is when this
run started, kept in its own column.
"""

import argparse
import os
import sys
import time

from pyspark.sql import Window
from pyspark.sql import functions as F

from vortex_streaming import CATALOG, spark_session

BRONZE = f"{CATALOG}.bronze.trades"
KEY = ["engine_run_id", "symbol", "match_id"]
TRADE_COLUMNS = [
    "engine_run_id", "symbol", "match_id", "price_ticks", "qty_lots",
    "buy_participant_id", "buy_order_id", "sell_participant_id", "sell_order_id",
    "aggressor_side", "sequence_number", "event_time_ns", "event_ts",
]
DEFAULT_SYMBOLS_FILE = os.environ.get("SYMBOLS_FILE", "/opt/vortex/config/symbols.yaml")


def load_symbol_names(path=DEFAULT_SYMBOLS_FILE):
    """The symbol names from config/symbols.yaml.

    The Spark image has no YAML library and the names are all silver needs, so this reads the
    `- symbol: NAME` lines of our own file instead of adding a dependency (ADR 0018).
    """
    names = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            text = line.strip()
            if text.startswith("- symbol:"):
                names.append(text.split(":", 1)[1].strip().strip("\"'"))
    if not names:
        raise ValueError(f"no symbols found in {path}")
    return names


def tables(namespace):
    base = f"{CATALOG}.{namespace}"
    return {"trades": f"{base}.trades", "rejected": f"{base}.trades_rejected", "progress": f"{base}.trades_progress"}


def create_tables(spark, namespace):
    t = tables(namespace)
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{namespace}")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {t['trades']} (
            engine_run_id          STRING  COMMENT 'unique key: (engine_run_id, symbol, match_id)',
            symbol                 STRING,
            match_id               BIGINT,
            price_ticks            BIGINT  COMMENT 'integer ticks, never a float',
            qty_lots               BIGINT,
            buy_participant_id     BIGINT,
            buy_order_id           BIGINT,
            sell_participant_id    BIGINT,
            sell_order_id          BIGINT,
            aggressor_side         STRING  COMMENT 'BUY or SELL',
            sequence_number        BIGINT,
            event_time_ns          BIGINT  COMMENT 'set by the engine, UTC ns since epoch',
            event_ts               TIMESTAMP COMMENT 'event_time_ns at microsecond precision, for partitioning',
            bronze_kafka_partition INT     COMMENT 'lineage: the bronze row this trade came from',
            bronze_kafka_offset    BIGINT,
            bronze_ingest_time_ns  BIGINT,
            silver_time_ns         BIGINT  COMMENT 'start of the silver run that wrote the row, UTC ns'
        )
        USING iceberg
        PARTITIONED BY (symbol, days(event_ts))
        TBLPROPERTIES ('format-version' = '2', 'write.parquet.compression-codec' = 'zstd')
    """)
    # one file per (symbol, day) per run, sorted by time inside: a time range reads few row groups
    spark.sql(f"ALTER TABLE {t['trades']} WRITE LOCALLY ORDERED BY event_time_ns")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {t['rejected']} (
            bronze_kafka_partition INT,
            bronze_kafka_offset    BIGINT  COMMENT '(partition, offset) is unique per row',
            kafka_timestamp        TIMESTAMP,
            reason                 STRING,
            raw_value              STRING  COMMENT 'the Kafka message exactly as received',
            bronze_ingest_time_ns  BIGINT,
            silver_time_ns         BIGINT
        )
        USING iceberg
        PARTITIONED BY (days(kafka_timestamp))
        TBLPROPERTIES ('format-version' = '2')
    """)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {t['progress']} (
            run_started_ns       BIGINT,
            mode                 STRING  COMMENT 'incremental, or full (first run, expired snapshot, --full)',
            bronze_snapshot_id   BIGINT  COMMENT 'bronze is processed up to and including this snapshot',
            bronze_rows_read     BIGINT,
            valid_rows           BIGINT,
            duplicates_in_batch  BIGINT,
            silver_inserted      BIGINT,
            rejected_inserted    BIGINT,
            seconds              DOUBLE
        )
        USING iceberg
        TBLPROPERTIES ('format-version' = '2')
    """)


def current_snapshot(spark, table):
    row = spark.sql(f"SELECT snapshot_id FROM {table}.refs WHERE name = 'main'").first()
    return row["snapshot_id"] if row else None


def last_processed_snapshot(spark, progress_table):
    row = spark.sql(f"SELECT bronze_snapshot_id FROM {progress_table} ORDER BY run_started_ns DESC LIMIT 1").first()
    return row["bronze_snapshot_id"] if row else None


def snapshot_exists(spark, table, snapshot_id):
    return spark.sql(f"SELECT 1 FROM {table}.snapshots WHERE snapshot_id = {int(snapshot_id)}").first() is not None


def read_bronze(spark, bronze, start, end):
    """Rows appended after snapshot `start` up to and including `end`; all rows as of `end` if start is None."""
    reader = spark.read.format("iceberg")
    if start is None:
        return reader.option("versionAsOf", str(end)).load(bronze)
    return reader.option("start-snapshot-id", str(start)).option("end-snapshot-id", str(end)).load(bronze)


def reject_reason(symbols):
    """Why a bronze row cannot be a silver trade; null if it is a valid trade."""
    return (
        F.when(~F.col("parse_ok") & F.col("schema_version").isNotNull() & (F.col("schema_version") != 1),
               "wrong_schema_version")
        .when(~F.col("parse_ok"), "unparseable_or_missing_field")
        .when(~F.col("symbol").isin(symbols), "unknown_symbol")
        .when(F.col("match_id") < 1, "match_id_not_positive")
        .when(F.col("price_ticks") < 1, "price_not_positive")
        .when(F.col("qty_lots") < 1, "qty_not_positive")
        .when(~F.col("aggressor_side").isin("BUY", "SELL"), "bad_aggressor_side")
    )


def classify(bronze_rows, symbols):
    """(valid trades with lineage columns, rejected rows with a reason), both still with duplicates."""
    rows = bronze_rows.withColumn("reason", reject_reason(symbols))
    valid = rows.where(F.col("reason").isNull()).select(
        *TRADE_COLUMNS,
        F.col("kafka_partition").alias("bronze_kafka_partition"),
        F.col("kafka_offset").alias("bronze_kafka_offset"),
        F.col("ingest_time_ns").alias("bronze_ingest_time_ns"),
    )
    rejected = rows.where(F.col("reason").isNotNull()).select(
        F.col("kafka_partition").alias("bronze_kafka_partition"),
        F.col("kafka_offset").alias("bronze_kafka_offset"),
        "kafka_timestamp", "reason", "raw_value",
        F.col("ingest_time_ns").alias("bronze_ingest_time_ns"),
    )
    return valid, rejected


def first_per_key(valid):
    """One row per trade key: the copy that reached Kafka first (lowest partition, then offset)."""
    order = Window.partitionBy(*KEY).orderBy("bronze_kafka_partition", "bronze_kafka_offset")
    return valid.withColumn("_n", F.row_number().over(order)).where("_n = 1").drop("_n")


def added_records(spark, table, before):
    """Rows a commit added: 0 if the table has no new snapshot since `before`."""
    after = current_snapshot(spark, table)
    if after is None or after == before:
        return 0
    row = spark.sql(f"SELECT summary['added-records'] AS n FROM {table}.snapshots WHERE snapshot_id = {after}").first()
    return int(row["n"] or 0)


def run(spark, namespace="silver", bronze=BRONZE, symbols=None, until_snapshot=None, full=False):
    """One silver run. Returns its counts as a dict (also stored in trades_progress)."""
    started = time.time()
    run_started_ns = time.time_ns()
    t = tables(namespace)
    create_tables(spark, namespace)
    symbols = symbols or load_symbol_names()

    end = until_snapshot or current_snapshot(spark, bronze)
    start = None if full else last_processed_snapshot(spark, t["progress"])
    mode = "incremental"
    if start is not None and not snapshot_exists(spark, bronze, start):
        print(f"silver: bronze snapshot {start} no longer exists (expired); reading all of bronze", flush=True)
        start = None
    if start is None:
        mode = "full"
    if end is None or end == start:
        return {"mode": mode, "bronze_snapshot_id": end, "nothing_new": True}

    batch = read_bronze(spark, bronze, start, end).persist()
    rows_read = batch.count()
    valid, rejected = classify(batch, symbols)
    stamp = F.lit(run_started_ns).alias("silver_time_ns")
    trades = first_per_key(valid).select("*", stamp).persist()
    rejects = rejected.select("*", stamp).persist()
    valid_rows = valid.count()
    unique_rows = trades.count()

    trades.createOrReplaceTempView("silver_batch_trades")
    rejects.createOrReplaceTempView("silver_batch_rejects")

    # insert-only MERGE: Spark turns it into "anti-join with silver, then append", so existing
    # files are never rewritten. event_ts is equal for equal keys; it lets the scan of silver
    # skip the files of other days.
    before = current_snapshot(spark, t["trades"])
    spark.sql(f"""
        MERGE INTO {t['trades']} AS s
        USING silver_batch_trades AS b
        ON s.engine_run_id = b.engine_run_id AND s.symbol = b.symbol AND s.match_id = b.match_id
           AND s.event_ts = b.event_ts
        WHEN NOT MATCHED THEN INSERT *
    """)
    inserted = added_records(spark, t["trades"], before)

    before = current_snapshot(spark, t["rejected"])
    spark.sql(f"""
        MERGE INTO {t['rejected']} AS r
        USING silver_batch_rejects AS b
        ON r.bronze_kafka_partition = b.bronze_kafka_partition AND r.bronze_kafka_offset = b.bronze_kafka_offset
        WHEN NOT MATCHED THEN INSERT *
    """)
    rejected_inserted = added_records(spark, t["rejected"], before)

    result = {
        "run_started_ns": run_started_ns,
        "mode": mode,
        "bronze_snapshot_id": end,
        "bronze_rows_read": rows_read,
        "valid_rows": valid_rows,
        "duplicates_in_batch": valid_rows - unique_rows,
        "silver_inserted": inserted,
        "rejected_inserted": rejected_inserted,
        "seconds": round(time.time() - started, 3),
    }
    spark.createDataFrame([result], spark.table(t["progress"]).schema).writeTo(t["progress"]).append()

    for frame in (batch, trades, rejects):
        frame.unpersist()
    return result


def describe(result):
    if result.get("nothing_new"):
        return f"SILVER nothing new (bronze snapshot {result['bronze_snapshot_id']})"
    already = result["valid_rows"] - result["duplicates_in_batch"] - result["silver_inserted"]
    return (f"SILVER mode={result['mode']} bronze_snapshot={result['bronze_snapshot_id']} "
            f"rows_read={result['bronze_rows_read']} valid={result['valid_rows']} "
            f"duplicates_in_batch={result['duplicates_in_batch']} already_in_silver={already} "
            f"inserted={result['silver_inserted']} rejected={result['rejected_inserted']} "
            f"seconds={result['seconds']}")


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--namespace", default="silver")
    parser.add_argument("--bronze", default=BRONZE)
    parser.add_argument("--full", action="store_true", help="ignore progress and read all of bronze (safe: MERGE)")
    args = parser.parse_args(argv)

    spark = spark_session("vortex-silver-trades")
    spark.sparkContext.setLogLevel("WARN")
    print(describe(run(spark, args.namespace, args.bronze, full=args.full)), flush=True)
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
