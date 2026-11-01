"""Silver tests (make test-silver), on their own tables: vortex.bronze_test.trades -> vortex.silver_test.*

Writes hand-made bronze rows, runs the real silver job on them and checks the result:

  1. first run: duplicates of the same trade end up once; bad rows go to trades_rejected
  2. a run with no new bronze rows changes nothing
  3. a later run reads only the new rows, and a key already in silver is not inserted again
  4. a late trade (yesterday's event time, arriving after today was processed) lands in yesterday's partition
  5. a rerun over all of bronze (a crash before progress was saved) inserts nothing twice
  6. an unknown symbol is rejected with its reason
  7. silver-check reconciles bronze, silver and rejects
  8. every silver commit is an append: the insert-only MERGE never rewrites existing files

The test tables are dropped at the start and at the end.
"""

import json
import sys
import time
from datetime import datetime, timedelta, timezone

from pyspark.sql import functions as F

from bronze_trades import create_table_sql
from silver_check import check
from silver_trades import run, tables
from vortex_streaming import CATALOG, spark_session

NAMESPACE = "silver_test"
BRONZE = f"{CATALOG}.bronze_test.trades"
SYMBOLS = ["AAPL", "MSFT"]
RUN = "aaaaaaaaaaaaaaaa"


class Bronze:
    """Appends hand-made rows to the test bronze table; every append is one bronze snapshot."""

    def __init__(self, spark):
        self.spark = spark
        self.offset = 0
        self.schema = spark.table(BRONZE).schema

    def trade(self, symbol, match_id, event_time_ns, price_ticks=19000, qty_lots=10, run=RUN):
        message = {
            "schema_version": 1, "engine_run_id": run, "match_id": match_id, "symbol": symbol,
            "price_ticks": price_ticks, "qty_lots": qty_lots, "buy_participant_id": 1001, "buy_order_id": 7,
            "sell_participant_id": 1002, "sell_order_id": 9, "aggressor_side": "BUY",
            "sequence_number": match_id, "event_time_ns": event_time_ns,
        }
        return self._row(message, parse_ok=True, raw=json.dumps(message), event_time_ns=event_time_ns)

    def unparseable(self, raw):
        return self._row({}, parse_ok=False, raw=raw, event_time_ns=None)

    def _row(self, message, parse_ok, raw, event_time_ns):
        self.offset += 1
        row = {field.name: message.get(field.name) for field in self.schema.fields}
        when = datetime.fromtimestamp(event_time_ns / 1e9, tz=timezone.utc) if event_time_ns else None
        row.update(event_ts=when, parse_ok=parse_ok, raw_value=raw, kafka_partition=0,
                   kafka_offset=self.offset, kafka_timestamp=datetime.now(timezone.utc),
                   ingest_time_ns=time.time_ns())
        return row

    def append(self, rows):
        self.spark.createDataFrame(rows, self.schema).writeTo(BRONZE).append()


def drop_tables(spark):
    for table in [BRONZE, *tables(NAMESPACE).values()]:
        spark.sql(f"DROP TABLE IF EXISTS {table} PURGE")


def main():
    spark = spark_session("vortex-test-silver")
    spark.sparkContext.setLogLevel("ERROR")
    drop_tables(spark)
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.bronze_test")
    spark.sql(create_table_sql(BRONZE))
    bronze = Bronze(spark)
    silver_table = tables(NAMESPACE)["trades"]
    passed, failures = [], []

    def expect(name, condition, detail=""):
        print(f"{'PASS' if condition else 'FAIL'}  {name}{'' if condition else '  ' + str(detail)}", flush=True)
        (passed if condition else failures).append(name)

    def silver_run(**kwargs):
        return run(spark, NAMESPACE, BRONZE, symbols=SYMBOLS, **kwargs)

    now = time.time_ns()
    today_noon = int(datetime.now(timezone.utc).replace(hour=12, minute=0, second=0, microsecond=0).timestamp()) * 10**9
    yesterday = today_noon - 86400 * 10**9

    # 1. duplicates and bad rows
    first = bronze.trade("AAPL", 1, today_noon)
    bronze.append([
        first,
        bronze.trade("AAPL", 2, today_noon + 1000),
        bronze.trade("MSFT", 1, today_noon + 2000),
        dict(bronze.trade("AAPL", 1, today_noon), raw_value=first["raw_value"]),  # same trade, another offset
        bronze.unparseable("this is not json"),
        bronze.trade("AAPL", 1, today_noon, price_ticks=0, run="bbbbbbbbbbbbbbbb"),
    ])
    result = silver_run()
    expect("first run reads everything", result["mode"] == "full" and result["bronze_rows_read"] == 6, result)
    expect("a duplicate in the same batch is kept once",
           result["duplicates_in_batch"] == 1 and result["silver_inserted"] == 3, result)
    reasons = sorted(r["reason"] for r in spark.table(tables(NAMESPACE)["rejected"]).collect())
    expect("bad rows go to trades_rejected with a reason",
           reasons == ["price_not_positive", "unparseable_or_missing_field"], reasons)

    # 2. nothing new
    expect("a run with no new bronze rows does nothing", silver_run().get("nothing_new") is True)

    # 3. incremental, and a key that is already in silver
    bronze.append([bronze.trade("AAPL", 2, today_noon + 1000), bronze.trade("AAPL", 3, today_noon + 3000)])
    result = silver_run()
    expect("the next run reads only the new rows", result["mode"] == "incremental" and result["bronze_rows_read"] == 2, result)
    expect("a key already in silver is not inserted again", result["silver_inserted"] == 1, result)

    # 4. a late trade, after today has been processed
    bronze.append([bronze.trade("MSFT", 2, yesterday)])
    result = silver_run()
    late = spark.table(silver_table).where("symbol = 'MSFT' AND match_id = 2") \
        .select(F.col("_partition.event_ts_day").cast("string").alias("day")).collect()
    expected_day = (datetime.now(timezone.utc) - timedelta(days=1)).date().isoformat()
    expect("a late trade lands in its event-time day",
           result["silver_inserted"] == 1 and [r["day"] for r in late] == [expected_day], (result, late, expected_day))

    # 5. crash before progress was saved: reading everything again inserts nothing
    result = silver_run(full=True)
    expect("a rerun over all of bronze inserts nothing twice",
           result["silver_inserted"] == 0 and result["rejected_inserted"] == 0, result)

    # 6. unknown symbol
    bronze.append([bronze.trade("TSLA", 1, today_noon)])
    silver_run()
    unknown = spark.table(tables(NAMESPACE)["rejected"]).where("reason = 'unknown_symbol'").count()
    expect("an unknown symbol is rejected", unknown == 1, unknown)

    # 7. the reconciliation agrees
    ok, lines = check(spark, NAMESPACE, BRONZE, symbols=SYMBOLS)
    expect("silver-check reconciles", ok and spark.table(silver_table).count() == 5, "\n".join(lines))

    # 8. an insert-only MERGE only adds files: every silver commit is an append, none a rewrite
    operations = sorted({r["operation"] for r in spark.sql(f"SELECT operation FROM {silver_table}.snapshots").collect()})
    expect("every silver commit is an append (no files rewritten)", operations == ["append"], operations)

    drop_tables(spark)
    print(f"RESULT {'OK' if not failures else 'FAIL'}: {len(passed)} passed, {len(failures)} failed "
          f"({(time.time_ns() - now) / 1e9:.0f} s)", flush=True)
    spark.stop()
    return 0 if not failures else 1


if __name__ == "__main__":
    sys.exit(main())
