"""Silver benchmark (make silver-bench): how long a silver run takes, replaying the real bronze table.

Uses whatever bronze holds (fill it with the simulator, engine and make stream-bronze first) and
writes to its own tables (vortex.silver_bench*), dropped at the end. Three experiments:

  incremental   replay bronze's snapshots in order, one silver run per ~ROWS_PER_RUN new rows:
                does a run get slower as silver grows (the MERGE joins new rows with silver)?
  full          one run over all of bronze into an empty silver (first run, or a rebuild)
  full rerun    the same again into the now full silver: every key exists, nothing is inserted
                (the worst case of crash recovery: the duplicate check over everything)

usage: silver_bench.py <output.md>      env ROWS_PER_RUN (default 100000), COMMIT, BENCH_ENV
"""

import os
import platform
import sys
from datetime import datetime, timezone

from silver_trades import BRONZE, load_symbol_names, run, tables
from vortex_streaming import spark_session

ROWS_PER_RUN = int(os.environ.get("ROWS_PER_RUN", "100000"))


def drop(spark, namespace):
    for table in tables(namespace).values():
        spark.sql(f"DROP TABLE IF EXISTS {table} PURGE")


def bronze_appends(spark):
    """Bronze's append snapshots that added rows, oldest first, with the rows each added.

    An empty micro-batch commits a snapshot with no added-records at all, so missing counts as 0.
    """
    return spark.sql(f"""
        SELECT snapshot_id, CAST(coalesce(summary['added-records'], '0') AS BIGINT) AS rows
        FROM {BRONZE}.snapshots WHERE operation = 'append' ORDER BY committed_at
    """).where("rows > 0").collect()


def cpu_name():
    try:
        with open("/proc/cpuinfo", encoding="utf-8") as f:
            return next(line.split(":", 1)[1].strip() for line in f if line.startswith("model name"))
    except (OSError, StopIteration):
        return platform.processor() or "unknown"


def main(argv):
    out = argv[1]
    spark = spark_session("vortex-silver-bench")
    spark.sparkContext.setLogLevel("ERROR")
    symbols = load_symbol_names()
    snapshots = bronze_appends(spark)
    total = sum(s["rows"] for s in snapshots)
    if total == 0:
        sys.exit("bronze is empty: run the simulator, the engine and make stream-bronze first")
    days = spark.sql(f"SELECT count(DISTINCT CAST(event_ts AS DATE)) AS n FROM {BRONZE}").first()["n"]

    # incremental: one run per ~ROWS_PER_RUN bronze rows, in commit order
    drop(spark, "silver_bench")
    incremental = []
    pending = 0
    for index, snapshot in enumerate(snapshots):
        pending += snapshot["rows"]
        if pending < ROWS_PER_RUN and index < len(snapshots) - 1:
            continue
        silver_rows = spark.table(tables("silver_bench")["trades"]).count() if incremental else 0
        result = run(spark, "silver_bench", symbols=symbols, until_snapshot=snapshot["snapshot_id"])
        result["silver_rows_before"] = silver_rows
        incremental.append(result)
        print(f"incremental run {len(incremental)}: {result['bronze_rows_read']} rows into "
              f"{silver_rows} in {result['seconds']} s", flush=True)
        pending = 0
    drop(spark, "silver_bench")

    # full, then a full rerun over the result
    drop(spark, "silver_bench_full")
    full = run(spark, "silver_bench_full", symbols=symbols, until_snapshot=snapshots[-1]["snapshot_id"])
    print(f"full: {full['bronze_rows_read']} rows in {full['seconds']} s", flush=True)
    rerun = run(spark, "silver_bench_full", symbols=symbols, until_snapshot=snapshots[-1]["snapshot_id"], full=True)
    print(f"full rerun: {rerun['bronze_rows_read']} rows, {rerun['silver_inserted']} inserted, in {rerun['seconds']} s", flush=True)
    files = spark.sql(f"SELECT count(*) AS n, sum(file_size_in_bytes) AS b FROM {tables('silver_bench_full')['trades']}.files").first()
    drop(spark, "silver_bench_full")

    def per_s(result):
        return f"{result['bronze_rows_read'] / result['seconds']:,.0f}"

    lines = [
        f"# Silver benchmark — {datetime.now(timezone.utc):%Y-%m-%d}",
        "",
        "## Conditions",
        "",
        "| | |",
        "|---|---|",
        f"| Commit | `{os.environ.get('COMMIT', 'unknown')}` |",
        f"| CPU | {cpu_name()} |",
        f"| Environment | {os.environ.get('BENCH_ENV', 'unspecified')} |",
        "| Spark | 4.1.3, local[2], driver 768 MB; Iceberg 1.12.0, REST catalog, SeaweedFS |",
        f"| Bronze | {total:,} rows in {len(snapshots)} append snapshots, {days} event day(s) |",
        "| Job | `silver_trades.run`: incremental read, dedup, insert-only MERGE into silver, rejects MERGE, progress row |",
        "",
        f"## Incremental: one run per ~{ROWS_PER_RUN:,} new bronze rows",
        "",
        "| Run | Silver rows before | Bronze rows read | Inserted | Duplicates in batch | Rejected | Seconds | Rows/s |",
        "|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for number, r in enumerate(incremental, 1):
        lines.append(f"| {number} | {r['silver_rows_before']:,} | {r['bronze_rows_read']:,} | {r['silver_inserted']:,} | "
                     f"{r['duplicates_in_batch']:,} | {r['rejected_inserted']:,} | {r['seconds']:.1f} | {per_s(r)} |")
    lines += [
        "",
        "## Full load and full rerun",
        "",
        "| Run | Bronze rows read | Inserted | Seconds | Rows/s |",
        "|---|---:|---:|---:|---:|",
        f"| full, empty silver | {full['bronze_rows_read']:,} | {full['silver_inserted']:,} | {full['seconds']:.1f} | {per_s(full)} |",
        f"| full rerun, every key already in silver | {rerun['bronze_rows_read']:,} | {rerun['silver_inserted']:,} | {rerun['seconds']:.1f} | {per_s(rerun)} |",
        "",
        f"Silver after the full load: {files['n']} data files, {(files['b'] or 0) / 1e6:.1f} MB.",
    ]
    with open(out, "w", encoding="utf-8", newline="\n") as f:
        f.write("\n".join(lines) + "\n")
    print("written to", out, flush=True)
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
