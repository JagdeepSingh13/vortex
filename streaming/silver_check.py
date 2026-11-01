"""Reconcile Iceberg bronze with silver (make silver-check).

Compares silver with bronze *as of the snapshot the last silver run processed*, so a bronze job
that keeps writing does not make the check fail:

    every valid bronze trade key is in silver           nothing lost
    silver has no key that bronze does not have         nothing invented
    each key appears once in silver                     no duplicates
    every rejected bronze row is in trades_rejected     bad data is visible, not dropped
    match ids per (engine run, symbol) have no gaps     no trade missing between two others
    no nulls in silver's columns

Also reports how far silver is behind bronze and silver's freshness (engine event time -> the
silver run that wrote the row). Exit code 1 if anything does not reconcile.
"""

import argparse
import sys

from pyspark.sql import functions as F

from silver_trades import BRONZE, KEY, classify, current_snapshot, last_processed_snapshot, load_symbol_names, read_bronze, tables
from vortex_streaming import spark_session


def check(spark, namespace="silver", bronze=BRONZE, symbols=None):
    """Returns (ok, list of report lines)."""
    t = tables(namespace)
    symbols = symbols or load_symbol_names()
    lines = []
    snapshot = last_processed_snapshot(spark, t["progress"])
    if snapshot is None:
        return False, ["silver has never run: make silver"]

    valid, rejected = classify(read_bronze(spark, bronze, None, snapshot), symbols)
    bronze_keys = valid.select(*KEY).distinct()
    silver = spark.table(t["trades"])
    silver_keys = silver.select(*KEY)

    counts = {
        "bronze valid rows": valid.count(),
        "bronze distinct trades": bronze_keys.count(),
        "silver rows": silver.count(),
        "silver distinct trades": silver_keys.distinct().count(),
        "missing from silver": bronze_keys.join(silver_keys, KEY, "left_anti").count(),
        "in silver, not in bronze": silver_keys.join(bronze_keys, KEY, "left_anti").count(),
        "bronze rejected rows": rejected.count(),
        "rejects table rows": spark.table(t["rejected"]).count(),
    }
    nulls = silver.select([F.sum(F.col(c).isNull().cast("long")).alias(c) for c in silver.columns]).first().asDict()
    null_columns = {c: n for c, n in nulls.items() if n}
    gaps = (
        silver.groupBy("engine_run_id", "symbol")
        .agg(F.min("match_id").alias("lo"), F.max("match_id").alias("hi"), F.countDistinct("match_id").alias("n"))
        .where("n != hi - lo + 1")
        .collect()
    )

    problems = []
    if counts["missing from silver"]:
        problems.append(f"{counts['missing from silver']} bronze trades missing from silver")
    if counts["in silver, not in bronze"]:
        problems.append(f"{counts['in silver, not in bronze']} silver trades not in bronze")
    if counts["silver rows"] != counts["silver distinct trades"]:
        problems.append(f"{counts['silver rows'] - counts['silver distinct trades']} duplicate keys in silver")
    if counts["rejects table rows"] != counts["bronze rejected rows"]:
        problems.append(f"rejects table has {counts['rejects table rows']} rows, bronze {counts['bronze rejected rows']}")
    if null_columns:
        problems.append(f"nulls in silver: {null_columns}")
    for row in gaps:
        problems.append(f"gap in match ids for run {row['engine_run_id']} {row['symbol']}: "
                        f"{row['n']} trades between {row['lo']} and {row['hi']}")

    lines.append(f"silver processed bronze up to snapshot {snapshot}")
    for name, value in counts.items():
        lines.append(f"  {name:<26} {value:>12,}")
    lines.append(f"  {'duplicates removed':<26} {counts['bronze valid rows'] - counts['bronze distinct trades']:>12,}")

    current = current_snapshot(spark, bronze)
    if current != snapshot:
        behind = read_bronze(spark, bronze, snapshot, current).count()
        lines.append(f"silver is behind bronze by {behind:,} rows (run make silver)")

    fresh = spark.sql(f"""
        SELECT percentile_approx((silver_time_ns - event_time_ns) / 1e9, array(0.5, 0.99)) AS p,
               max((silver_time_ns - event_time_ns) / 1e9) AS max_s
        FROM {t['trades']}
    """).first()
    if fresh["p"]:
        lines.append(f"freshness (engine event -> silver run start, s): p50={fresh['p'][0]:.0f} "
                     f"p99={fresh['p'][1]:.0f} max={fresh['max_s']:.0f}")

    files = spark.sql(f"SELECT count(*) AS n, sum(file_size_in_bytes) AS bytes FROM {t['trades']}.files").first()
    partitions = spark.sql(f"SELECT count(*) AS n FROM {t['trades']}.partitions").first()["n"]
    lines.append(f"silver table: {partitions} partitions (symbol x day), {files['n']} data files, "
                 f"{(files['bytes'] or 0) / 1e6:.1f} MB")
    lines.extend(f"PROBLEM: {p}" for p in problems)
    return not problems, lines


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--namespace", default="silver")
    args = parser.parse_args(argv)

    spark = spark_session("vortex-silver-check")
    spark.sparkContext.setLogLevel("ERROR")
    ok, lines = check(spark, args.namespace)
    print("\n".join(lines))
    print("RESULT " + ("OK" if ok else "FAIL"), flush=True)
    spark.stop()
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
