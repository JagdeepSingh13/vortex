"""Reconcile Kafka `trades` with Iceberg `vortex.bronze.trades` (make bronze-check).

For every Kafka partition, all offsets from the earliest still in the topic to the latest must
be in bronze exactly once:

    rows == distinct offsets          no duplicates
    min offset == Kafka earliest      nothing missing at the start
    max offset == Kafka latest - 1    caught up, nothing missing at the end
    rows == max - min + 1             no gaps in between

Also reports rows that failed to parse and the ingest lag (engine event time -> start of the
micro-batch that wrote the row). Exit code 1 if anything does not reconcile.
"""

import os
import sys

from pyspark.sql import functions as F

from vortex_streaming import CATALOG, spark_session

TABLE = f"{CATALOG}.bronze.trades"


def main():
    spark = spark_session("vortex-bronze-check")
    spark.sparkContext.setLogLevel("ERROR")

    kafka = (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", os.environ["KAFKA_BOOTSTRAP"])
        .option("subscribe", "trades")
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .load()
        .groupBy("partition")
        .agg(F.min("offset").alias("k_min"), F.max("offset").alias("k_max"), F.count("*").alias("k_rows"))
    )
    bronze = (
        spark.table(TABLE)
        .groupBy("kafka_partition")
        .agg(
            F.count("*").alias("rows"),
            F.countDistinct("kafka_offset").alias("distinct_offsets"),
            F.min("kafka_offset").alias("b_min"),
            F.max("kafka_offset").alias("b_max"),
            F.sum(F.when(F.col("parse_ok"), 0).otherwise(1)).alias("unparsed"),
        )
    )
    joined = kafka.join(bronze, kafka.partition == bronze.kafka_partition, "full_outer").orderBy("partition")

    ok = True
    print(f"{'partition':>9} {'kafka':>9} {'bronze':>9} {'distinct':>9} {'unparsed':>9}  result")
    for row in joined.collect():
        partition = row["partition"] if row["partition"] is not None else row["kafka_partition"]
        problems = []
        if row["rows"] is None:
            problems.append("partition missing from bronze")
        elif row["k_rows"] is None:
            problems.append("partition not in Kafka")
        else:
            if row["rows"] != row["distinct_offsets"]:
                problems.append(f"{row['rows'] - row['distinct_offsets']} duplicate offsets")
            if row["b_min"] != row["k_min"]:
                problems.append(f"starts at {row['b_min']}, Kafka at {row['k_min']}")
            if row["b_max"] != row["k_max"]:
                problems.append(f"ends at {row['b_max']}, Kafka at {row['k_max']} (behind or missing)")
            if row["distinct_offsets"] != row["b_max"] - row["b_min"] + 1:
                problems.append("gaps between offsets")
            if row["unparsed"]:
                problems.append(f"{row['unparsed']} rows did not parse")
        ok = ok and not problems
        print(f"{partition:>9} {row['k_rows'] or 0:>9} {row['rows'] or 0:>9} {row['distinct_offsets'] or 0:>9} "
              f"{row['unparsed'] or 0:>9}  {'OK' if not problems else '; '.join(problems)}")

    lag = spark.sql(f"""
        SELECT count(*) AS n,
               percentile_approx((ingest_time_ns - event_time_ns) / 1e6, array(0.5, 0.9, 0.99)) AS p,
               max((ingest_time_ns - event_time_ns) / 1e6) AS max_ms
        FROM {TABLE} WHERE parse_ok
    """).first()
    if lag["n"]:
        p50, p90, p99 = lag["p"]
        print(f"ingest lag (engine event -> micro-batch start, ms): n={lag['n']} "
              f"p50={p50:.0f} p90={p90:.0f} p99={p99:.0f} max={lag['max_ms']:.0f}")

    snapshots = spark.sql(f"SELECT count(*) AS n FROM {TABLE}.snapshots").first()["n"]
    files = spark.sql(f"SELECT count(*) AS n, sum(file_size_in_bytes) AS bytes FROM {TABLE}.files").first()
    print(f"table: {snapshots} snapshots, {files['n']} data files, {(files['bytes'] or 0) / 1e6:.1f} MB")
    print("RESULT " + ("OK" if ok else "FAIL"))
    spark.stop()
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
