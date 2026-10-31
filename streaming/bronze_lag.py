"""Ingest lag and file count for one engine run in bronze (used by streaming/bench.sh).

    python bronze_lag.py <engine_run_id>

Lag per row = ingest_time_ns (start of the micro-batch that wrote it) - event_time_ns (set by
the engine). A row becomes readable when that batch commits, so the batch duration (logged by
the stream) comes on top.
"""

import sys

from vortex_streaming import CATALOG, spark_session

TABLE = f"{CATALOG}.bronze.trades"


def main():
    run_id = sys.argv[1]
    spark = spark_session("vortex-bronze-lag")
    spark.sparkContext.setLogLevel("ERROR")
    row = spark.sql(f"""
        SELECT count(*) AS n,
               count(DISTINCT ingest_time_ns) AS batches,
               percentile_approx((ingest_time_ns - event_time_ns) / 1e6, array(0.5, 0.9, 0.99)) AS p,
               max((ingest_time_ns - event_time_ns) / 1e6) AS max_ms
        FROM {TABLE} WHERE engine_run_id = '{run_id}'
    """).first()
    files = spark.sql(f"""
        SELECT count(*) AS n FROM {TABLE}.files
        WHERE readable_metrics.engine_run_id.lower_bound <= '{run_id}'
          AND readable_metrics.engine_run_id.upper_bound >= '{run_id}'
    """).first()["n"]
    p50, p90, p99 = row["p"] if row["n"] else (0, 0, 0)
    # one line, parsed by bench.sh
    print(f"LAG rows={row['n']} batches={row['batches']} files={files} "
          f"p50={p50:.0f} p90={p90:.0f} p99={p99:.0f} max={row['max_ms'] or 0:.0f}")
    spark.stop()


if __name__ == "__main__":
    main()
