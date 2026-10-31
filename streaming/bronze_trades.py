"""Bronze ingestion: Kafka `trades` -> Iceberg `vortex.bronze.trades` (M6, ADR 0016).

Every Kafka message becomes one row, exactly once:
  - Spark's checkpoint records which Kafka offsets each micro-batch covered;
  - Iceberg's streaming sink records the batch id in its commit, so after a crash a batch that
    is replayed is recognised as already committed and not written twice.

A row keeps the message as received (raw_value, Kafka partition/offset/timestamp) plus every
trade.v1 field as a typed column. Integers stay integers (hard rule). A message that does not
parse is still stored, with parse_ok = false, so nothing is ever dropped.

Times: event_time_ns comes from the engine; ingest_time_ns is when this job's micro-batch
started (the hard rule keeps the two in separate columns); event_ts exists only to partition
the table by day.
"""

import os

from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.streaming import StreamingQueryListener

from vortex_streaming import CATALOG, spark_session

TABLE = f"{CATALOG}.bronze.trades"

# trade.v1 as Spark types: every number is a 64-bit integer. A float like 18990.0 does not
# parse into LongType and becomes null, so parse_ok catches it (strict integers, as in M1).
TRADE_V1 = T.StructType([
    T.StructField("schema_version", T.IntegerType()),
    T.StructField("engine_run_id", T.StringType()),
    T.StructField("match_id", T.LongType()),
    T.StructField("symbol", T.StringType()),
    T.StructField("price_ticks", T.LongType()),
    T.StructField("qty_lots", T.LongType()),
    T.StructField("buy_participant_id", T.LongType()),
    T.StructField("buy_order_id", T.LongType()),
    T.StructField("sell_participant_id", T.LongType()),
    T.StructField("sell_order_id", T.LongType()),
    T.StructField("aggressor_side", T.StringType()),
    T.StructField("sequence_number", T.LongType()),
    T.StructField("event_time_ns", T.LongType()),
])

CREATE_TABLE = f"""
CREATE TABLE IF NOT EXISTS {TABLE} (
    engine_run_id        STRING  COMMENT 'trade.v1; unique key with symbol and match_id',
    symbol               STRING,
    match_id             BIGINT,
    price_ticks          BIGINT  COMMENT 'integer ticks, never a float',
    qty_lots             BIGINT,
    buy_participant_id   BIGINT,
    buy_order_id         BIGINT,
    sell_participant_id  BIGINT,
    sell_order_id        BIGINT,
    aggressor_side       STRING,
    sequence_number      BIGINT,
    event_time_ns        BIGINT  COMMENT 'set by the engine, UTC ns since epoch',
    schema_version       INT,
    event_ts             TIMESTAMP COMMENT 'event_time_ns at microsecond precision, for partitioning',
    parse_ok             BOOLEAN COMMENT 'false: raw_value is kept, typed columns may be null',
    raw_value            STRING  COMMENT 'the Kafka message exactly as received',
    kafka_partition      INT,
    kafka_offset         BIGINT  COMMENT '(kafka_partition, kafka_offset) is unique per row',
    kafka_timestamp      TIMESTAMP COMMENT 'producer CreateTime',
    ingest_time_ns       BIGINT  COMMENT 'start of the micro-batch that wrote the row, UTC ns'
)
USING iceberg
PARTITIONED BY (days(event_ts))
TBLPROPERTIES ('format-version' = '2', 'write.parquet.compression-codec' = 'zstd')
"""


def bronze_rows(kafka_stream):
    parsed = kafka_stream.select(
        F.col("value").cast("string").alias("raw_value"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
        F.from_json(F.col("value").cast("string"), TRADE_V1).alias("t"),
    )
    required = [F.col(f"t.{field.name}").isNotNull() for field in TRADE_V1.fields]
    parse_ok = required[0]
    for condition in required[1:]:
        parse_ok = parse_ok & condition
    parse_ok = parse_ok & (F.col("t.schema_version") == 1)

    return parsed.select(
        "t.engine_run_id", "t.symbol", "t.match_id", "t.price_ticks", "t.qty_lots",
        "t.buy_participant_id", "t.buy_order_id", "t.sell_participant_id", "t.sell_order_id",
        "t.aggressor_side", "t.sequence_number", "t.event_time_ns", "t.schema_version",
        F.timestamp_micros(F.expr("t.event_time_ns div 1000")).alias("event_ts"),
        F.coalesce(parse_ok, F.lit(False)).alias("parse_ok"),
        "raw_value", "kafka_partition", "kafka_offset", "kafka_timestamp",
        # current_timestamp() is fixed for the whole micro-batch: the batch start, in microseconds
        (F.unix_micros(F.current_timestamp()) * 1000).alias("ingest_time_ns"),
    )


def main():
    spark = spark_session("vortex-bronze-trades")
    spark.sparkContext.setLogLevel("WARN")
    spark.streams.addListener(ProgressLog())
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.bronze")
    spark.sql(CREATE_TABLE)

    trigger_seconds = int(os.environ.get("BRONZE_TRIGGER_SECONDS", "10"))
    kafka = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", os.environ["KAFKA_BOOTSTRAP"])
        .option("subscribe", "trades")
        # first start: everything still in the topic; later starts: from the checkpoint
        .option("startingOffsets", "earliest")
        # bounded batches keep memory flat when catching up on a backlog
        .option("maxOffsetsPerTrigger", os.environ.get("BRONZE_MAX_OFFSETS", "200000"))
        # if retention deleted offsets we never read, stop instead of silently skipping them
        .option("failOnDataLoss", "true")
        .load()
    )

    query = (
        bronze_rows(kafka).writeStream.format("iceberg")
        .outputMode("append")
        .trigger(processingTime=f"{trigger_seconds} seconds")
        .option("checkpointLocation", os.environ["CHECKPOINT_DIR"])
        .option("fanout-enabled", "true")  # rows arrive unsorted by partition; no shuffle needed
        .toTable(TABLE)
    )
    print(f"bronze: streaming trades -> {TABLE} every {trigger_seconds}s, "
          f"checkpoint {os.environ['CHECKPOINT_DIR']}", flush=True)
    query.awaitTermination()


class ProgressLog(StreamingQueryListener):
    """One line per micro-batch. Polling lastProgress can skip batches; a listener cannot."""

    def onQueryStarted(self, event):
        pass

    def onQueryProgress(self, event):
        progress = event.progress
        duration = progress.durationMs.get("triggerExecution", 0)
        print(f"bronze: batch {progress.batchId} rows {progress.numInputRows} in {duration} ms",
              flush=True)

    def onQueryIdle(self, event):
        pass

    def onQueryTerminated(self, event):
        print(f"bronze: stopped{' with error: ' + event.exception if event.exception else ''}", flush=True)


if __name__ == "__main__":
    main()
