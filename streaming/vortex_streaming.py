"""Shared Spark setup for the streaming jobs: one Iceberg catalog called `vortex` (ADR 0016).

    vortex  ->  Iceberg REST catalog (table name -> current metadata file)
            ->  S3FileIO on SeaweedFS (data and metadata files in s3://warehouse/)

Connection details come from the environment (docker-compose passes them from .env).
"""

import os

from pyspark.sql import SparkSession

CATALOG = "vortex"


def spark_session(app_name):
    s3_endpoint = os.environ["S3_ENDPOINT"]
    catalog = f"spark.sql.catalog.{CATALOG}"
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(catalog, "org.apache.iceberg.spark.SparkCatalog")
        .config(f"{catalog}.type", "rest")
        .config(f"{catalog}.uri", os.environ["ICEBERG_REST_URI"])
        .config(f"{catalog}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config(f"{catalog}.s3.endpoint", s3_endpoint)
        .config(f"{catalog}.s3.path-style-access", "true")
        .config(f"{catalog}.s3.access-key-id", os.environ["S3_ACCESS_KEY"])
        .config(f"{catalog}.s3.secret-access-key", os.environ["S3_SECRET_KEY"])
        .config(f"{catalog}.client.region", os.environ.get("AWS_REGION", "us-east-1"))
        # all timestamps in UTC, like event_time_ns
        .config("spark.sql.session.timeZone", "UTC")
        # small local job: few shuffle partitions instead of the default 200
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
