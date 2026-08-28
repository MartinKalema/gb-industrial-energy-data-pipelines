"""Run a bounded Structured Streaming append into an Iceberg table on R2."""

from __future__ import annotations

import os
import re

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from pyspark.sql.types import LongType, StringType, StructField, StructType


def required_environment(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"Required environment variable is missing: {name}")
    return value


catalog_uri = required_environment("R2_CATALOG_URI")
warehouse = required_environment("R2_CATALOG_WAREHOUSE")
token = required_environment("R2_CATALOG_TOKEN")
smoke_run_id = required_environment("SMOKE_RUN_ID")

if not re.fullmatch(r"[a-f0-9]{32}", smoke_run_id):
    raise RuntimeError("SMOKE_RUN_ID must be exactly 32 lowercase hexadecimal characters")

spark = (
    SparkSession.builder.appName("gb-industrial-energy-r2-streaming-smoke")
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    )
    .config("spark.sql.catalog.r2", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.r2.type", "rest")
    .config("spark.sql.catalog.r2.uri", catalog_uri)
    .config("spark.sql.catalog.r2.warehouse", warehouse)
    .config("spark.sql.catalog.r2.token", token)
    .config(
        "spark.sql.catalog.r2.header.X-Iceberg-Access-Delegation",
        "vended-credentials",
    )
    .config("spark.sql.catalog.r2.s3.remote-signing-enabled", "false")
    .config("spark.sql.defaultCatalog", "r2")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)

input_schema = StructType(
    [
        StructField("event_id", StringType(), nullable=False),
        StructField("written_by", StringType(), nullable=False),
        StructField("sequence_number", LongType(), nullable=False),
        StructField("note", StringType(), nullable=False),
    ]
)
table_name = "r2.industrial_energy_smoke.structured_stream_roundtrip"
checkpoint = f"/opt/industrial-energy/checkpoints/{smoke_run_id}"

try:
    events = (
        spark.readStream.schema(input_schema)
        .json("/opt/industrial-energy/smoke/input")
        .withColumn("smoke_run_id", lit(smoke_run_id))
        .select(
            "smoke_run_id",
            "event_id",
            "written_by",
            "sequence_number",
            "note",
        )
    )

    query = (
        events.writeStream.format("iceberg")
        .outputMode("append")
        .trigger(availableNow=True)
        .option("checkpointLocation", checkpoint)
        .toTable(table_name)
    )
    query.awaitTermination()

    visible_rows = (
        spark.table(table_name)
        .where(f"smoke_run_id = '{smoke_run_id}'")
        .count()
    )
    if visible_rows != 3:
        raise RuntimeError(
            f"Expected exactly three rows for {smoke_run_id}, found {visible_rows}"
        )

    print(
        "SPARK_STRUCTURED_STREAMING_OK "
        f"run_id={smoke_run_id} rows={visible_rows}"
    )
finally:
    spark.stop()
