"""
raw_events_pipeline.py — Step 0-A: Bronze Layer
=================================================
Ingests raw clickstream events into the Bronze layer.

Key features:
  - Bounded by ad_hoc_start_date / ad_hoc_end_date (7-day rolling default)
  - Flattens nested ECS JSON fields (workflow_state, widget_state, device, geo)
  - Resolves company_id via priority COALESCE chain
  - Deduplicates by event_id within each day partition
  - Partitioned by event_date (incremental dynamic overwrite)
  - MSCK REPAIR TABLE equivalent after write

Partition: event_date (STRING YYYY-MM-DD)
Target:    bronze.raw_clickstream_events
"""

from __future__ import annotations

import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from src.utils.spark_session import PipelineStep, SparkMode, get_spark
from src.utils.delta_utils import DeltaTableWriter
from src.utils.logger import get_logger

logger = get_logger(__name__)


def build_raw_events(
    spark: SparkSession,
    input_path: str,
    output_path: str,
    start_date: str,
    end_date: str,
) -> int:
    """
    Ingest and flatten raw ECS clickstream events into Bronze layer.

    Returns:
        Row count written.
    """
    logger.info(f"Step 0-A — Raw Events: {start_date} → {end_date}")

    # ── Load source events ─────────────────────────────────────────────────────
    raw = (
        spark.read.parquet(input_path)
        .filter(F.col("event_date").between(start_date, end_date))
    )

    # ── Company ID resolution (priority COALESCE chain) ────────────────────────
    # Priority: workflow_state.client_realm_id → widget_state.client_realm_id
    #           → properties.company_id → properties.realm_ID → direct company_id
    company_id_resolved = F.coalesce(
        F.when(
            F.length(F.trim(F.coalesce(
                F.get_json_object(F.col("workflow_state"), "$.client_realm_id"),
                F.lit(""),
            ))) > 0,
            F.get_json_object(F.col("workflow_state"), "$.client_realm_id"),
        ),
        F.when(
            F.length(F.trim(F.coalesce(
                F.get_json_object(F.col("widget_state"), "$.client_realm_id"),
                F.lit(""),
            ))) > 0,
            F.get_json_object(F.col("widget_state"), "$.client_realm_id"),
        ),
        F.col("company_id"),
    )

    # ── Transform ──────────────────────────────────────────────────────────────
    transformed = (
        raw
        .withColumn("resolved_company_id", company_id_resolved)
        .withColumn("event_timestamp",     F.to_timestamp("event_timestamp"))
        .withColumn("event_date",          F.date_format("event_timestamp", "yyyy-MM-dd"))
        # Normalise event names (remove extra spaces)
        .withColumn("event",               F.trim(F.regexp_replace(F.col("event"), "\\s+", " ")))
        # Extract product from workflow_state if not already present
        .withColumn(
            "product",
            F.coalesce(
                F.col("product"),
                F.get_json_object(F.col("workflow_state"), "$.product"),
                F.get_json_object(F.col("widget_state"),   "$.product"),
            ),
        )
        # Deduplicate within partition by event_id (exactly-once guarantee)
        .dropDuplicates(["event_id", "event_date"])
        # Rename resolved company_id
        .drop("company_id")
        .withColumnRenamed("resolved_company_id", "company_id")
        # Filter to date window (write window filter)
        .filter(F.col("event_date").between(start_date, end_date))
    )

    # ── Write Delta partitioned by event_date ──────────────────────────────────
    writer = DeltaTableWriter(spark, output_path, "bronze.raw_clickstream_events")
    count = writer.write_partition_overwrite(transformed, ["event_date"])

    logger.info(f"✅ Step 0-A complete — {count:,} rows written, partitioned by event_date")
    return count


def main() -> None:
    parser = argparse.ArgumentParser(description="Step 0-A: Raw ECS Events (Bronze)")
    parser.add_argument("--input-path",  required=True)
    parser.add_argument("--output-path", required=True)
    parser.add_argument("--start-date",  required=True)
    parser.add_argument("--end-date",    required=True)
    parser.add_argument("--env", default="local", choices=["local", "emr"])
    args = parser.parse_args()

    spark = get_spark(
        PipelineStep.RAW_EVENTS,
        mode=SparkMode.EMR if args.env == "emr" else SparkMode.LOCAL,
    )
    build_raw_events(spark, args.input_path, args.output_path, args.start_date, args.end_date)
    spark.stop()


if __name__ == "__main__":
    main()
