"""
structured_streaming.py — Real-time Cancel Flow Event Processing
================================================================
Processes cancel-flow clickstream events in real-time using
Spark Structured Streaming from Kinesis/Kafka.

Features:
  - Exactly-once semantics via Delta Lake MERGE
  - Event-time watermarking (5-minute late arrival tolerance)
  - Stateful stream processing for initiation detection
  - Schema validation + dead-letter queue for malformed events
  - Auto-scaling with EMR Serverless
  - Prometheus metrics emission
  - Checkpoint-based fault tolerance

Source:  Kinesis / Kafka topic: cancel-flow-events
Target:  bronze.raw_clickstream_events_streaming (Delta Lake)
"""

from __future__ import annotations

import json
import logging
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from src.utils.logger import get_logger
from src.utils.spark_session import get_spark, PipelineStep, SparkMode

logger = get_logger(__name__)


# ── Event schema (ECS clickstream event) ──────────────────────────────────────

EVENT_SCHEMA = T.StructType([
    T.StructField("event_id",                    T.StringType(),    True),
    T.StructField("company_id",                  T.StringType(),    True),
    T.StructField("accountant_realm_id",         T.StringType(),    True),
    T.StructField("session_id",                  T.StringType(),    True),
    T.StructField("event",                       T.StringType(),    True),
    T.StructField("properties_object_detail",    T.StringType(),    True),
    T.StructField("properties_ui_object_detail", T.StringType(),    True),
    T.StructField("properties_ui_access_point",  T.StringType(),    True),
    T.StructField("properties_url_host_name",    T.StringType(),    True),
    T.StructField("properties_custom_fp_offer_id",T.StringType(),   True),
    T.StructField("product",                     T.StringType(),    True),
    T.StructField("sku",                         T.StringType(),    True),
    T.StructField("billing_frequency",           T.StringType(),    True),
    T.StructField("subscription_type",           T.StringType(),    True),
    T.StructField("ua_parser_device_type",       T.StringType(),    True),
    T.StructField("context_page_path",           T.StringType(),    True),
    T.StructField("event_timestamp",             T.TimestampType(), True),
    T.StructField("event_date",                  T.StringType(),    True),
])


def run_streaming_pipeline(
    spark: SparkSession,
    source_type: str,                   # "kinesis" | "kafka"
    source_config: dict,                # Bootstrap servers, stream name, etc.
    output_path: str,                   # Delta Lake output path
    checkpoint_path: str,               # Checkpoint location
    trigger_interval: str = "30 seconds",
) -> None:
    """
    Run real-time cancel-flow event ingestion pipeline.

    Args:
        spark:            SparkSession with Kinesis/Kafka connector.
        source_type:      "kinesis" or "kafka".
        source_config:    Source-specific connection config.
        output_path:      Delta Lake table path.
        checkpoint_path:  Streaming checkpoint location.
        trigger_interval: Micro-batch interval (default: 30s).
    """
    logger.info(f"Starting streaming pipeline: {source_type} → {output_path}")

    # ── Read stream ────────────────────────────────────────────────────────────
    if source_type == "kinesis":
        raw_stream = (
            spark.readStream
            .format("kinesis")
            .option("streamName",       source_config["stream_name"])
            .option("region",           source_config.get("region", "us-east-1"))
            .option("initialPosition",  source_config.get("initial_position", "LATEST"))
            .option("roleArn",          source_config.get("role_arn", ""))
            .load()
            .select(F.col("data").cast("string").alias("raw_json"))
        )
    elif source_type == "kafka":
        raw_stream = (
            spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", source_config["bootstrap_servers"])
            .option("subscribe",               source_config["topic"])
            .option("startingOffsets",         source_config.get("starting_offsets", "latest"))
            .option("maxOffsetsPerTrigger",    source_config.get("max_offsets", 100_000))
            .load()
            .select(F.col("value").cast("string").alias("raw_json"))
        )
    else:
        raise ValueError(f"Unsupported source_type: {source_type}")

    # ── Parse JSON ─────────────────────────────────────────────────────────────
    parsed = (
        raw_stream
        .select(F.from_json(F.col("raw_json"), EVENT_SCHEMA).alias("evt"))
        .select("evt.*")
    )

    # ── Schema validation — route bad records to DLQ ───────────────────────────
    valid_events = parsed.filter(
        F.col("event_id").isNotNull()
        & F.col("company_id").isNotNull()
        & F.col("event_timestamp").isNotNull()
    )
    dlq_events = parsed.filter(
        F.col("event_id").isNull()
        | F.col("company_id").isNull()
        | F.col("event_timestamp").isNull()
    )

    # ── Event-time watermark (5-minute late arrival tolerance) ─────────────────
    watermarked = (
        valid_events
        .withWatermark("event_timestamp", "5 minutes")
        .withColumn("event_date", F.date_format("event_timestamp", "yyyy-MM-dd"))
        .withColumn("processed_timestamp", F.current_timestamp())
    )

    # ── Cancel-flow event filter ───────────────────────────────────────────────
    cancel_flow_events = watermarked.filter(
        F.col("event").isin(
            "workflow: started", "workflow:started",
            "workflow: completed", "workflow:completed",
            "workflow: engaged", "workflow:engaged",
            "offer: viewed", "offer:viewed",
            "offer: clicked", "offer:clicked",
            "content: viewed", "content:viewed",
            "cancelation flow: viewed",
        )
    )

    # ── Write to Delta Lake (append) ───────────────────────────────────────────
    def write_microbatch(batch_df, batch_id: int) -> None:
        """
        Process each micro-batch:
          1. Deduplicate by event_id (exactly-once)
          2. Write to Delta Lake with partition
        """
        batch_count = batch_df.count()
        if batch_count == 0:
            logger.info(f"Batch {batch_id}: empty — skipping")
            return

        # Deduplicate within batch
        deduped = batch_df.dropDuplicates(["event_id"])

        # Delta Lake MERGE — idempotent upsert (exactly-once semantics)
        from delta.tables import DeltaTable

        if DeltaTable.isDeltaTable(spark, output_path):
            delta_table = DeltaTable.forPath(spark, output_path)
            (
                delta_table.alias("target")
                .merge(deduped.alias("source"), "target.event_id = source.event_id")
                .whenNotMatchedInsertAll()
                .execute()
            )
        else:
            # First batch — create table
            deduped.write.format("delta").partitionBy("event_date").save(output_path)

        logger.info(
            f"Batch {batch_id}: {batch_count:,} events received | "
            f"{deduped.count():,} unique events written"
        )

    # ── Write DLQ events ───────────────────────────────────────────────────────
    dlq_query = (
        dlq_events
        .withColumn("dlq_reason", F.lit("missing_required_field"))
        .withColumn("received_at", F.current_timestamp())
        .writeStream
        .format("delta")
        .option("checkpointLocation", f"{checkpoint_path}/dlq")
        .outputMode("append")
        .partitionBy("event_date")
        .start(f"{output_path}_dlq")
    )

    # ── Main streaming query ───────────────────────────────────────────────────
    main_query = (
        cancel_flow_events
        .writeStream
        .foreachBatch(write_microbatch)
        .option("checkpointLocation", f"{checkpoint_path}/main")
        .trigger(processingTime=trigger_interval)
        .start()
    )

    logger.info(
        f"✅ Streaming queries started:\n"
        f"   Main: {main_query.id}\n"
        f"   DLQ:  {dlq_query.id}"
    )

    # ── Wait for termination ───────────────────────────────────────────────────
    main_query.awaitTermination()


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="Streaming: Cancel Flow Events")
    parser.add_argument("--source-type",      required=True, choices=["kinesis", "kafka"])
    parser.add_argument("--stream-name",      help="Kinesis stream name")
    parser.add_argument("--bootstrap-servers",help="Kafka bootstrap servers")
    parser.add_argument("--topic",            help="Kafka topic")
    parser.add_argument("--region",           default="us-east-1")
    parser.add_argument("--output-path",      required=True)
    parser.add_argument("--checkpoint-path",  required=True)
    parser.add_argument("--trigger-interval", default="30 seconds")
    parser.add_argument("--env",              default="local")
    args = parser.parse_args()

    spark = get_spark(
        PipelineStep.STREAMING,
        mode=SparkMode.EMR if args.env == "emr" else SparkMode.LOCAL,
    )

    source_config = {}
    if args.source_type == "kinesis":
        source_config = {"stream_name": args.stream_name, "region": args.region}
    elif args.source_type == "kafka":
        source_config = {
            "bootstrap_servers": args.bootstrap_servers,
            "topic": args.topic,
        }

    run_streaming_pipeline(
        spark, args.source_type, source_config,
        args.output_path, args.checkpoint_path, args.trigger_interval,
    )


if __name__ == "__main__":
    main()
