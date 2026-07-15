"""
spark_session.py — Production Spark Session Factory
=====================================================
Creates optimised SparkSession instances for different pipeline steps.
Includes Delta Lake, AQE, dynamic partition pruning, and EMR-specific configs.
"""

from __future__ import annotations

import os
from enum import Enum
from typing import Optional

from pyspark.sql import SparkSession


class SparkMode(str, Enum):
    LOCAL      = "local"
    EMR        = "emr"
    DATABRICKS = "databricks"


class PipelineStep(str, Enum):
    RAW_EVENTS   = "step0_raw_events"
    IXP          = "step0_ixp"
    INITIATIONS  = "step1_cancel_initiations"
    IPD          = "step2_ipd_engagement"
    ATTRIBUTION  = "step3_save_attribution"
    FINAL        = "step4_final_metrics"
    STREAMING    = "streaming"
    ML           = "ml_features"


# ── Per-step shuffle partition tuning ─────────────────────────────────────────
SHUFFLE_PARTITIONS: dict[PipelineStep, int] = {
    PipelineStep.RAW_EVENTS:  400,
    PipelineStep.IXP:         200,
    PipelineStep.INITIATIONS: 2000,   # Large: window functions + multiple joins
    PipelineStep.IPD:         800,
    PipelineStep.ATTRIBUTION: 800,
    PipelineStep.FINAL:       800,
    PipelineStep.STREAMING:   100,
    PipelineStep.ML:          400,
}


def get_spark(
    step: PipelineStep,
    app_name: Optional[str] = None,
    mode: SparkMode = SparkMode.LOCAL,
    enable_delta: bool = True,
    enable_hive: bool = True,
) -> SparkSession:
    """
    Create a production-optimised SparkSession.

    Args:
        step:         Pipeline step — determines shuffle partition count and tuning.
        app_name:     Application name (defaults to step name).
        mode:         Execution mode — local | emr | databricks.
        enable_delta: Enable Delta Lake support.
        enable_hive:  Enable Hive metastore support.

    Returns:
        Configured SparkSession.

    Example:
        >>> spark = get_spark(PipelineStep.INITIATIONS, mode=SparkMode.EMR)
    """
    name = app_name or f"streamflow_analytics_{step.value}"
    shuffle_parts = SHUFFLE_PARTITIONS.get(step, 800)

    builder = SparkSession.builder.appName(name)

    # ── Delta Lake packages ────────────────────────────────────────────────────
    if enable_delta and mode != SparkMode.DATABRICKS:
        builder = builder.config(
            "spark.jars.packages",
            "io.delta:delta-core_2.12:2.4.0,"
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0",
        )
        builder = builder.config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        builder = builder.config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )

    # ── Core SQL settings ──────────────────────────────────────────────────────
    builder = builder.config("spark.sql.session.timeZone", "US/Pacific")
    builder = builder.config("spark.sql.shuffle.partitions", str(shuffle_parts))
    builder = builder.config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    builder = builder.config("spark.hadoop.hive.exec.dynamic.partition", "true")
    builder = builder.config("spark.hadoop.hive.exec.dynamic.partition.mode", "nonstrict")

    # ── Adaptive Query Execution (AQE) ─────────────────────────────────────────
    builder = builder.config("spark.sql.adaptive.enabled", "true")
    builder = builder.config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    builder = builder.config("spark.sql.adaptive.coalescePartitions.minPartitionSize", "64m")
    builder = builder.config("spark.sql.adaptive.skewJoin.enabled", "true")
    builder = builder.config("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
    builder = builder.config("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256m")
    builder = builder.config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128m")
    builder = builder.config("spark.sql.adaptive.localShuffleReader.enabled", "true")

    # ── Dynamic partition pruning ──────────────────────────────────────────────
    builder = builder.config("spark.sql.optimizer.dynamicPartitionPruning.enabled", "true")

    # ── I/O & network ─────────────────────────────────────────────────────────
    builder = builder.config("spark.sql.files.maxPartitionBytes", "128m")
    builder = builder.config("spark.sql.files.openCostInBytes", "16m")
    builder = builder.config("spark.sql.parquet.compression.codec", "snappy")
    builder = builder.config("spark.sql.parquet.rowGroupSizeBytes", "134217728")
    builder = builder.config("spark.sql.parquet.asyncFileDownload.enabled", "false")
    builder = builder.config("spark.network.timeout", "600s")
    builder = builder.config("spark.executor.heartbeatInterval", "60s")

    # ── Dynamic allocation ─────────────────────────────────────────────────────
    builder = builder.config("spark.dynamicAllocation.shuffleTracking.enabled", "true")
    builder = builder.config("spark.dynamicAllocation.shuffleTracking.timeout", "1800s")
    builder = builder.config("spark.dynamicAllocation.executorIdleTimeout", "600s")
    builder = builder.config("spark.dynamicAllocation.cachedExecutorIdleTimeout", "1800s")

    # ── Step-specific overrides ────────────────────────────────────────────────
    if step == PipelineStep.INITIATIONS:
        builder = builder.config("spark.driver.memory", "32g")
        builder = builder.config("spark.driver.maxResultSize", "8g")
        builder = builder.config("spark.sql.hive.convertMetastoreParquet", "false")

    # ── Hive metastore ─────────────────────────────────────────────────────────
    if enable_hive:
        builder = builder.config("spark.hadoop.hive.metastore.client.socket.timeout", "1800000")
        builder = builder.enableHiveSupport()

    # ── Local mode config ──────────────────────────────────────────────────────
    if mode == SparkMode.LOCAL:
        builder = builder.master("local[*]")
        builder = builder.config("spark.sql.shuffle.partitions", "8")  # Override for local

    return builder.getOrCreate()


def stop_spark(spark: SparkSession) -> None:
    """Gracefully stop a SparkSession and log the app ID."""
    app_id = spark.sparkContext.applicationId
    spark.stop()
    print(f"✅ SparkSession {app_id} stopped.")
