"""
ixp_assignments.py — Step 0-B: Bronze Layer
============================================
Ingests IXP experiment assignments into the Bronze layer.

Key features:
  - Reads all experiments (no hard-coded experiment_id filter — Phase 2 change)
  - Deduplicates to single-treatment companies (HAVING COUNT(DISTINCT treatment) = 1)
  - Partitioned by first_assignment_date (incremental dynamic overwrite)
  - Used by Step 4 for experiment-level analysis (no longer filters Step 4 grain)

Partition: first_assignment_date (DATE)
Target:    bronze.stg_ixp_assignments
"""

from __future__ import annotations

import argparse

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

from src.utils.spark_session import PipelineStep, SparkMode, get_spark
from src.utils.delta_utils import DeltaTableWriter
from src.utils.logger import get_logger

logger = get_logger(__name__)


def build_ixp_assignments(
    spark: SparkSession,
    output_path: str,
    start_date: str,
    end_date: str,
    ixp_source_path: str = None,
) -> int:
    """
    Build IXP experiment assignment table.

    Phase-2 change: experiment_id filter REMOVED — reads all experiments.
    Still deduplicates to single-treatment companies for clean cohort analysis.

    Returns:
        Row count written.
    """
    logger.info(f"Step 0-B — IXP Assignments: {start_date} → {end_date}")

    # ── Load IXP source ────────────────────────────────────────────────────────
    if ixp_source_path:
        ixp_raw = spark.read.parquet(ixp_source_path)
    else:
        # In production: read from Hive/Glue table
        ixp_raw = spark.table("ixp_dwh.ixp_first_assignment")

    # ── Filter by date window ──────────────────────────────────────────────────
    ixp_filtered = (
        ixp_raw
        .filter(F.col("version") >= 1)
        .filter(
            F.to_date(F.col("first_timestamp")).between(start_date, end_date)
        )
    )

    # ── Single-treatment dedup ─────────────────────────────────────────────────
    # Companies assigned to exactly one treatment_name (clean cohort)
    treatment_counts = (
        ixp_filtered
        .groupBy("company_id", "experiment_id")
        .agg(F.countDistinct("treatment_name").alias("n_treatments"))
    )

    single_treatment_companies = (
        treatment_counts
        .filter(F.col("n_treatments") == 1)
        .select("company_id", "experiment_id")
    )

    # ── Join back to get treatment_name and first_assignment_date ──────────────
    result = (
        ixp_filtered.alias("ixp")
        .join(
            single_treatment_companies.alias("st"),
            (F.col("ixp.company_id") == F.col("st.company_id"))
            & (F.col("ixp.experiment_id") == F.col("st.experiment_id")),
        )
        .select(
            F.col("ixp.company_id").cast("long").alias("company_id"),
            F.col("ixp.experiment_id"),
            F.col("ixp.treatment_name"),
            F.to_date(F.col("ixp.first_timestamp")).alias("first_assignment_date"),
        )
        .dropDuplicates(["company_id", "experiment_id", "first_assignment_date"])
    )

    # ── Write Delta partitioned by first_assignment_date ──────────────────────
    writer = DeltaTableWriter(spark, output_path, "bronze.stg_ixp_assignments")
    count = writer.write_partition_overwrite(result, ["first_assignment_date"])

    logger.info(f"✅ Step 0-B complete — {count:,} rows | single-treatment companies only")
    return count


def main() -> None:
    parser = argparse.ArgumentParser(description="Step 0-B: IXP Assignments (Bronze)")
    parser.add_argument("--output-path",      required=True)
    parser.add_argument("--start-date",       required=True)
    parser.add_argument("--end-date",         required=True)
    parser.add_argument("--ixp-source-path",  default=None)
    parser.add_argument("--env", default="local", choices=["local", "emr"])
    args = parser.parse_args()

    spark = get_spark(
        PipelineStep.IXP,
        mode=SparkMode.EMR if args.env == "emr" else SparkMode.LOCAL,
    )
    build_ixp_assignments(
        spark, args.output_path, args.start_date,
        args.end_date, args.ixp_source_path,
    )
    spark.stop()


if __name__ == "__main__":
    main()
