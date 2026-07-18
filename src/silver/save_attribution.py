"""
save_attribution.py — Step 3: Silver Layer
===========================================
Classifies each cancel initiation into a save outcome.

Priority waterfall:
  1. CS Save     — CS agent contacted + cancel not confirmed
  2. Cancelled   — cancel_confirmed = 1, not saved by CS
  3. Upgrade Save — upgraded within 1 day, not cancelled or CS saved
  4. Downgrade Save — downgraded within 1 day
  5. Discount Save  — accepted discount offer within 1 day
  6. Abandoned   — none of the above (passive save)

Sources:
  - stg_cancel_initiations     (Step 1) — upgrade/downgrade already computed
  - rpt_ipd_detailed_engagement (Step 2) — discount IPD offer IDs
  - cs_reactive_saves_tb   — CS agent save data (7-day window)
  - company_offer_history        — discount purchase validation

Partition: (product, initiation_year, initiation_month)
Target:    silver.stg_save_attribution
"""

from __future__ import annotations

import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.utils.spark_session import PipelineStep, SparkMode, get_spark
from src.utils.delta_utils import DeltaTableWriter
from src.utils.logger import get_logger

logger = get_logger(__name__)


def build_save_attribution(
    spark: SparkSession,
    cancel_initiations_path: str,
    ipd_engagement_path: str,
    reactive_saves_path: str,
    offer_history_path: str,
    output_path: str,
) -> int:
    logger.info("Step 3 — Save Attribution (Silver)")

    # ── Load cancel initiations ────────────────────────────────────────────────
    ci = (
        spark.read.parquet(cancel_initiations_path)
        .select(
            "company_id", "initiation_rank", "initiation_timestamp",
            "initiation_date", "cancel_confirmed", "confirmation_timestamp",
            "upgraded", "upgrade_timestamp",
            "downgraded", "downgrade_timestamp",
            "accountant_id_starting_cancellation",
            "is_accountant_starting_cancellation",
            "product", "initiation_year", "initiation_month",
        )
        .cache()
    )

    # ── CS save CTE — 7-day reactive save window ───────────────────────────────
    reactive_saves = spark.read.parquet(reactive_saves_path)

    cs_save = (
        ci.alias("ci")
        .join(
            reactive_saves.alias("rsv"),
            (F.col("rsv.report_company_id") == F.col("ci.company_id"))
            & (F.to_date("rsv.report_dt").between(
                F.to_date(F.col("ci.initiation_timestamp")),
                F.date_add(F.to_date(F.col("ci.initiation_timestamp")), 7),
            ))
            & F.col("rsv.program").isin("SaaS Core", "SaaS Plus", "SaaS Advanced"),
            "left",
        )
        .groupBy("ci.company_id", "ci.initiation_rank", "ci.initiation_timestamp")
        .agg(
            F.max(F.when(F.col("rsv.cc_id").isNotNull(), F.lit(1)).otherwise(F.lit(0))).alias("contacted_by_cs"),
            F.max(F.when(F.coalesce(F.col("rsv.saved_cases"), F.lit(0)) == 1, F.lit(1)).otherwise(F.lit(0))).alias("saved_by_cs"),
        )
    )

    # ── Discount IPD shown CTE ────────────────────────────────────────────────
    ipd = spark.read.parquet(ipd_engagement_path)

    discount_ipd_shown = (
        ipd
        .filter(
            (F.col("ipd_type") == "Discount IPD")
            & F.col("obill_offer_id").isNotNull()
        )
        .select(
            "company_id", "initiation_rank", "initiation_timestamp",
            # Clean offer ID (remove quotes/apostrophes)
            F.regexp_replace(F.col("obill_offer_id"), "[\"']", "").alias("obill_offer_id"),
        )
        .distinct()
    )

    # ── Discount taken CTE — validate against purchase history ────────────────
    offer_history = spark.read.parquet(offer_history_path)

    discount_taken = (
        ci.alias("ci")
        .join(
            discount_ipd_shown.alias("dis"),
            (F.col("dis.company_id") == F.col("ci.company_id"))
            & (F.col("dis.initiation_rank") == F.col("ci.initiation_rank"))
            & (F.col("dis.initiation_timestamp") == F.col("ci.initiation_timestamp")),
        )
        .join(
            offer_history.alias("offr"),
            (F.col("offr.company_id") == F.col("ci.company_id"))
            & (F.col("offr.offer_id").cast("string") == F.trim(F.col("dis.obill_offer_id")))
            & (F.to_date("offr.purchase_datetime").between(
                F.to_date(F.col("ci.initiation_timestamp")),
                F.date_add(F.to_date(F.col("ci.initiation_timestamp")), 1),
            )),
        )
        .groupBy("ci.company_id", "ci.initiation_rank", "ci.initiation_timestamp")
        .agg(
            F.lit(1).alias("took_discount"),
            F.min(F.to_timestamp("offr.purchase_datetime")).alias("discount_take_timestamp"),
        )
    )

    # ── Final SELECT — save attribution priority waterfall ─────────────────────
    result = (
        ci.alias("ci")
        .join(
            cs_save.alias("cs"),
            (F.col("cs.company_id") == F.col("ci.company_id"))
            & (F.col("cs.initiation_rank") == F.col("ci.initiation_rank"))
            & (F.col("cs.initiation_timestamp") == F.col("ci.initiation_timestamp")),
            "left",
        )
        .join(
            discount_taken.alias("dt"),
            (F.col("dt.company_id") == F.col("ci.company_id"))
            & (F.col("dt.initiation_rank") == F.col("ci.initiation_rank"))
            & (F.col("dt.initiation_timestamp") == F.col("ci.initiation_timestamp")),
            "left",
        )
        .select(
            F.col("ci.company_id"),
            F.col("ci.initiation_rank"),
            F.col("ci.initiation_timestamp"),
            F.col("ci.initiation_date"),
            F.col("ci.cancel_confirmed"),
            F.col("ci.confirmation_timestamp"),
            # CS save (from reactive saves table)
            F.coalesce(F.col("cs.contacted_by_cs"), F.lit(0)).alias("contacted_by_cs"),
            F.coalesce(F.col("cs.saved_by_cs"),     F.lit(0)).alias("saved_by_cs"),
            # Upgrade / downgrade (from Step 1)
            F.coalesce(F.col("ci.upgraded"),         F.lit(0)).alias("upgraded"),
            F.col("ci.upgrade_timestamp"),
            F.coalesce(F.col("ci.downgraded"),       F.lit(0)).alias("downgraded"),
            F.col("ci.downgrade_timestamp"),
            # Discount (from offer history)
            F.coalesce(F.col("dt.took_discount"),    F.lit(0)).alias("took_discount"),
            F.col("dt.discount_take_timestamp"),
            # ── Save attribution priority waterfall ──────────────────────────
            F.when(F.coalesce(F.col("cs.saved_by_cs"), F.lit(0)) == 1,
                   F.lit("CS Save"))
            .when((F.col("ci.cancel_confirmed") == 1) & (F.coalesce(F.col("cs.saved_by_cs"), F.lit(0)) == 0),
                  F.lit("Cancelled"))
            .when((F.col("ci.cancel_confirmed") == 0) & (F.coalesce(F.col("cs.saved_by_cs"), F.lit(0)) == 0)
                  & (F.coalesce(F.col("ci.upgraded"), F.lit(0)) == 1),
                  F.lit("Upgrade Save"))
            .when((F.col("ci.cancel_confirmed") == 0) & (F.coalesce(F.col("cs.saved_by_cs"), F.lit(0)) == 0)
                  & (F.coalesce(F.col("ci.downgraded"), F.lit(0)) == 1),
                  F.lit("Downgrade Save"))
            .when((F.col("ci.cancel_confirmed") == 0) & (F.coalesce(F.col("cs.saved_by_cs"), F.lit(0)) == 0)
                  & (F.coalesce(F.col("dt.took_discount"), F.lit(0)) == 1),
                  F.lit("Discount Save"))
            .otherwise(F.lit("Abandoned"))
            .alias("save_attribution"),
            # saved_by_abandoning: Y when no active save mechanism and not cancelled
            F.when(
                (F.coalesce(F.col("cs.saved_by_cs"),   F.lit(0)) == 0)
                & (F.col("ci.cancel_confirmed") == 0)
                & (F.coalesce(F.col("ci.upgraded"),    F.lit(0)) == 0)
                & (F.coalesce(F.col("ci.downgraded"),  F.lit(0)) == 0)
                & (F.coalesce(F.col("dt.took_discount"),F.lit(0)) == 0),
                F.lit("Y"),
            ).otherwise(F.lit("N")).alias("saved_by_abandoning"),
            F.col("ci.accountant_id_starting_cancellation"),
            F.col("ci.is_accountant_starting_cancellation"),
            # Partition keys
            F.col("ci.product"),
            F.col("ci.initiation_year"),
            F.col("ci.initiation_month"),
        )
    )

    # ── Write Delta partitioned output ─────────────────────────────────────────
    writer = DeltaTableWriter(spark, output_path, "silver.stg_save_attribution")
    count = writer.write_partition_overwrite(result, ["product", "initiation_year", "initiation_month"])

    ci.unpersist()
    logger.info(f"✅ Step 3 complete — {count:,} rows")
    return count


def main() -> None:
    parser = argparse.ArgumentParser(description="Step 3: Save Attribution (Silver)")
    parser.add_argument("--cancel-initiations-path", required=True)
    parser.add_argument("--ipd-engagement-path",     required=True)
    parser.add_argument("--reactive-saves-path",     required=True)
    parser.add_argument("--offer-history-path",      required=True)
    parser.add_argument("--output-path",             required=True)
    parser.add_argument("--env", default="local", choices=["local", "emr"])
    args = parser.parse_args()

    spark = get_spark(
        PipelineStep.ATTRIBUTION,
        mode=SparkMode.EMR if args.env == "emr" else SparkMode.LOCAL,
    )
    build_save_attribution(
        spark, args.cancel_initiations_path, args.ipd_engagement_path,
        args.reactive_saves_path, args.offer_history_path, args.output_path,
    )
    spark.stop()


if __name__ == "__main__":
    main()
