"""
ipd_engagement.py — Step 2: Gold Layer (3★ Consumable)
=======================================================
Builds offer-grain IPD and DIC engagement per cancel initiation.

Key features:
  - INNER JOIN on access_point + offer_id → non-nullable composite key
  - 6 IPD type classifications from offer copy_data JSON
  - DIC (usage-highlights-widget) LEFT JOIN with COALESCE(0) guarantee
  - SELECT DISTINCT → uniqueness = 100% (DQ guarantee)
  - Partitioned by (product, initiation_year, initiation_month)
  - Delta Lake OPTIMIZE + ZORDER after write

Unique key: (company_id, initiation_rank, initiation_timestamp, access_point, offer_id)
Target:     gold.rpt_ipd_detailed_engagement
"""

from __future__ import annotations

import argparse
import logging

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.utils.spark_session import PipelineStep, SparkMode, get_spark
from src.utils.delta_utils import DeltaTableWriter
from src.utils.logger import get_logger

logger = get_logger(__name__)

# ── Cancel-flow access points ──────────────────────────────────────────────────
CANCEL_FLOW_ACCESS_POINTS = [
    "CancelFlowBillingCancel",
    "CancelFlowTalkToExpert",
    "AccountSettingsCancel",
    "MobileAppBillingCancel",
    # Legacy access points
    "CancelFlowBillingPage",
    "CancelFlowTalkToExpert",
    "AccountSettingsCancel",
    "MobileAppCancelSupport",
]


def classify_ipd_type() -> F.Column:
    """
    Derive IPD type from offer copy_data JSON fields.
    Priority order: CS > Discount > Upgrade > Downgrade > Keep my Plan > Unknown
    """
    return (
        F.when(
            F.coalesce(
                F.get_json_object(F.col("offr.copy_data"), "$.ctaAction"),
                F.get_json_object(F.col("offr.copy_data"), "$.primaryCtaAction"),
            ) == "contact-us-widget",
            F.lit("CS IPD"),
        )
        .when(
            F.get_json_object(F.col("offr.copy_data"), "$.obillOfferId").isNotNull(),
            F.lit("Discount IPD"),
        )
        .when(
            (F.get_json_object(F.col("offr.copy_data"), "$.primaryCtaAction") == "external")
            & F.get_json_object(F.col("offr.copy_data"), "$.primaryCtaUrl").contains("/obillupgrade"),
            F.lit("Upgrade IPD"),
        )
        .when(
            (F.get_json_object(F.col("offr.copy_data"), "$.primaryCtaAction") == "external")
            & F.get_json_object(F.col("offr.copy_data"), "$.primaryCtaUrl").contains("/changeplan"),
            F.lit("Downgrade IPD"),
        )
        .when(
            (F.get_json_object(F.col("offr.copy_data"), "$.primaryCtaAction") == "callbackOnly")
            & F.get_json_object(F.col("offr.copy_data"), "$.obillOfferId").isNull(),
            F.lit("Keep my Plan IPD"),
        )
        .otherwise(F.lit("Unknown"))
    )


def build_ipd_engagement(
    spark: SparkSession,
    cancel_initiations_path: str,
    raw_events_path: str,
    offer_catalog_path: str,
    output_path: str,
) -> int:
    logger.info("Step 2 — IPD Detailed Engagement (Gold)")

    # ── Load inputs ────────────────────────────────────────────────────────────
    ci = (
        spark.read.parquet(cancel_initiations_path)
        .select(
            "company_id", "product", "initiation_rank",
            "initiation_timestamp", "initiation_year", "initiation_month",
            F.coalesce(
                F.col("confirmation_timestamp"),
                F.col("window_end_timestamp"),
            ).alias("window_end_timestamp"),
        )
        .cache()
    )

    raw = spark.read.parquet(raw_events_path).cache()
    offer_catalog = spark.read.parquet(offer_catalog_path)

    # ── IPD engagement CTE ─────────────────────────────────────────────────────
    ipd_engagement = (
        ci.alias("ci")
        .join(
            raw.alias("ecs"),
            (F.col("ecs.company_id").cast("long") == F.col("ci.company_id"))
            & F.col("ecs.event").isin("offer: viewed", "offer:viewed", "offer: clicked", "offer:clicked")
            & F.col("ecs.properties_ui_access_point").isin(CANCEL_FLOW_ACCESS_POINTS)
            & (F.to_timestamp("ecs.event_timestamp") >= F.col("ci.initiation_timestamp"))
            & (F.to_timestamp("ecs.event_timestamp") < F.col("ci.window_end_timestamp")),
        )
        .join(
            offer_catalog.alias("offr"),
            F.col("offr.offer_id") == F.col("ecs.properties_custom_fp_offer_id"),
        )
        .groupBy(
            "ci.company_id", "ci.product", "ci.initiation_year", "ci.initiation_month",
            "ci.initiation_rank", "ci.initiation_timestamp", "ci.window_end_timestamp",
            "ecs.properties_ui_access_point", "ecs.properties_custom_fp_offer_id",
            "offr.offer_name",
            F.coalesce(
                F.get_json_object(F.col("offr.copy_data"), "$.ctaText"),
                F.get_json_object(F.col("offr.copy_data"), "$.primaryCtaText"),
            ).alias("primaryCtaText"),
            F.get_json_object(F.col("offr.copy_data"), "$.obillOfferId").alias("obill_offer_id"),
            classify_ipd_type().alias("ipd_type"),
        )
        .agg(
            F.min(
                F.when(
                    F.col("ecs.event").isin("offer: viewed", "offer:viewed"),
                    F.to_timestamp("ecs.event_timestamp"),
                )
            ).alias("ipd_view_timestamp"),
            F.max(
                F.when(F.col("ecs.event").isin("offer: viewed", "offer:viewed"), F.lit(1)).otherwise(F.lit(0))
            ).alias("viewed_ipd"),
            F.max(
                F.when(F.col("ecs.event").isin("offer: clicked", "offer:clicked"), F.lit(1)).otherwise(F.lit(0))
            ).alias("clicked_ipd"),
        )
    )

    # ── DIC (Data-Informed Cancellation) CTE ───────────────────────────────────
    data_in_context = (
        ci.alias("ci")
        .join(
            raw.alias("ecs"),
            (F.col("ecs.company_id").cast("long") == F.col("ci.company_id"))
            & F.col("ecs.event").isin("content: viewed", "content:viewed")
            & (F.col("ecs.properties_object_detail") == "usage-highlights-widget")
            & (F.to_timestamp("ecs.event_timestamp") >= F.col("ci.initiation_timestamp"))
            & (F.to_timestamp("ecs.event_timestamp") < F.col("ci.window_end_timestamp")),
        )
        .groupBy(
            "ci.company_id", "ci.product", "ci.initiation_year", "ci.initiation_month",
            "ci.initiation_rank", "ci.initiation_timestamp",
        )
        .agg(
            F.lit(1).alias("viewed_dic_component"),
            # MAX data points shown across all DIC impressions in this window
            F.max(
                F.cast(
                    F.get_json_object(F.col("ecs.properties_ui_object_detail"), "$.data_object_display_count"),
                    "int",
                )
            ).alias("number_of_data_points"),
            # Most recent DIC payload for debugging
            F.last("ecs.properties_ui_object_detail", ignorenulls=True).alias("dic_component_detail"),
            F.min(F.to_timestamp("ecs.event_timestamp")).alias("dic_impression_timestamp"),
        )
    )

    # ── Final SELECT — SELECT DISTINCT guarantees DQ uniqueness ──────────────
    result = (
        ci.alias("ci")
        .join(
            ipd_engagement.alias("ipd"),
            (F.col("ipd.company_id") == F.col("ci.company_id"))
            & (F.col("ipd.initiation_rank") == F.col("ci.initiation_rank"))
            & (F.col("ipd.initiation_timestamp") == F.col("ci.initiation_timestamp")),
        )
        .join(
            data_in_context.alias("dic"),
            (F.col("dic.company_id") == F.col("ci.company_id"))
            & (F.col("dic.initiation_rank") == F.col("ci.initiation_rank"))
            & (F.col("dic.initiation_timestamp") == F.col("ci.initiation_timestamp")),
            "left",
        )
        .select(
            F.col("ci.company_id"),
            F.col("ci.initiation_rank"),
            F.col("ci.initiation_timestamp"),
            F.col("ci.window_end_timestamp"),
            F.col("ipd.properties_ui_access_point"),
            F.col("ipd.properties_custom_fp_offer_id").alias("offer_id"),
            F.col("ipd.offer_name"),
            F.col("ipd.primaryCtaText"),
            F.col("ipd.obill_offer_id"),
            F.col("ipd.ipd_type"),
            F.col("ipd.ipd_view_timestamp"),
            F.col("ipd.viewed_ipd"),
            F.col("ipd.clicked_ipd"),
            # COALESCE guarantees 0 not NULL (DQ requirement)
            F.coalesce(F.col("dic.viewed_dic_component"),   F.lit(0)).alias("viewed_dic_component"),
            F.coalesce(F.col("dic.number_of_data_points"),  F.lit(0)).alias("number_of_data_points_shown"),
            F.col("dic.dic_component_detail"),
            F.col("dic.dic_impression_timestamp"),
            # Partition keys
            F.col("ci.product"),
            F.col("ci.initiation_year"),
            F.col("ci.initiation_month"),
        )
        .distinct()   # ← DQ uniqueness guarantee — prevents multi-DIC row multiplication
    )

    # ── Write Delta partitioned output ─────────────────────────────────────────
    writer = DeltaTableWriter(spark, output_path, "gold.rpt_ipd_detailed_engagement")
    count = writer.write_partition_overwrite(result, ["product", "initiation_year", "initiation_month"])

    # OPTIMIZE + ZORDER for Step 4 join performance
    writer.optimize(zorder_cols=["company_id", "initiation_timestamp", "ipd_type"])

    ci.unpersist()
    raw.unpersist()

    logger.info(f"✅ Step 2 complete — {count:,} rows")
    return count


def main() -> None:
    parser = argparse.ArgumentParser(description="Step 2: IPD Detailed Engagement (Gold)")
    parser.add_argument("--cancel-initiations-path", required=True)
    parser.add_argument("--raw-events-path",          required=True)
    parser.add_argument("--offer-catalog-path",       required=True)
    parser.add_argument("--output-path",              required=True)
    parser.add_argument("--env", default="local", choices=["local", "emr"])
    args = parser.parse_args()

    spark = get_spark(
        PipelineStep.IPD,
        mode=SparkMode.EMR if args.env == "emr" else SparkMode.LOCAL,
    )
    build_ipd_engagement(
        spark, args.cancel_initiations_path, args.raw_events_path,
        args.offer_catalog_path, args.output_path,
    )
    spark.stop()


if __name__ == "__main__":
    main()
