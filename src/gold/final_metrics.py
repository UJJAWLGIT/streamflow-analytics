"""
final_metrics.py — Step 4: Gold Layer (3★ Consumable)
======================================================
Builds the final cancel-flow reporting table: one row per cancel initiation.

Phase-2 key changes:
  - IXP filter COMPLETELY REMOVED — one row per initiation, all companies
  - cancel_confirmed INT → STRING (Y/N)
  - treatment_name / first_assignment_date columns kept in DDL (always NULL)
  - Partitioned by (product, initiation_year, initiation_month)
  - Delta Lake DROP + CREATE + OPTIMIZE + ZORDER after write
  - Row count invariant: Step 1 = Step 3 = Step 4 (validated)

Grain:     (company_id, initiation_rank, cancel_flow_start_timestamp)
Partition: (product, initiation_year, initiation_month)
Target:    gold.rpt_cancel_flow_final_metrics
"""

from __future__ import annotations

import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.utils.spark_session import PipelineStep, SparkMode, get_spark
from src.utils.delta_utils import DeltaTableWriter, assert_row_counts_match
from src.utils.logger import get_logger

logger = get_logger(__name__)


def classify_cancel_flow_screen() -> F.Column:
    """Cancel flow screen classification from URL host + device type + page path."""
    return (
        F.when(
            F.lower(F.coalesce(F.col("ci.properties_url_host_name"), F.lit(""))).contains("accounts.")
            | F.lower(F.coalesce(F.col("ci.context_page_path"), F.lit(""))).contains("accountmanager"),
            F.lit("Account Portal"),
        )
        .when(
            F.lower(F.coalesce(F.col("ci.ua_parser_device_type"), F.lit(""))).isin("mobile", "tablet", "smartphone", "phone")
            | F.lower(F.coalesce(F.col("ci.properties_url_host_name"), F.lit(""))).contains("mobile"),
            F.lit("SaaS Mobile App"),
        )
        .when(
            F.lower(F.coalesce(F.col("ci.properties_url_host_name"), F.lit(""))).contains("app.")
            | F.lower(F.coalesce(F.col("ci.properties_url_host_name"), F.lit(""))).contains("saas."),
            F.lit("SaaS Web App"),
        )
        .otherwise(F.lit("Unknown"))
    )


def build_final_metrics(
    spark: SparkSession,
    cancel_initiations_path: str,
    ipd_engagement_path: str,
    save_attribution_path: str,
    subscriber_status_path: str,
    output_path: str,
) -> int:
    logger.info("Step 4 — Final Metrics (Gold ⭐⭐⭐)")

    # ── Load inputs ────────────────────────────────────────────────────────────
    ci = spark.read.parquet(cancel_initiations_path).cache()
    sa = spark.read.parquet(save_attribution_path)
    status = spark.read.parquet(subscriber_status_path)

    ci_count = ci.count()
    sa_count = sa.count()
    assert_row_counts_match(ci_count, sa_count, "stg_cancel_initiations", "stg_save_attribution")

    # ── IPD engagement flags (aggregate from Step 2 offer-grain) ──────────────
    ipd = spark.read.parquet(ipd_engagement_path)

    ipd_types = {
        "cs":        "CS IPD",
        "discount":  "Discount IPD",
        "upgrade":   "Upgrade IPD",
        "downgrade": "Downgrade IPD",
        "keepplan":  "Keep my Plan IPD",
    }

    engagement_flags = (
        ipd
        .groupBy("company_id", "initiation_rank", "initiation_timestamp")
        .agg(
            *[
                F.max(
                    F.when(F.col("ipd_type") == label,
                           F.coalesce(F.col("viewed_ipd"), F.lit(0))).otherwise(F.lit(0))
                ).alias(f"viewed_{key}_ipd")
                for key, label in ipd_types.items()
            ],
            *[
                F.max(
                    F.when(F.col("ipd_type") == label,
                           F.coalesce(F.col("clicked_ipd"), F.lit(0))).otherwise(F.lit(0))
                ).alias(f"clicked_{key}_ipd")
                for key, label in ipd_types.items()
            ],
            F.max(F.coalesce(F.col("viewed_dic_component"),      F.lit(0))).alias("viewed_dic"),
            F.max(F.coalesce(F.col("number_of_data_points_shown"), F.lit(0))).alias("dic_max_data_points"),
        )
    )

    # ── Retention joins (LEFT JOIN on 31d + 92d status) ────────────────────────
    status_open = status.filter(F.col("open_subscriber") == 1).select("company_id", "date_of")

    # ── Final SELECT ───────────────────────────────────────────────────────────
    result = (
        ci.alias("ci")
        .join(
            sa.alias("sa"),
            (F.col("sa.company_id") == F.col("ci.company_id"))
            & (F.col("sa.initiation_rank") == F.col("ci.initiation_rank"))
            & (F.col("sa.initiation_timestamp") == F.col("ci.initiation_timestamp")),
            "left",
        )
        .join(
            engagement_flags.alias("ef"),
            (F.col("ef.company_id") == F.col("ci.company_id"))
            & (F.col("ef.initiation_rank") == F.col("ci.initiation_rank"))
            & (F.col("ef.initiation_timestamp") == F.col("ci.initiation_timestamp")),
            "left",
        )
        .join(
            status_open.alias("s31"),
            (F.col("s31.company_id") == F.col("ci.company_id"))
            & (F.col("s31.date_of") == F.date_add(F.col("ci.initiation_date"), 31)),
            "left",
        )
        .join(
            status_open.alias("s92"),
            (F.col("s92.company_id") == F.col("ci.company_id"))
            & (F.col("s92.date_of") == F.date_add(F.col("ci.initiation_date"), 92)),
            "left",
        )
        .select(
            # ── Identity ──────────────────────────────────────────────────────
            F.col("ci.company_id"),
            F.col("ci.company_id").cast("string").alias("realm_id"),
            F.col("ci.signup_date"),
            F.col("ci.tenure_at_cancel_initiation"),
            # ── Product context ───────────────────────────────────────────────
            F.col("ci.sku"),
            F.col("ci.billing_frequency"),
            F.col("ci.subscription_type"),
            # ── Initiation ────────────────────────────────────────────────────
            F.col("ci.initiation_date"),
            F.col("ci.initiation_timestamp").alias("cancel_flow_start_timestamp"),
            F.col("ci.initiation_rank"),
            # ── Confirmation (INT → Y/N) ──────────────────────────────────────
            F.when(F.col("ci.cancel_confirmed") == 1, F.lit("Y")).otherwise(F.lit("N")).alias("cancel_confirmed"),
            F.col("ci.confirmation_timestamp").alias("cancel_confirmation_timestamp"),
            # ── Save flags (Y/N) ──────────────────────────────────────────────
            F.when(F.coalesce(F.col("sa.saved_by_cs"),    F.lit(0)) == 1, F.lit("Y")).otherwise(F.lit("N")).alias("saved_by_customer_support"),
            F.when(F.coalesce(F.col("sa.upgraded"),       F.lit(0)) == 1, F.lit("Y")).otherwise(F.lit("N")).alias("saved_by_upgrading"),
            F.when(F.coalesce(F.col("sa.downgraded"),     F.lit(0)) == 1, F.lit("Y")).otherwise(F.lit("N")).alias("saved_by_downgrading"),
            F.when(F.coalesce(F.col("sa.took_discount"),  F.lit(0)) == 1, F.lit("Y")).otherwise(F.lit("N")).alias("saved_by_taking_discount"),
            F.col("sa.discount_take_timestamp"),
            F.coalesce(F.col("sa.saved_by_abandoning"), F.lit("N")).alias("saved_by_abandoning_cancel_flow"),
            # ── Accountant context ────────────────────────────────────────────
            F.col("ci.is_accountant_starting_cancellation"),
            F.col("ci.accountant_id_starting_cancellation"),
            # ── Cancel flow screen ────────────────────────────────────────────
            classify_cancel_flow_screen().alias("cancel_flow_screen"),
            F.col("ci.properties_url_host_name"),
            F.col("ci.ua_parser_device_type"),
            F.col("ci.context_page_path"),
            # ── Save attribution ──────────────────────────────────────────────
            F.col("sa.save_attribution"),
            F.coalesce(F.col("sa.contacted_by_cs"),  F.lit(0)).alias("contacted_by_cs"),
            F.coalesce(F.col("sa.saved_by_cs"),      F.lit(0)).alias("saved_by_cs"),
            F.coalesce(F.col("sa.upgraded"),         F.lit(0)).alias("upgraded"),
            F.col("sa.upgrade_timestamp"),
            F.coalesce(F.col("sa.downgraded"),       F.lit(0)).alias("downgraded"),
            F.col("sa.downgrade_timestamp"),
            F.coalesce(F.col("sa.took_discount"),    F.lit(0)).alias("took_discount"),
            # ── IPD engagement flags ──────────────────────────────────────────
            F.coalesce(F.col("ef.viewed_cs_ipd"),        F.lit(0)).alias("viewed_cs_ipd"),
            F.coalesce(F.col("ef.clicked_cs_ipd"),       F.lit(0)).alias("clicked_cs_ipd"),
            F.coalesce(F.col("ef.viewed_discount_ipd"),  F.lit(0)).alias("viewed_discount_ipd"),
            F.coalesce(F.col("ef.clicked_discount_ipd"), F.lit(0)).alias("clicked_discount_ipd"),
            F.coalesce(F.col("ef.viewed_upgrade_ipd"),   F.lit(0)).alias("viewed_upgrade_ipd"),
            F.coalesce(F.col("ef.clicked_upgrade_ipd"),  F.lit(0)).alias("clicked_upgrade_ipd"),
            F.coalesce(F.col("ef.viewed_downgrade_ipd"), F.lit(0)).alias("viewed_downgrade_ipd"),
            F.coalesce(F.col("ef.clicked_downgrade_ipd"),F.lit(0)).alias("clicked_downgrade_ipd"),
            F.coalesce(F.col("ef.viewed_keepplan_ipd"),  F.lit(0)).alias("viewed_keep_plan_ipd"),
            F.coalesce(F.col("ef.clicked_keepplan_ipd"), F.lit(0)).alias("clicked_keep_plan_ipd"),
            F.coalesce(F.col("ef.viewed_dic"),           F.lit(0)).alias("viewed_dic"),
            F.coalesce(F.col("ef.dic_max_data_points"),  F.lit(0)).alias("dic_max_data_points"),
            # ── Bake + retention flags ────────────────────────────────────────
            F.when(F.date_add(F.col("ci.initiation_date"), 31) < F.current_date(), F.lit(1)).otherwise(F.lit(0)).alias("baked_31d"),
            F.when(F.col("s31.company_id").isNotNull(), F.lit(1)).otherwise(F.lit(0)).alias("retained_31d"),
            F.when(F.date_add(F.col("ci.initiation_date"), 92) < F.current_date(), F.lit(1)).otherwise(F.lit(0)).alias("baked_92d"),
            F.when(F.col("s92.company_id").isNotNull(), F.lit(1)).otherwise(F.lit(0)).alias("retained_92d"),
            # ── Legacy columns (always NULL — deprecated from Phase-1) ────────
            F.lit(None).cast("string").alias("treatment_name"),
            F.lit(None).cast("date").alias("first_assignment_date"),
            # ── Audit ─────────────────────────────────────────────────────────
            F.current_timestamp().alias("dwh_create_date"),
            F.current_timestamp().alias("dwh_update_date"),
            # ── Partition keys ────────────────────────────────────────────────
            F.col("ci.product"),
            F.date_format(F.col("ci.initiation_date"), "yyyy").alias("initiation_year"),
            F.date_format(F.col("ci.initiation_date"), "MM").alias("initiation_month"),
        )
    )

    # ── Write: DROP + CREATE + partitioned INSERT OVERWRITE ───────────────────
    writer = DeltaTableWriter(spark, output_path, "gold.rpt_cancel_flow_final_metrics")
    count = writer.create_or_replace(result, ["product", "initiation_year", "initiation_month"])

    # ── Validate row count invariant: Step 1 = Step 3 = Step 4 ───────────────
    assert_row_counts_match(ci_count, count, "stg_cancel_initiations", "rpt_cancel_flow_final_metrics")

    # ── OPTIMIZE + ZORDER (sub-500ms query SLO) ───────────────────────────────
    writer.optimize(zorder_cols=["company_id", "initiation_date", "product"])

    ci.unpersist()
    logger.info(f"✅ Step 4 complete — {count:,} rows | OPTIMIZE+ZORDER done")
    return count


def main() -> None:
    parser = argparse.ArgumentParser(description="Step 4: Final Cancel Flow Metrics (Gold)")
    parser.add_argument("--cancel-initiations-path", required=True)
    parser.add_argument("--ipd-engagement-path",     required=True)
    parser.add_argument("--save-attribution-path",   required=True)
    parser.add_argument("--subscriber-status-path",  required=True)
    parser.add_argument("--output-path",             required=True)
    parser.add_argument("--env", default="local", choices=["local", "emr"])
    args = parser.parse_args()

    spark = get_spark(
        PipelineStep.FINAL,
        mode=SparkMode.EMR if args.env == "emr" else SparkMode.LOCAL,
    )
    build_final_metrics(
        spark,
        args.cancel_initiations_path,
        args.ipd_engagement_path,
        args.save_attribution_path,
        args.subscriber_status_path,
        args.output_path,
    )
    spark.stop()


if __name__ == "__main__":
    main()
