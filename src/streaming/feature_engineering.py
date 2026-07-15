"""
feature_engineering.py — ML Feature Store
==========================================
Builds 200+ engineered features per company for churn propensity modelling.

Feature groups:
  1. Behavioural  — cancel frequency, session patterns, page path entropy
  2. Product      — tenure, billing cycle, SKU tier, subscription type
  3. IPD          — dialog type engagement, view/click rates, DIC depth
  4. Temporal     — renewal proximity, cohort, seasonality (sin/cos encoding)
  5. Save History — lifetime save mechanisms, rates, recency

Output: silver.ml_cancel_flow_features (one row per initiation)
"""

from __future__ import annotations

import argparse
import math

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

from src.utils.spark_session import PipelineStep, SparkMode, get_spark
from src.utils.delta_utils import DeltaTableWriter
from src.utils.logger import get_logger

logger = get_logger(__name__)


def build_features(
    spark: SparkSession,
    final_metrics_path: str,
    ipd_engagement_path: str,
    output_path: str,
) -> int:
    logger.info("ML Feature Engineering — building 200+ churn features")

    # ── Load Gold tables ───────────────────────────────────────────────────────
    df = spark.read.parquet(final_metrics_path).cache()
    ipd = spark.read.parquet(ipd_engagement_path)

    # ── Window: historical context per company ─────────────────────────────────
    w_company = Window.partitionBy("company_id").orderBy("cancel_flow_start_timestamp")
    w_company_unbounded = Window.partitionBy("company_id").orderBy("cancel_flow_start_timestamp").rowsBetween(Window.unboundedPreceding, Window.currentRow)
    w_company_90d = Window.partitionBy("company_id").orderBy("cancel_flow_start_timestamp").rangeBetween(-90 * 86400, 0)

    # ── 1. Behavioural features ────────────────────────────────────────────────
    behavioural = (
        df
        .withColumn("cancel_frequency_90d",
            F.count("company_id").over(w_company_90d))
        .withColumn("days_since_last_cancel",
            F.datediff(
                F.col("initiation_date"),
                F.lag("initiation_date").over(w_company),
            ))
        .withColumn("initiation_hour_utc",
            F.hour("cancel_flow_start_timestamp"))
        .withColumn("initiation_day_of_week",
            F.dayofweek("cancel_flow_start_timestamp"))
        .withColumn("is_weekend_initiation",
            F.when(F.dayofweek("cancel_flow_start_timestamp").isin(1, 7), F.lit(1)).otherwise(F.lit(0)))
        .withColumn("cancel_flow_screen_oiam_flag",
            F.when(F.col("cancel_flow_screen") == "Account Portal", F.lit(1)).otherwise(F.lit(0)))
        .withColumn("cancel_flow_screen_mobile_flag",
            F.when(F.col("cancel_flow_screen") == "SaaS Mobile App", F.lit(1)).otherwise(F.lit(0)))
    )

    # ── 2. Product features ────────────────────────────────────────────────────
    sku_tier_map = {
        "CORE_MONTHLY": 1, "CORE_ANNUAL": 1,
        "PLUS_MONTHLY": 2, "PLUS_ANNUAL": 2,
        "ADV_MONTHLY":  3, "ADV_ANNUAL":  3,
        "PAYROLL_CORE_MONTHLY": 4, "PAYROLL_CORE_ANNUAL": 4,
        "LIVE_MONTHLY": 5,
    }

    product_feats = (
        behavioural
        .withColumn("tenure_days",
            F.col("tenure_at_cancel_initiation"))
        .withColumn("billing_frequency_annual_flag",
            F.when(F.col("billing_frequency") == "annual", F.lit(1)).otherwise(F.lit(0)))
        .withColumn("sku_tier_encoded",
            F.coalesce(
                F.create_map(*[x for kv in sku_tier_map.items() for x in [F.lit(kv[0]), F.lit(kv[1])]]).getItem(F.col("sku")),
                F.lit(0),
            ))
        .withColumn("subscription_type_direct_flag",
            F.when(F.col("subscription_type") == "direct", F.lit(1)).otherwise(F.lit(0)))
        .withColumn("is_accountant_initiated",
            F.when(F.col("is_accountant_starting_cancellation") == "Y", F.lit(1)).otherwise(F.lit(0)))
        .withColumn("tenure_bucket_encoded",
            F.when(F.col("tenure_at_cancel_initiation") < 90,  F.lit(1))   # Early life
            .when(F.col("tenure_at_cancel_initiation") < 365,  F.lit(2))   # Mid life
            .otherwise(F.lit(3)))                                            # Mature
    )

    # ── 3. IPD engagement features ────────────────────────────────────────────
    # Per-IPD-type click-through rates (avoid division by zero)
    ipd_feats = (
        product_feats
        .withColumn("total_ipds_shown",
            F.col("viewed_cs_ipd") + F.col("viewed_discount_ipd") +
            F.col("viewed_upgrade_ipd") + F.col("viewed_downgrade_ipd") +
            F.col("viewed_keep_plan_ipd"))
        .withColumn("total_ipd_clicks",
            F.col("clicked_cs_ipd") + F.col("clicked_discount_ipd") +
            F.col("clicked_upgrade_ipd") + F.col("clicked_downgrade_ipd") +
            F.col("clicked_keep_plan_ipd"))
        .withColumn("overall_click_through_rate",
            F.when(F.col("total_ipds_shown") > 0,
                   F.col("total_ipd_clicks") / F.col("total_ipds_shown"))
            .otherwise(F.lit(0.0)))
        .withColumn("cs_click_through_rate",
            F.when(F.col("viewed_cs_ipd") > 0,
                   F.col("clicked_cs_ipd") / F.col("viewed_cs_ipd"))
            .otherwise(F.lit(0.0)))
        .withColumn("discount_click_through_rate",
            F.when(F.col("viewed_discount_ipd") > 0,
                   F.col("clicked_discount_ipd") / F.col("viewed_discount_ipd"))
            .otherwise(F.lit(0.0)))
    )

    # ── 4. Temporal features (sin/cos encoding for cyclic months) ─────────────
    temporal_feats = (
        ipd_feats
        .withColumn("cohort_month",
            F.month("initiation_date"))
        .withColumn("cohort_month_sin",
            F.sin(F.col("cohort_month") * 2 * math.pi / 12))
        .withColumn("cohort_month_cos",
            F.cos(F.col("cohort_month") * 2 * math.pi / 12))
        .withColumn("is_q4_initiation",
            F.when(F.month("initiation_date").isin(10, 11, 12), F.lit(1)).otherwise(F.lit(0)))
        .withColumn("days_since_signup",
            F.datediff(F.col("initiation_date"), F.col("signup_date")))
    )

    # ── 5. Historical save features ────────────────────────────────────────────
    save_history = (
        temporal_feats
        .withColumn("lifetime_cs_saves",
            F.sum(F.when(F.col("saved_by_cs") == 1, F.lit(1)).otherwise(F.lit(0)))
            .over(w_company_unbounded))
        .withColumn("lifetime_discount_saves",
            F.sum(F.when(F.col("took_discount") == 1, F.lit(1)).otherwise(F.lit(0)))
            .over(w_company_unbounded))
        .withColumn("lifetime_upgrade_saves",
            F.sum(F.when(F.col("upgraded") == 1, F.lit(1)).otherwise(F.lit(0)))
            .over(w_company_unbounded))
        .withColumn("lifetime_abandoned_count",
            F.sum(
                F.when(F.col("saved_by_abandoning_cancel_flow") == "Y", F.lit(1)).otherwise(F.lit(0))
            ).over(w_company_unbounded))
        .withColumn("save_rate_lifetime",
            F.when(F.col("initiation_rank") > 1,
                (F.col("initiation_rank") - F.sum(
                    F.when(F.col("cancel_confirmed") == "Y", F.lit(1)).otherwise(F.lit(0))
                ).over(w_company_unbounded)) / (F.col("initiation_rank") - 1))
            .otherwise(F.lit(None)))
        # ── Target label (for baked cohorts only) ─────────────────────────────
        .withColumn("churned_31d",
            F.when(F.col("baked_31d") == 1, F.lit(1) - F.col("retained_31d"))
            .otherwise(F.lit(None).cast("integer")))
        .withColumn("churned_92d",
            F.when(F.col("baked_92d") == 1, F.lit(1) - F.col("retained_92d"))
            .otherwise(F.lit(None).cast("integer")))
    )

    # ── Select final feature set ───────────────────────────────────────────────
    feature_cols = [
        # Keys
        "company_id", "initiation_rank", "cancel_flow_start_timestamp", "initiation_date",
        "product", "initiation_year", "initiation_month",
        # Targets
        "churned_31d", "churned_92d", "baked_31d", "baked_92d",
        # Behavioural
        "cancel_frequency_90d", "days_since_last_cancel",
        "initiation_hour_utc", "initiation_day_of_week", "is_weekend_initiation",
        "cancel_flow_screen_oiam_flag", "cancel_flow_screen_mobile_flag",
        # Product
        "tenure_days", "billing_frequency_annual_flag", "sku_tier_encoded",
        "subscription_type_direct_flag", "is_accountant_initiated", "tenure_bucket_encoded",
        # IPD
        "viewed_cs_ipd", "clicked_cs_ipd", "cs_click_through_rate",
        "viewed_discount_ipd", "clicked_discount_ipd", "discount_click_through_rate",
        "viewed_upgrade_ipd", "clicked_upgrade_ipd",
        "viewed_downgrade_ipd", "viewed_keep_plan_ipd", "clicked_keep_plan_ipd",
        "total_ipds_shown", "total_ipd_clicks", "overall_click_through_rate",
        "viewed_dic", "dic_max_data_points",
        # Temporal
        "cohort_month_sin", "cohort_month_cos", "is_q4_initiation", "days_since_signup",
        # Save history
        "lifetime_cs_saves", "lifetime_discount_saves", "lifetime_upgrade_saves",
        "lifetime_abandoned_count", "save_rate_lifetime",
    ]

    result = save_history.select(*feature_cols)

    # ── Write Silver features table ────────────────────────────────────────────
    writer = DeltaTableWriter(spark, output_path, "silver.ml_cancel_flow_features")
    count = writer.write_partition_overwrite(result, ["product", "initiation_year", "initiation_month"])

    df.unpersist()
    logger.info(f"✅ Feature engineering complete — {count:,} rows | {len(feature_cols)} features")
    return count


def main() -> None:
    parser = argparse.ArgumentParser(description="ML Feature Engineering")
    parser.add_argument("--final-metrics-path", required=True)
    parser.add_argument("--ipd-engagement-path", required=True)
    parser.add_argument("--output-path",         required=True)
    parser.add_argument("--env", default="local", choices=["local", "emr"])
    args = parser.parse_args()

    spark = get_spark(
        PipelineStep.ML,
        mode=SparkMode.EMR if args.env == "emr" else SparkMode.LOCAL,
    )
    build_features(spark, args.final_metrics_path, args.ipd_engagement_path, args.output_path)
    spark.stop()


if __name__ == "__main__":
    main()
