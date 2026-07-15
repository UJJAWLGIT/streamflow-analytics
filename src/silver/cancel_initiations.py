"""
cancel_initiations.py — Step 1: Silver Layer
=============================================
Builds the cancel-initiation grain from raw clickstream events.

Key features:
  - All-product coverage (no QBO/product filter)
  - Tri-taxonomy confirmation events (cancel_success, yes_cancel, cancelation flow)
  - Window functions: initiation_rank + observation window boundary
  - Upgrade / downgrade detection within 1-day window
  - Accountant-initiated cancel support (country filter fix)
  - Delta Lake partitioned INSERT OVERWRITE
  - OPTIMIZE + ZORDER after write

Partition: (product, initiation_year, initiation_month)
Target:    silver.stg_cancel_initiations
"""

from __future__ import annotations

import argparse
import logging
from datetime import date

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from src.utils.spark_session import PipelineStep, SparkMode, get_spark
from src.utils.delta_utils import DeltaTableWriter, assert_row_counts_match
from src.utils.logger import get_logger

logger = get_logger(__name__)


# ── Schema definition ──────────────────────────────────────────────────────────

OUTPUT_SCHEMA = T.StructType([
    T.StructField("company_id",                        T.LongType(),      False),
    T.StructField("signup_date",                       T.DateType(),      True),
    T.StructField("tenure_at_cancel_initiation",       T.IntegerType(),   True),
    T.StructField("sku",                               T.StringType(),    True),
    T.StructField("billing_frequency",                 T.StringType(),    True),
    T.StructField("subscription_type",                 T.StringType(),    True),
    T.StructField("properties_url_host_name",          T.StringType(),    True),
    T.StructField("ua_parser_device_type",             T.StringType(),    True),
    T.StructField("context_page_path",                 T.StringType(),    True),
    T.StructField("accountant_id_starting_cancellation", T.StringType(), True),
    T.StructField("is_accountant_starting_cancellation", T.StringType(), False),
    T.StructField("initiation_date",                   T.DateType(),      True),
    T.StructField("initiation_timestamp",              T.TimestampType(), False),
    T.StructField("initiation_rank",                   T.IntegerType(),   False),
    T.StructField("window_end_timestamp",              T.TimestampType(), False),
    T.StructField("cancel_confirmed",                  T.IntegerType(),   False),
    T.StructField("confirmation_timestamp",            T.TimestampType(), True),
    T.StructField("upgraded",                          T.IntegerType(),   False),
    T.StructField("upgrade_timestamp",                 T.TimestampType(), True),
    T.StructField("downgraded",                        T.IntegerType(),   False),
    T.StructField("downgrade_timestamp",               T.TimestampType(), True),
    # Partition keys
    T.StructField("product",                           T.StringType(),    True),
    T.StructField("initiation_year",                   T.StringType(),    True),
    T.StructField("initiation_month",                  T.StringType(),    True),
])


def build_cancel_initiations(
    spark: SparkSession,
    raw_events_path: str,
    companies_path: str,
    output_path: str,
    start_date: str,
    end_date: str,
) -> int:
    """
    Build the cancel initiation grain.

    Returns:
        Row count written.
    """
    logger.info(f"Step 1 — Cancel Initiations: {start_date} → {end_date}")

    # ── Load raw events (partition-pruned) ────────────────────────────────────
    raw = (
        spark.read.parquet(raw_events_path)
        .filter(F.col("event_date").between(start_date, end_date))
        .cache()
    )
    logger.info(f"Raw events loaded: {raw.count():,}")

    # ── 1. Cancel Initiations CTE ──────────────────────────────────────────────
    # Broadened matching: covers all products + both event name formats
    cancel_initiations = (
        raw
        .filter(
            F.col("event").isin("workflow: started", "workflow:started")
            & F.col("properties_object_detail").isin("cancel", "cancellation_workflow")
            & F.col("properties_ui_object_detail").isin("cancel_subscription", "cancel")
        )
        .groupBy("company_id", F.col("event_timestamp").alias("initiation_timestamp"))
        .agg(
            F.max("product").alias("product"),
            F.max("sku").alias("sku"),
            F.max("billing_frequency").alias("billing_frequency"),
            F.max("subscription_type").alias("subscription_type"),
            F.max("properties_url_host_name").alias("properties_url_host_name"),
            F.max("ua_parser_device_type").alias("ua_parser_device_type"),
            F.max("context_page_path").alias("context_page_path"),
            # Accountant realm: non-empty = accountant-initiated
            F.max(
                F.when(
                    F.length(F.trim(F.coalesce(F.col("accountant_realm_id"), F.lit("")))) > 0,
                    F.col("accountant_realm_id"),
                )
            ).alias("accountant_id_starting_cancellation"),
        )
        .withColumn(
            "is_accountant_starting_cancellation",
            F.when(
                F.length(F.trim(F.coalesce(
                    F.col("accountant_id_starting_cancellation"), F.lit("")
                ))) > 0,
                F.lit("Y"),
            ).otherwise(F.lit("N")),
        )
        .withColumn("initiation_timestamp", F.to_timestamp("initiation_timestamp"))
        .withColumn("initiation_date",      F.to_date("initiation_timestamp"))
    )

    # ── 2. Window functions: rank + observation window ─────────────────────────
    w_company_sku = Window.partitionBy("company_id", "sku").orderBy("initiation_timestamp")

    cancel_windowed = (
        cancel_initiations
        .withColumn("next_initiation_timestamp",
                    F.lead("initiation_timestamp").over(w_company_sku))
        .withColumn("initiation_rank", F.row_number().over(w_company_sku))
        .withColumn(
            "window_end_timestamp",
            F.coalesce(
                F.col("next_initiation_timestamp"),
                F.col("initiation_timestamp") + F.expr("INTERVAL 1 HOUR"),
            ),
        )
    ).cache()

    # ── 3. Cancel Confirmations — tri-taxonomy ─────────────────────────────────
    #
    # Generation 3 (2024-05-07+): cancel_success
    confirm_new = (
        raw
        .filter(
            F.col("event").isin("workflow: completed", "workflow:completed")
            & (F.col("properties_object_detail") == "cancel")
            & (F.col("properties_ui_object_detail") == "cancel_success")
        )
        .select("company_id", F.to_timestamp("event_timestamp").alias("cfr_ts"))
    )
    # Generation 2: yes_cancel (legacy single-screen)
    confirm_yes_cancel = (
        raw
        .filter(
            F.col("event").isin(
                "workflow: engaged", "workflow:engaged",
                "widget: engaged", "widget:engaged",
            )
            & F.col("properties_object_detail").isin("cancellation_workflow", "cancel")
            & (F.col("properties_ui_object_detail") == "yes_cancel")
        )
        .select("company_id", F.to_timestamp("event_timestamp").alias("cfr_ts"))
    )
    # Generation 1: cancelation flow (legacy multi-screen)
    confirm_legacy = (
        raw
        .filter(
            F.col("event").isin("cancelation flow: viewed", "cancelation flow:viewed")
            & (F.col("properties_ui_access_point") == "cancel success")
            & (
                (F.col("properties_screen") == "cancel complete")
                | F.col("properties_object_detail").isin("canceled", "cancel trowser")
            )
        )
        .select("company_id", F.to_timestamp("event_timestamp").alias("cfr_ts"))
    )

    all_confirmations = confirm_new.union(confirm_yes_cancel).union(confirm_legacy)

    # Join confirmations to initiations within window
    cancel_confirmations = (
        cancel_windowed.alias("ci")
        .join(all_confirmations.alias("cfr"), "company_id")
        .filter(
            (F.col("cfr.cfr_ts") >= F.col("ci.initiation_timestamp"))
            & (F.col("cfr.cfr_ts") < F.col("ci.window_end_timestamp"))
        )
        .groupBy("ci.company_id", "ci.initiation_timestamp")
        .agg(
            F.lit(1).alias("cancel_confirmed"),
            F.min("cfr.cfr_ts").alias("confirmation_timestamp"),
        )
    )

    # ── 4. Upgrade events (within 1 day of initiation) ─────────────────────────
    upgrade_events = (
        raw
        .filter(
            (F.col("event") == "workflow: completed")
            & (F.col("properties_object_detail") == "upgrade")
            & (F.col("properties_ui_object_detail") == "get_started")
        )
        .select("company_id", F.to_timestamp("event_timestamp").alias("ug_ts"))
    )
    upgrades = (
        cancel_windowed.alias("ci")
        .join(upgrade_events.alias("ug"), "company_id")
        .filter(
            (F.col("ug.ug_ts") >= F.col("ci.initiation_timestamp"))
            & (F.col("ug.ug_ts") <= F.col("ci.initiation_timestamp") + F.expr("INTERVAL 1 DAY"))
        )
        .groupBy("ci.company_id", "ci.initiation_timestamp")
        .agg(F.lit(1).alias("upgraded"), F.min("ug.ug_ts").alias("upgrade_timestamp"))
    )

    # ── 5. Downgrade events ────────────────────────────────────────────────────
    downgrade_events = (
        raw
        .filter(
            (F.col("event") == "workflow: completed")
            & (F.col("properties_object_detail") == "downgrade")
            & (F.col("properties_ui_object_detail") == "get_started")
        )
        .select("company_id", F.to_timestamp("event_timestamp").alias("dg_ts"))
    )
    downgrades = (
        cancel_windowed.alias("ci")
        .join(downgrade_events.alias("dg"), "company_id")
        .filter(
            (F.col("dg.dg_ts") >= F.col("ci.initiation_timestamp"))
            & (F.col("dg.dg_ts") <= F.col("ci.initiation_timestamp") + F.expr("INTERVAL 1 DAY"))
        )
        .groupBy("ci.company_id", "ci.initiation_timestamp")
        .agg(F.lit(1).alias("downgraded"), F.min("dg.dg_ts").alias("downgrade_timestamp"))
    )

    # ── 6. Company dimension join ──────────────────────────────────────────────
    companies = spark.read.parquet(companies_path)

    # ── 7. Final SELECT ────────────────────────────────────────────────────────
    result = (
        cancel_windowed.alias("ci")
        .join(
            companies.alias("co"),
            F.col("ci.company_id") == F.col("co.company_id").cast("string"),
            "inner",
        )
        # Country filter: US companies OR accountant-initiated (any country)
        .filter(
            (F.col("co.country") == "United States")
            | (F.col("ci.is_accountant_starting_cancellation") == "Y")
        )
        .filter(F.col("co.is_suspicious") == 0)
        .join(
            cancel_confirmations.alias("cc"),
            (F.col("ci.company_id") == F.col("cc.company_id")) &
            (F.col("ci.initiation_timestamp") == F.col("cc.initiation_timestamp")),
            "left",
        )
        .join(
            upgrades.alias("ug"),
            (F.col("ci.company_id") == F.col("ug.company_id")) &
            (F.col("ci.initiation_timestamp") == F.col("ug.initiation_timestamp")),
            "left",
        )
        .join(
            downgrades.alias("dg"),
            (F.col("ci.company_id") == F.col("dg.company_id")) &
            (F.col("ci.initiation_timestamp") == F.col("dg.initiation_timestamp")),
            "left",
        )
        # Write window filter — prevents partition overwrite for out-of-window dates
        .filter(F.col("ci.initiation_date").between(start_date, end_date))
        .select(
            F.col("co.company_id").cast("long").alias("company_id"),
            F.to_date("co.signup_date").alias("signup_date"),
            F.datediff(F.col("ci.initiation_date"), F.to_date("co.signup_date")).alias("tenure_at_cancel_initiation"),
            F.col("ci.sku"),
            F.col("ci.billing_frequency"),
            F.col("ci.subscription_type"),
            F.col("ci.properties_url_host_name"),
            F.col("ci.ua_parser_device_type"),
            F.col("ci.context_page_path"),
            F.col("ci.accountant_id_starting_cancellation"),
            F.col("ci.is_accountant_starting_cancellation"),
            F.col("ci.initiation_date"),
            F.col("ci.initiation_timestamp"),
            F.col("ci.initiation_rank"),
            F.col("ci.window_end_timestamp"),
            F.coalesce(F.col("cc.cancel_confirmed"), F.lit(0)).alias("cancel_confirmed"),
            F.col("cc.confirmation_timestamp"),
            F.coalesce(F.col("ug.upgraded"), F.lit(0)).alias("upgraded"),
            F.col("ug.upgrade_timestamp"),
            F.coalesce(F.col("dg.downgraded"), F.lit(0)).alias("downgraded"),
            F.col("dg.downgrade_timestamp"),
            # Partition keys
            F.col("ci.product"),
            F.date_format("ci.initiation_date", "yyyy").alias("initiation_year"),
            F.date_format("ci.initiation_date", "MM").alias("initiation_month"),
        )
    )

    # ── 8. Write Delta partitioned output ──────────────────────────────────────
    writer = DeltaTableWriter(spark, output_path, "silver.stg_cancel_initiations")
    count = writer.write_partition_overwrite(result, ["product", "initiation_year", "initiation_month"])

    # OPTIMIZE + ZORDER for downstream join performance
    writer.optimize(zorder_cols=["company_id", "initiation_timestamp"])

    raw.unpersist()
    cancel_windowed.unpersist()

    logger.info(f"✅ Step 1 complete — {count:,} rows")
    return count


def main() -> None:
    parser = argparse.ArgumentParser(description="Step 1: Cancel Initiations (Silver)")
    parser.add_argument("--raw-events-path", required=True)
    parser.add_argument("--companies-path",  required=True)
    parser.add_argument("--output-path",     required=True)
    parser.add_argument("--start-date",      required=True)
    parser.add_argument("--end-date",        required=True)
    parser.add_argument("--env",             default="local", choices=["local", "emr"])
    args = parser.parse_args()

    spark = get_spark(
        PipelineStep.INITIATIONS,
        mode=SparkMode.EMR if args.env == "emr" else SparkMode.LOCAL,
    )
    build_cancel_initiations(
        spark, args.raw_events_path, args.companies_path,
        args.output_path, args.start_date, args.end_date,
    )
    spark.stop()


if __name__ == "__main__":
    main()
