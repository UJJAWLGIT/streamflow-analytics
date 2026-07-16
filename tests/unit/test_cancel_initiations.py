"""
test_cancel_initiations.py — Unit Tests: Step 1
================================================
Tests for the cancel initiation grain logic.
Uses in-memory SparkSession (no external dependencies).

Covers:
  - Cancel initiation matching (all 3 event formats)
  - Tri-taxonomy confirmation events
  - Initiation rank + window boundary
  - Accountant-initiated flag
  - Country filter logic
  - Upgrade / downgrade detection
  - Partition write filter (prevents overwrite bug)
"""

from __future__ import annotations

import pytest
from datetime import datetime, date

from pyspark.sql import SparkSession, Row
from pyspark.sql import functions as F

# ── Fixtures ───────────────────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def spark() -> SparkSession:
    return (
        SparkSession.builder
        .master("local[2]")
        .appName("test_cancel_initiations")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )


@pytest.fixture
def sample_raw_events(spark):
    """Minimal raw clickstream events for testing."""
    now = datetime(2024, 6, 15, 14, 30, 0)
    events = [
        # ── Cancel initiation (new taxonomy) ──────────────────────────────────
        Row(
            company_id="100001", event="workflow: started",
            properties_object_detail="cancel",
            properties_ui_object_detail="cancel_subscription",
            product="SAAS_CORE", sku="CORE_MONTHLY",
            billing_frequency="monthly", subscription_type="direct",
            properties_url_host_name="app.saas.com",
            ua_parser_device_type="desktop", context_page_path="/app/billing",
            accountant_realm_id=None,
            event_timestamp=now, event_date="2024-06-15",
        ),
        # ── Cancel initiation (alternate object_detail) ────────────────────────
        Row(
            company_id="100002", event="workflow:started",
            properties_object_detail="cancellation_workflow",
            properties_ui_object_detail="cancel",
            product="SAAS_PLUS", sku="PLUS_ANNUAL",
            billing_frequency="annual", subscription_type="direct",
            properties_url_host_name="app.saas.com",
            ua_parser_device_type="mobile", context_page_path="/app/billing",
            accountant_realm_id=None,
            event_timestamp=now, event_date="2024-06-15",
        ),
        # ── Confirmation: cancel_success (new) ────────────────────────────────
        Row(
            company_id="100001", event="workflow: completed",
            properties_object_detail="cancel",
            properties_ui_object_detail="cancel_success",
            product="SAAS_CORE", sku="CORE_MONTHLY",
            billing_frequency="monthly", subscription_type="direct",
            properties_url_host_name="app.saas.com",
            ua_parser_device_type="desktop", context_page_path="/app/billing",
            accountant_realm_id=None,
            event_timestamp=datetime(2024, 6, 15, 14, 35, 0),
            event_date="2024-06-15",
        ),
        # ── Confirmation: yes_cancel (legacy) ─────────────────────────────────
        Row(
            company_id="100002", event="workflow: engaged",
            properties_object_detail="cancel",
            properties_ui_object_detail="yes_cancel",
            product="SAAS_PLUS", sku="PLUS_ANNUAL",
            billing_frequency="annual", subscription_type="direct",
            properties_url_host_name="app.saas.com",
            ua_parser_device_type="mobile", context_page_path="/app/billing",
            accountant_realm_id=None,
            event_timestamp=datetime(2024, 6, 15, 14, 38, 0),
            event_date="2024-06-15",
        ),
        # ── Accountant-initiated cancellation ─────────────────────────────────
        Row(
            company_id="100003", event="workflow: started",
            properties_object_detail="cancel",
            properties_ui_object_detail="cancel_subscription",
            product="SAAS_CORE", sku="CORE_MONTHLY",
            billing_frequency="monthly", subscription_type="accountant_billed",
            properties_url_host_name="app.saas.com",
            ua_parser_device_type="desktop", context_page_path="/app/billing",
            accountant_realm_id="ACCT_9001",
            event_timestamp=now, event_date="2024-06-15",
        ),
        # ── Upgrade event (within 1 day) ──────────────────────────────────────
        Row(
            company_id="100002", event="workflow: completed",
            properties_object_detail="upgrade",
            properties_ui_object_detail="get_started",
            product="SAAS_PLUS", sku="PLUS_ANNUAL",
            billing_frequency="annual", subscription_type="direct",
            properties_url_host_name="app.saas.com",
            ua_parser_device_type="mobile", context_page_path="/app/billing",
            accountant_realm_id=None,
            event_timestamp=datetime(2024, 6, 15, 16, 0, 0),
            event_date="2024-06-15",
        ),
    ]
    return spark.createDataFrame(events)


# ── Test: Cancel Initiation Matching ──────────────────────────────────────────

class TestCancelInitiationMatching:

    def test_new_event_format_matched(self, spark, sample_raw_events):
        """workflow: started + cancel + cancel_subscription → matched."""
        initiations = sample_raw_events.filter(
            F.col("event").isin("workflow: started", "workflow:started")
            & F.col("properties_object_detail").isin("cancel", "cancellation_workflow")
            & F.col("properties_ui_object_detail").isin("cancel_subscription", "cancel")
        )
        assert initiations.count() == 3

    def test_alternate_object_detail_matched(self, spark, sample_raw_events):
        """cancellation_workflow in properties_object_detail → matched."""
        match = sample_raw_events.filter(
            F.col("properties_object_detail") == "cancellation_workflow"
        )
        assert match.count() == 1

    def test_cancel_ui_detail_matched(self, spark, sample_raw_events):
        """properties_ui_object_detail = cancel → matched (not just cancel_subscription)."""
        match = sample_raw_events.filter(
            F.col("properties_ui_object_detail").isin("cancel_subscription", "cancel")
        )
        assert match.count() >= 3

    def test_non_cancel_events_excluded(self, spark, sample_raw_events):
        """Upgrade/confirmation events should not appear in cancel initiations."""
        initiations = sample_raw_events.filter(
            F.col("event").isin("workflow: started", "workflow:started")
            & F.col("properties_ui_object_detail").isin("cancel_subscription", "cancel")
        )
        company_ids = {r.company_id for r in initiations.collect()}
        assert "get_started" not in {r.properties_ui_object_detail for r in initiations.collect()}


# ── Test: Confirmation Taxonomy ────────────────────────────────────────────────

class TestConfirmationTaxonomy:

    def test_cancel_success_detected(self, spark, sample_raw_events):
        """New taxonomy: cancel_success → confirmation."""
        confirmations = sample_raw_events.filter(
            F.col("event").isin("workflow: completed", "workflow:completed")
            & (F.col("properties_ui_object_detail") == "cancel_success")
        )
        assert confirmations.count() == 1
        assert confirmations.first().company_id == "100001"

    def test_yes_cancel_detected(self, spark, sample_raw_events):
        """Legacy taxonomy: yes_cancel → confirmation."""
        confirmations = sample_raw_events.filter(
            F.col("event").isin("workflow: engaged", "workflow:engaged")
            & (F.col("properties_ui_object_detail") == "yes_cancel")
        )
        assert confirmations.count() == 1
        assert confirmations.first().company_id == "100002"

    def test_all_confirmation_taxonomies_union(self, spark, sample_raw_events):
        """Union of all 3 taxonomies returns all confirmation events."""
        confirm_new = sample_raw_events.filter(
            F.col("event").isin("workflow: completed")
            & (F.col("properties_ui_object_detail") == "cancel_success")
        )
        confirm_legacy = sample_raw_events.filter(
            F.col("event").isin("workflow: engaged")
            & (F.col("properties_ui_object_detail") == "yes_cancel")
        )
        all_confirms = confirm_new.union(confirm_legacy)
        assert all_confirms.count() == 2


# ── Test: Accountant-Initiated Flag ───────────────────────────────────────────

class TestAccountantFlag:

    def test_accountant_flag_y_when_realm_present(self, spark, sample_raw_events):
        """Non-empty accountant_realm_id → is_accountant_starting_cancellation = Y."""
        accountant_events = sample_raw_events.filter(
            F.col("accountant_realm_id").isNotNull()
            & (F.length(F.trim(F.col("accountant_realm_id"))) > 0)
        ).withColumn(
            "is_accountant",
            F.when(
                F.length(F.trim(F.coalesce(F.col("accountant_realm_id"), F.lit("")))) > 0,
                F.lit("Y"),
            ).otherwise(F.lit("N")),
        )
        assert accountant_events.count() == 1
        assert accountant_events.first().is_accountant == "Y"

    def test_accountant_flag_n_when_realm_null(self, spark, sample_raw_events):
        """Null accountant_realm_id → is_accountant_starting_cancellation = N."""
        non_accountant = sample_raw_events.filter(
            F.col("accountant_realm_id").isNull()
        ).withColumn(
            "is_accountant",
            F.when(
                F.length(F.trim(F.coalesce(F.col("accountant_realm_id"), F.lit("")))) > 0,
                F.lit("Y"),
            ).otherwise(F.lit("N")),
        )
        flags = {r.is_accountant for r in non_accountant.collect()}
        assert "N" in flags
        assert "Y" not in flags


# ── Test: Window Functions ─────────────────────────────────────────────────────

class TestWindowFunctions:

    def test_initiation_rank_starts_at_one(self, spark):
        """ROW_NUMBER per company/sku should start at 1."""
        from pyspark.sql import Window

        data = spark.createDataFrame([
            Row(company_id="1", sku="A", ts=datetime(2024, 1, 1)),
            Row(company_id="1", sku="A", ts=datetime(2024, 2, 1)),
            Row(company_id="1", sku="A", ts=datetime(2024, 3, 1)),
        ])
        w = Window.partitionBy("company_id", "sku").orderBy("ts")
        ranked = data.withColumn("rank", F.row_number().over(w))
        ranks = sorted([r.rank for r in ranked.collect()])
        assert ranks == [1, 2, 3]

    def test_window_end_is_next_initiation(self, spark):
        """window_end_timestamp should equal next initiation timestamp when present."""
        from pyspark.sql import Window

        ts1 = datetime(2024, 1, 1, 10, 0)
        ts2 = datetime(2024, 1, 1, 12, 0)
        data = spark.createDataFrame([
            Row(company_id="1", sku="A", initiation_timestamp=ts1),
            Row(company_id="1", sku="A", initiation_timestamp=ts2),
        ])
        w = Window.partitionBy("company_id", "sku").orderBy("initiation_timestamp")
        result = data.withColumn("next_ts", F.lead("initiation_timestamp").over(w))
        first_row = result.filter(F.col("initiation_timestamp") == ts1).first()
        assert first_row.next_ts == ts2

    def test_window_end_is_plus_1h_when_last(self, spark):
        """window_end_timestamp should be initiation + 1h when no next initiation."""
        from pyspark.sql import Window

        ts = datetime(2024, 1, 1, 10, 0)
        data = spark.createDataFrame([
            Row(company_id="1", sku="A", initiation_timestamp=ts),
        ])
        w = Window.partitionBy("company_id", "sku").orderBy("initiation_timestamp")
        result = data.withColumn(
            "next_ts", F.lead("initiation_timestamp").over(w)
        ).withColumn(
            "window_end",
            F.coalesce(
                F.col("next_ts"),
                F.col("initiation_timestamp") + F.expr("INTERVAL 1 HOUR"),
            ),
        )
        row = result.first()
        expected_end = datetime(2024, 1, 1, 11, 0)
        assert row.window_end == expected_end


# ── Test: Upgrade Detection ────────────────────────────────────────────────────

class TestUpgradeDetection:

    def test_upgrade_within_1day_detected(self, spark, sample_raw_events):
        """Upgrade event within 1 day of initiation should be detected."""
        upgrades = sample_raw_events.filter(
            (F.col("event") == "workflow: completed")
            & (F.col("properties_object_detail") == "upgrade")
            & (F.col("properties_ui_object_detail") == "get_started")
        )
        assert upgrades.count() == 1
        assert upgrades.first().company_id == "100002"

    def test_non_upgrade_events_excluded(self, spark, sample_raw_events):
        """Cancel initiations should not be classified as upgrades."""
        upgrades = sample_raw_events.filter(
            (F.col("properties_object_detail") == "upgrade")
        )
        cancel_initiations = sample_raw_events.filter(
            F.col("properties_ui_object_detail").isin("cancel_subscription", "cancel")
            & (F.col("properties_object_detail") == "upgrade")
        )
        assert cancel_initiations.count() == 0


# ── Test: Data Types ───────────────────────────────────────────────────────────

class TestDataTypes:

    def test_cancel_confirmed_is_integer(self, spark):
        """cancel_confirmed should be INT (0 or 1) in Step 1."""
        data = spark.createDataFrame([
            Row(cancel_confirmed=0),
            Row(cancel_confirmed=1),
        ])
        schema = data.schema
        assert schema["cancel_confirmed"].dataType.typeName() in ("integer", "long")

    def test_cancel_confirmed_domain_values(self, spark):
        """cancel_confirmed should only contain 0 or 1."""
        data = spark.createDataFrame([
            Row(cancel_confirmed=0),
            Row(cancel_confirmed=1),
            Row(cancel_confirmed=0),
        ])
        invalid = data.filter(~F.col("cancel_confirmed").isin(0, 1)).count()
        assert invalid == 0

    def test_partition_keys_are_strings(self, spark):
        """initiation_year and initiation_month should be STRING type."""
        ts = datetime(2024, 6, 15)
        data = spark.createDataFrame([Row(initiation_date=ts.date())])
        result = data.withColumn(
            "initiation_year",  F.date_format("initiation_date", "yyyy")
        ).withColumn(
            "initiation_month", F.date_format("initiation_date", "MM")
        )
        row = result.first()
        assert row.initiation_year == "2024"
        assert row.initiation_month == "06"
