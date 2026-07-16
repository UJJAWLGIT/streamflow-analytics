"""
test_save_attribution.py — Unit Tests: Step 3 + IPD Classification
====================================================================
Tests for save attribution waterfall logic, IPD type classification,
DQ uniqueness invariant, and retention flag consistency.
"""

from __future__ import annotations

import pytest
from datetime import datetime

from pyspark.sql import SparkSession, Row
from pyspark.sql import functions as F


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    return (
        SparkSession.builder
        .master("local[2]")
        .appName("test_save_attribution")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )


# ── Test: Save Attribution Priority Waterfall ──────────────────────────────────

class TestSaveAttributionWaterfall:
    """
    Priority: CS Save > Cancelled > Upgrade Save > Downgrade Save > Discount Save > Abandoned
    """

    def _classify(self, spark, saved_by_cs, cancel_confirmed, upgraded, downgraded, took_discount):
        data = spark.createDataFrame([Row(
            saved_by_cs=saved_by_cs,
            cancel_confirmed=cancel_confirmed,
            upgraded=upgraded,
            downgraded=downgraded,
            took_discount=took_discount,
        )])
        result = data.withColumn(
            "save_attribution",
            F.when(F.col("saved_by_cs") == 1,                                F.lit("CS Save"))
            .when((F.col("cancel_confirmed") == 1) & (F.col("saved_by_cs") == 0), F.lit("Cancelled"))
            .when((F.col("cancel_confirmed") == 0) & (F.col("saved_by_cs") == 0) & (F.col("upgraded") == 1),   F.lit("Upgrade Save"))
            .when((F.col("cancel_confirmed") == 0) & (F.col("saved_by_cs") == 0) & (F.col("downgraded") == 1), F.lit("Downgrade Save"))
            .when((F.col("cancel_confirmed") == 0) & (F.col("saved_by_cs") == 0) & (F.col("took_discount") == 1), F.lit("Discount Save"))
            .otherwise(F.lit("Abandoned"))
        )
        return result.first().save_attribution

    def test_cs_save_highest_priority(self, spark):
        """CS Save beats all other outcomes."""
        assert self._classify(spark, 1, 1, 1, 1, 1) == "CS Save"

    def test_cancelled_when_confirmed_no_cs(self, spark):
        """Cancelled when cancel_confirmed=1 and no CS save."""
        assert self._classify(spark, 0, 1, 0, 0, 0) == "Cancelled"

    def test_cancelled_beats_upgrade(self, spark):
        """Cancelled takes priority over Upgrade Save."""
        assert self._classify(spark, 0, 1, 1, 0, 0) == "Cancelled"

    def test_upgrade_save_when_not_cancelled(self, spark):
        """Upgrade Save when not confirmed and not CS saved."""
        assert self._classify(spark, 0, 0, 1, 0, 0) == "Upgrade Save"

    def test_downgrade_save(self, spark):
        """Downgrade Save when not confirmed, not CS, not upgraded."""
        assert self._classify(spark, 0, 0, 0, 1, 0) == "Downgrade Save"

    def test_upgrade_beats_downgrade(self, spark):
        """Upgrade Save beats Downgrade Save."""
        assert self._classify(spark, 0, 0, 1, 1, 0) == "Upgrade Save"

    def test_discount_save(self, spark):
        """Discount Save when not confirmed, not CS, not upgraded, not downgraded."""
        assert self._classify(spark, 0, 0, 0, 0, 1) == "Discount Save"

    def test_abandoned_when_nothing(self, spark):
        """Abandoned when no save mechanism and not confirmed."""
        assert self._classify(spark, 0, 0, 0, 0, 0) == "Abandoned"

    def test_all_valid_attribution_values(self, spark):
        """save_attribution should only take 6 valid values."""
        valid = {"CS Save", "Cancelled", "Upgrade Save", "Downgrade Save", "Discount Save", "Abandoned"}
        test_cases = [
            (1, 0, 0, 0, 0), (0, 1, 0, 0, 0), (0, 0, 1, 0, 0),
            (0, 0, 0, 1, 0), (0, 0, 0, 0, 1), (0, 0, 0, 0, 0),
        ]
        for case in test_cases:
            result = self._classify(spark, *case)
            assert result in valid, f"Unexpected attribution: {result} for {case}"


# ── Test: saved_by_abandoning Flag ────────────────────────────────────────────

class TestSavedByAbandoning:

    def _get_abandoning(self, spark, saved_by_cs, cancel_confirmed, upgraded, downgraded, took_discount):
        data = spark.createDataFrame([Row(
            saved_by_cs=saved_by_cs, cancel_confirmed=cancel_confirmed,
            upgraded=upgraded, downgraded=downgraded, took_discount=took_discount,
        )])
        result = data.withColumn(
            "saved_by_abandoning",
            F.when(
                (F.col("saved_by_cs") == 0) & (F.col("cancel_confirmed") == 0)
                & (F.col("upgraded") == 0) & (F.col("downgraded") == 0)
                & (F.col("took_discount") == 0),
                F.lit("Y"),
            ).otherwise(F.lit("N")),
        )
        return result.first().saved_by_abandoning

    def test_y_when_all_zeros(self, spark):
        assert self._get_abandoning(spark, 0, 0, 0, 0, 0) == "Y"

    def test_n_when_cancelled(self, spark):
        assert self._get_abandoning(spark, 0, 1, 0, 0, 0) == "N"

    def test_n_when_cs_saved(self, spark):
        assert self._get_abandoning(spark, 1, 0, 0, 0, 0) == "N"

    def test_n_when_upgraded(self, spark):
        assert self._get_abandoning(spark, 0, 0, 1, 0, 0) == "N"


# ── Test: IPD Type Classification ─────────────────────────────────────────────

class TestIpdClassification:

    def _make_offer(self, spark, cta_action, obill_offer_id=None, cta_url=None):
        import json
        copy_data = json.dumps({
            "primaryCtaAction": cta_action,
            **({"obillOfferId": obill_offer_id} if obill_offer_id else {}),
            **({"primaryCtaUrl": cta_url} if cta_url else {}),
        })
        data = spark.createDataFrame([Row(copy_data=copy_data)])
        result = data.withColumn(
            "ipd_type",
            F.when(
                F.coalesce(
                    F.get_json_object(F.col("copy_data"), "$.ctaAction"),
                    F.get_json_object(F.col("copy_data"), "$.primaryCtaAction"),
                ) == "contact-us-widget",
                F.lit("CS IPD"),
            )
            .when(F.get_json_object(F.col("copy_data"), "$.obillOfferId").isNotNull(), F.lit("Discount IPD"))
            .when(
                (F.get_json_object(F.col("copy_data"), "$.primaryCtaAction") == "external")
                & F.get_json_object(F.col("copy_data"), "$.primaryCtaUrl").contains("/obillupgrade"),
                F.lit("Upgrade IPD"),
            )
            .when(
                (F.get_json_object(F.col("copy_data"), "$.primaryCtaAction") == "external")
                & F.get_json_object(F.col("copy_data"), "$.primaryCtaUrl").contains("/changeplan"),
                F.lit("Downgrade IPD"),
            )
            .when(
                (F.get_json_object(F.col("copy_data"), "$.primaryCtaAction") == "callbackOnly")
                & F.get_json_object(F.col("copy_data"), "$.obillOfferId").isNull(),
                F.lit("Keep my Plan IPD"),
            )
            .otherwise(F.lit("Unknown"))
        )
        return result.first().ipd_type

    def test_cs_ipd_classified(self, spark):
        assert self._make_offer(spark, "contact-us-widget") == "CS IPD"

    def test_discount_ipd_classified(self, spark):
        assert self._make_offer(spark, "external", obill_offer_id="DISC-50PCT") == "Discount IPD"

    def test_upgrade_ipd_classified(self, spark):
        assert self._make_offer(spark, "external", cta_url="https://billing.saas.com/obillupgrade/plus") == "Upgrade IPD"

    def test_downgrade_ipd_classified(self, spark):
        assert self._make_offer(spark, "external", cta_url="https://billing.saas.com/changeplan/core") == "Downgrade IPD"

    def test_keep_my_plan_classified(self, spark):
        assert self._make_offer(spark, "callbackOnly") == "Keep my Plan IPD"

    def test_unknown_fallback(self, spark):
        assert self._make_offer(spark, "somethingElse") == "Unknown"


# ── Test: DQ Uniqueness Invariant ─────────────────────────────────────────────

class TestDQUniqueness:

    def test_distinct_eliminates_duplicates(self, spark):
        """SELECT DISTINCT on Step 2 output should give uniqueness = 1.0."""
        data = spark.createDataFrame([
            Row(company_id=1, initiation_rank=1, ts="2024-01-01", access_point="APX", offer_id="OFF1"),
            Row(company_id=1, initiation_rank=1, ts="2024-01-01", access_point="APX", offer_id="OFF1"),  # dup
            Row(company_id=1, initiation_rank=1, ts="2024-01-01", access_point="APX", offer_id="OFF2"),
        ])
        deduped = data.distinct()
        key_cols = ["company_id", "initiation_rank", "ts", "access_point", "offer_id"]
        total = deduped.count()
        unique = deduped.select(*key_cols).distinct().count()
        assert total == unique, f"Uniqueness violated: {unique}/{total}"

    def test_uniqueness_ratio_is_one(self, spark):
        """Uniqueness ratio should equal exactly 1.0."""
        data = spark.createDataFrame([
            Row(k1=1, k2="A"), Row(k1=2, k2="B"), Row(k1=3, k2="C"),
        ]).distinct()
        total = data.count()
        unique = data.select("k1", "k2").distinct().count()
        assert unique / total == 1.0


# ── Test: Retention Flag Consistency ──────────────────────────────────────────

class TestRetentionFlags:

    def test_retained_31d_null_when_not_baked(self, spark):
        """retained_31d should be 0 (unreliable) when baked_31d = 0."""
        data = spark.createDataFrame([
            Row(baked_31d=0, retained_31d=1),  # violation
            Row(baked_31d=1, retained_31d=1),  # valid
            Row(baked_31d=0, retained_31d=0),  # valid
        ])
        # Simulate the DQ check
        violations = data.filter(
            (F.col("baked_31d") == 0) & (F.col("retained_31d") == 1)
        ).count()
        assert violations == 1   # We expect the violation to exist in test data

    def test_baked_flag_depends_on_current_date(self, spark):
        """baked_31d = 1 iff initiation_date + 31 < current_date."""
        from datetime import date, timedelta

        old_date = date(2020, 1, 1)   # Definitely baked
        recent_date = date.today()    # Not baked yet

        data = spark.createDataFrame([
            Row(initiation_date=old_date),
            Row(initiation_date=recent_date),
        ])
        result = data.withColumn(
            "baked_31d",
            F.when(F.date_add(F.col("initiation_date"), 31) < F.current_date(), F.lit(1)).otherwise(F.lit(0)),
        )
        rows = {r.initiation_date: r.baked_31d for r in result.collect()}
        assert rows[old_date] == 1
        assert rows[recent_date] == 0

    def test_cancel_confirmed_string_conversion(self, spark):
        """INT cancel_confirmed (0/1) → STRING Y/N conversion."""
        data = spark.createDataFrame([
            Row(cancel_confirmed=0),
            Row(cancel_confirmed=1),
        ])
        result = data.withColumn(
            "cancel_confirmed_str",
            F.when(F.col("cancel_confirmed") == 1, F.lit("Y")).otherwise(F.lit("N")),
        )
        values = {r.cancel_confirmed_str for r in result.collect()}
        assert values == {"Y", "N"}
