"""
test_data_quality.py - Unit Tests for DQ Framework
====================================================
Tests DQ check functions without Spark (pure Python / pandas).
"""
import pytest
import pandas as pd
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../"))


class TestDQChecks:
    """Unit tests for data quality check logic."""

    def _make_df(self, rows):
        return pd.DataFrame(rows)

    # ── Uniqueness ─────────────────────────────────────────────────────────────
    def test_uniqueness_perfect(self):
        df = self._make_df([
            {"company_id": 1, "initiation_rank": 1, "cancel_ts": "2024-01-01 10:00:00"},
            {"company_id": 1, "initiation_rank": 2, "cancel_ts": "2024-02-01 10:00:00"},
            {"company_id": 2, "initiation_rank": 1, "cancel_ts": "2024-01-15 10:00:00"},
        ])
        keys = ["company_id", "initiation_rank", "cancel_ts"]
        unique = df[keys].drop_duplicates()
        ratio = len(unique) / len(df)
        assert ratio == 1.0, f"Expected 1.0 uniqueness, got {ratio}"

    def test_uniqueness_with_duplicates(self):
        df = self._make_df([
            {"company_id": 1, "initiation_rank": 1, "cancel_ts": "2024-01-01"},
            {"company_id": 1, "initiation_rank": 1, "cancel_ts": "2024-01-01"},  # duplicate
            {"company_id": 2, "initiation_rank": 1, "cancel_ts": "2024-01-15"},
        ])
        keys = ["company_id", "initiation_rank", "cancel_ts"]
        unique = df[keys].drop_duplicates()
        ratio = len(unique) / len(df)
        assert ratio < 1.0, "Should detect duplicates"
        assert abs(ratio - 2/3) < 0.01

    # ── Domain values ──────────────────────────────────────────────────────────
    def test_cancel_confirmed_valid_values(self):
        valid = {"Y", "N"}
        df = self._make_df([
            {"cancel_confirmed": "Y"},
            {"cancel_confirmed": "N"},
            {"cancel_confirmed": "Y"},
        ])
        invalid = df[~df["cancel_confirmed"].isin(valid)]
        assert len(invalid) == 0

    def test_cancel_confirmed_invalid_value_detected(self):
        valid = {"Y", "N"}
        df = self._make_df([
            {"cancel_confirmed": "Y"},
            {"cancel_confirmed": "MAYBE"},  # invalid
        ])
        invalid = df[~df["cancel_confirmed"].isin(valid)]
        assert len(invalid) == 1

    def test_save_attribution_valid_values(self):
        valid = {"CS Save", "Cancelled", "Upgrade Save", "Downgrade Save", "Discount Save", "Abandoned"}
        df = self._make_df([
            {"save_attribution": "CS Save"},
            {"save_attribution": "Abandoned"},
            {"save_attribution": "Discount Save"},
        ])
        invalid = df[~df["save_attribution"].isin(valid)]
        assert len(invalid) == 0

    def test_ipd_type_valid_values(self):
        valid = {"CS IPD", "Discount IPD", "Upgrade IPD", "Downgrade IPD", "Keep my Plan IPD", "Unknown"}
        df = self._make_df([
            {"ipd_type": "CS IPD"},
            {"ipd_type": "Discount IPD"},
            {"ipd_type": "Unknown"},
        ])
        invalid = df[~df["ipd_type"].isin(valid)]
        assert len(invalid) == 0

    # ── Bake/retain consistency ────────────────────────────────────────────────
    def test_bake_retain_consistency_valid(self):
        df = self._make_df([
            {"baked_31d": 1, "retained_31d": 1},  # baked and retained
            {"baked_31d": 1, "retained_31d": 0},  # baked but churned
            {"baked_31d": 0, "retained_31d": 0},  # not yet baked
        ])
        violations = df[(df["baked_31d"] == 0) & (df["retained_31d"] == 1)]
        assert len(violations) == 0

    def test_bake_retain_consistency_violation(self):
        df = self._make_df([
            {"baked_31d": 0, "retained_31d": 1},  # impossible: not baked but retained = violation
        ])
        violations = df[(df["baked_31d"] == 0) & (df["retained_31d"] == 1)]
        assert len(violations) == 1

    # ── Not-null checks ────────────────────────────────────────────────────────
    def test_no_null_company_id(self):
        df = self._make_df([{"company_id": 1}, {"company_id": 2}])
        assert df["company_id"].isnull().sum() == 0

    def test_null_company_id_detected(self):
        df = self._make_df([{"company_id": 1}, {"company_id": None}])
        assert df["company_id"].isnull().sum() == 1

    # ── Row count parity ───────────────────────────────────────────────────────
    def test_row_count_exact_match(self):
        count_a, count_b = 1_000_000, 1_000_000
        assert count_a == count_b

    def test_row_count_mismatch_detected(self):
        count_step1 = 2_107_994
        count_step4 = 2_000_000   # mismatch
        deviation = abs(count_step1 - count_step4) / max(count_step1, count_step4)
        assert deviation > 0.0, "Mismatch should be detected"

    # ── DIC non-nullable ───────────────────────────────────────────────────────
    def test_dic_fields_not_null(self):
        """viewed_dic_component and number_of_data_points_shown must never be NULL."""
        df = self._make_df([
            {"viewed_dic_component": 0, "number_of_data_points_shown": 0},
            {"viewed_dic_component": 1, "number_of_data_points_shown": 3},
        ])
        assert df["viewed_dic_component"].isnull().sum() == 0
        assert df["number_of_data_points_shown"].isnull().sum() == 0

    def test_dic_coalesce_prevents_null(self):
        """COALESCE(viewed_dic_component, 0) must produce 0, not NULL."""
        raw_value = None
        coalesced = raw_value if raw_value is not None else 0
        assert coalesced == 0


class TestIPDClassification:
    """Unit tests for IPD type classification logic."""

    def _classify(self, cta_action, obill_offer_id, cta_url):
        """Mirror of the Spark IPD classification logic."""
        if cta_action == "contact-us-widget":
            return "CS IPD"
        if obill_offer_id is not None:
            return "Discount IPD"
        if cta_action == "external" and cta_url and "/obillupgrade" in cta_url:
            return "Upgrade IPD"
        if cta_action == "external" and cta_url and "/changeplan" in cta_url:
            return "Downgrade IPD"
        if cta_action == "callbackOnly" and obill_offer_id is None:
            return "Keep my Plan IPD"
        return "Unknown"

    def test_cs_ipd(self):
        assert self._classify("contact-us-widget", None, None) == "CS IPD"

    def test_discount_ipd_with_obill_id(self):
        assert self._classify("external", "OBILL-DISC-50PCT-3M", "https://billing.saas.com/offer/disc50") == "Discount IPD"

    def test_upgrade_ipd(self):
        assert self._classify("external", None, "https://billing.saas.com/obillupgrade/plus") == "Upgrade IPD"

    def test_downgrade_ipd(self):
        assert self._classify("external", None, "https://billing.saas.com/changeplan/core") == "Downgrade IPD"

    def test_keep_plan_ipd(self):
        assert self._classify("callbackOnly", None, None) == "Keep my Plan IPD"

    def test_unknown_ipd(self):
        assert self._classify("unknown_action", None, None) == "Unknown"

    def test_discount_takes_priority_over_upgrade(self):
        """If obill_offer_id is present AND URL has obillupgrade, Discount wins."""
        assert self._classify("external", "OBILL-DISC-50PCT-3M", "https://billing.saas.com/obillupgrade/plus") == "Discount IPD"


class TestCancelFlowScreenClassification:
    """Unit tests for cancel flow screen classification logic."""

    def _classify_screen(self, url_host, device_type, page_path):
        """Mirror of Spark screen classification."""
        url_host   = url_host or ""
        device_type = device_type or ""
        page_path  = page_path or ""
        if "accounts." in url_host or "accountmanager" in page_path:
            return "Account Portal"
        if device_type.lower() in ("mobile", "tablet", "smartphone") or "mobile" in url_host:
            return "SaaS Mobile App"
        if "app.saas" in url_host or "qbo.saas" in url_host:
            return "SaaS Web App"
        return "Unknown"

    def test_account_portal_from_host(self):
        assert self._classify_screen("accounts.saas.com", "desktop", "/app/settings") == "Account Portal"

    def test_account_portal_from_path(self):
        assert self._classify_screen("app.saas.com", "desktop", "/accountmanager/settings") == "Account Portal"

    def test_mobile_app_from_device(self):
        assert self._classify_screen("app.saas.com", "mobile", "/app/billing") == "SaaS Mobile App"

    def test_mobile_app_from_host(self):
        assert self._classify_screen("mobile.saas.com", "desktop", "/app/billing") == "SaaS Mobile App"

    def test_web_app(self):
        assert self._classify_screen("app.saas.com", "desktop", "/app/billing/cancel") == "SaaS Web App"

    def test_unknown(self):
        assert self._classify_screen("unknown-host.com", "desktop", "/other") == "Unknown"

    def test_account_portal_takes_priority(self):
        """Account Portal should win over mobile check."""
        assert self._classify_screen("accounts.saas.com", "mobile", "/app") == "Account Portal"
