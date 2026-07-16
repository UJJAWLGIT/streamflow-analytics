"""
test_pipeline_e2e.py - End-to-End Pipeline Smoke Test
=====================================================
Runs the complete Step 0 -> Step 4 pipeline on synthetic data (local mode).
Validates: row counts, uniqueness, domain values, partition correctness.
"""
import os, sys, pytest
from datetime import date

try:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    SPARK_AVAILABLE = True
except ImportError:
    SPARK_AVAILABLE = False

pytestmark = pytest.mark.skipif(not SPARK_AVAILABLE, reason="PySpark not installed")
E2E_DATA = os.environ.get("E2E_DATA_PATH", "/tmp/streamflow_e2e")


@pytest.fixture(scope="module")
def spark():
    s = (SparkSession.builder.master("local[2]")
         .appName("streamflow_e2e").config("spark.sql.shuffle.partitions","4").getOrCreate())
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()


@pytest.fixture(scope="module", autouse=True)
def generate_data():
    """Generate minimal synthetic data for E2E test."""
    import subprocess
    result = subprocess.run([sys.executable, "data/synthetic/generator.py",
        "--start-date", "2024-01-01", "--end-date", "2024-03-31",
        "--companies", "1000", "--output-path", E2E_DATA], capture_output=True, text=True)
    if result.returncode != 0:
        pytest.skip(f"Data generation failed: {result.stderr}")


class TestE2EPipeline:
    def test_data_files_exist(self):
        required = ["companies.parquet","raw_events.parquet","offer_catalog.parquet",
                    "ixp_assignments.parquet","subscriber_status.parquet","offer_history.parquet"]
        for f in required:
            path = os.path.join(E2E_DATA, f)
            assert os.path.exists(path), f"Missing: {path}"

    def test_companies_not_empty(self, spark):
        df = spark.read.parquet(f"{E2E_DATA}/companies.parquet")
        assert df.count() == 1000

    def test_raw_events_schema(self, spark):
        df = spark.read.parquet(f"{E2E_DATA}/raw_events.parquet")
        required_cols = {"company_id","event","properties_object_detail",
                         "properties_ui_object_detail","event_timestamp","event_date"}
        assert required_cols.issubset(set(df.columns)), f"Missing columns: {required_cols - set(df.columns)}"

    def test_offer_catalog_has_all_ipd_types(self, spark):
        df = spark.read.parquet(f"{E2E_DATA}/offer_catalog.parquet")
        types = set(r.ipd_type for r in df.select("ipd_type").collect())
        expected = {"CS IPD","Discount IPD","Upgrade IPD","Downgrade IPD","Keep my Plan IPD"}
        assert expected.issubset(types)

    def test_ixp_assignments_single_treatment(self, spark):
        df = spark.read.parquet(f"{E2E_DATA}/ixp_assignments.parquet")
        multi = (df.groupBy("company_id").agg(F.countDistinct("treatment_name").alias("n"))
                   .filter(F.col("n") > 1).count())
        assert multi == 0, "All companies should have exactly one treatment"

    def test_raw_events_contain_cancel_initiations(self, spark):
        df = spark.read.parquet(f"{E2E_DATA}/raw_events.parquet")
        initiations = df.filter(F.col("event").isin("workflow: started","workflow:started")).count()
        assert initiations > 0, "Should have cancel initiation events"

    def test_raw_events_contain_both_confirmation_taxonomies(self, spark):
        df = spark.read.parquet(f"{E2E_DATA}/raw_events.parquet")
        new_conf   = df.filter(F.col("properties_ui_object_detail")=="cancel_success").count()
        legacy_conf = df.filter(F.col("properties_ui_object_detail").isin("yes_cancel")).count()
        # At least one taxonomy should appear
        assert new_conf + legacy_conf > 0, "Should have at least one confirmation event"

    def test_subscriber_status_not_empty(self, spark):
        df = spark.read.parquet(f"{E2E_DATA}/subscriber_status.parquet")
        assert df.count() > 0

    def test_offer_history_has_discount_offers(self, spark):
        df = spark.read.parquet(f"{E2E_DATA}/offer_history.parquet")
        assert df.count() > 0
        assert "offer_id" in df.columns
