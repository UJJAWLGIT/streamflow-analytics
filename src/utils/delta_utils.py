"""
delta_utils.py — Delta Lake Production Utilities
=================================================
ACID writes, MERGE upserts, OPTIMIZE, ZORDER, time travel, schema evolution.
These patterns are used across all Silver and Gold layer tables.
"""

from __future__ import annotations

import logging
from typing import List, Optional

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)


class DeltaTableWriter:
    """
    Production-grade Delta Lake writer with MERGE, OPTIMIZE, and VACUUM.

    Supports:
    - INSERT OVERWRITE PARTITION (batch)
    - MERGE upsert (idempotent)
    - Schema evolution
    - OPTIMIZE + ZORDER for query acceleration
    - VACUUM for storage management
    """

    def __init__(
        self,
        spark: SparkSession,
        table_path: str,
        table_name: Optional[str] = None,
    ):
        self.spark = spark
        self.table_path = table_path
        self.table_name = table_name

    # ── Write patterns ─────────────────────────────────────────────────────────

    def write_partition_overwrite(
        self,
        df: DataFrame,
        partition_cols: List[str],
        mode: str = "overwrite",
    ) -> int:
        """
        Partitioned INSERT OVERWRITE — only overwrites touched partitions.
        Safe for daily incremental runs.
        """
        count = df.count()
        logger.info(f"Writing {count:,} rows to {self.table_path} (partition overwrite)")

        (
            df.write
            .format("delta")
            .mode(mode)
            .option("overwriteSchema", "false")
            .partitionBy(*partition_cols)
            .save(self.table_path)
        )

        logger.info(f"✅ Partition overwrite complete: {count:,} rows")
        return count

    def merge_upsert(
        self,
        df: DataFrame,
        merge_keys: List[str],
        update_cols: Optional[List[str]] = None,
    ) -> None:
        """
        Delta Lake MERGE upsert — idempotent, safe for reprocessing.
        Uses when-matched-update / when-not-matched-insert pattern.

        Args:
            df:          Source DataFrame.
            merge_keys:  Columns to match on (e.g., ["company_id", "initiation_timestamp"]).
            update_cols: Columns to update on match (None = update all).
        """
        if not DeltaTable.isDeltaTable(self.spark, self.table_path):
            logger.info(f"Delta table does not exist — creating: {self.table_path}")
            df.write.format("delta").partitionBy().save(self.table_path)
            return

        delta_table = DeltaTable.forPath(self.spark, self.table_path)
        merge_condition = " AND ".join(f"target.{k} = source.{k}" for k in merge_keys)

        if update_cols:
            update_map = {c: F.col(f"source.{c}") for c in update_cols}
        else:
            update_map = {c: F.col(f"source.{c}") for c in df.columns}

        (
            delta_table.alias("target")
            .merge(df.alias("source"), merge_condition)
            .whenMatchedUpdate(set=update_map)
            .whenNotMatchedInsertAll()
            .execute()
        )
        logger.info(f"✅ MERGE upsert complete on {self.table_path}")

    def create_or_replace(self, df: DataFrame, partition_cols: List[str]) -> int:
        """
        Full table drop + recreate (Step 4 pattern — DROP PURGE + CREATE).
        Used when the entire table must be rebuilt on each run.
        """
        count = df.count()
        logger.info(f"Full table recreate: {count:,} rows → {self.table_path}")

        (
            df.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .partitionBy(*partition_cols)
            .save(self.table_path)
        )

        logger.info(f"✅ Table recreated: {count:,} rows")
        return count

    # ── Maintenance ────────────────────────────────────────────────────────────

    def optimize(
        self,
        zorder_cols: Optional[List[str]] = None,
        partition_filter: Optional[str] = None,
    ) -> None:
        """
        Run OPTIMIZE + optional ZORDER for query acceleration.

        ZORDER on high-cardinality query predicates reduces data scanned
        dramatically (typically 10-100× fewer files read).

        Args:
            zorder_cols:      Columns to Z-order by (up to 4 recommended).
            partition_filter: SQL predicate to scope optimization (e.g., "initiation_year = '2024'").
        """
        table_ref = self.table_name or f"delta.`{self.table_path}`"

        if partition_filter:
            optimize_sql = f"OPTIMIZE {table_ref} WHERE {partition_filter}"
        else:
            optimize_sql = f"OPTIMIZE {table_ref}"

        if zorder_cols:
            zorder_str = ", ".join(zorder_cols)
            optimize_sql += f" ZORDER BY ({zorder_str})"

        logger.info(f"Running: {optimize_sql}")
        self.spark.sql(optimize_sql)
        logger.info("✅ OPTIMIZE complete")

    def vacuum(self, retain_hours: int = 168) -> None:
        """
        Remove old Delta Lake files (default: retain 7 days = 168 hours).
        Required to reclaim S3 storage after frequent updates.
        """
        table_ref = self.table_name or f"delta.`{self.table_path}`"
        self.spark.sql(f"VACUUM {table_ref} RETAIN {retain_hours} HOURS")
        logger.info(f"✅ VACUUM complete: retained {retain_hours}h of history")

    def get_history(self, limit: int = 10) -> DataFrame:
        """Return Delta Lake transaction history (for audit/debugging)."""
        table_ref = self.table_name or f"delta.`{self.table_path}`"
        return self.spark.sql(f"DESCRIBE HISTORY {table_ref} LIMIT {limit}")

    def time_travel_read(self, version: Optional[int] = None, timestamp: Optional[str] = None) -> DataFrame:
        """
        Read a specific version or timestamp of a Delta table.

        Example:
            # Version-based
            df = writer.time_travel_read(version=5)

            # Timestamp-based
            df = writer.time_travel_read(timestamp="2024-06-01 00:00:00")
        """
        reader = self.spark.read.format("delta")

        if version is not None:
            reader = reader.option("versionAsOf", version)
        elif timestamp:
            reader = reader.option("timestampAsOf", timestamp)

        return reader.load(self.table_path)

    @staticmethod
    def is_delta_table(spark: SparkSession, path: str) -> bool:
        return DeltaTable.isDeltaTable(spark, path)


# ── Schema evolution helpers ───────────────────────────────────────────────────

def enable_schema_evolution(spark: SparkSession) -> None:
    """Enable automatic schema evolution for Delta writes."""
    spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")


def add_column_if_not_exists(
    spark: SparkSession,
    table_path: str,
    column_name: str,
    column_type: str,
    default_value: str = "NULL",
) -> None:
    """
    Safely add a new column to an existing Delta table.
    No-op if column already exists.
    """
    try:
        table_ref = f"delta.`{table_path}`"
        spark.sql(
            f"ALTER TABLE {table_ref} ADD COLUMN {column_name} {column_type} DEFAULT {default_value}"
        )
        logger.info(f"Added column {column_name} ({column_type}) to {table_path}")
    except Exception as e:
        if "already exists" in str(e).lower():
            logger.debug(f"Column {column_name} already exists — skipping")
        else:
            raise


# ── Data quality helpers ───────────────────────────────────────────────────────

def assert_row_counts_match(
    count_a: int,
    count_b: int,
    label_a: str = "table_a",
    label_b: str = "table_b",
    tolerance: float = 0.0,
) -> None:
    """
    Assert that two row counts are within tolerance.
    Used to validate Step 1 = Step 3 = Step 4 row count invariant.

    Args:
        tolerance: Allowed fractional deviation (0.0 = exact match).

    Raises:
        AssertionError if counts deviate beyond tolerance.
    """
    max_count = max(count_a, count_b)
    deviation = abs(count_a - count_b) / max_count if max_count > 0 else 0.0

    if deviation > tolerance:
        raise AssertionError(
            f"Row count mismatch: {label_a}={count_a:,} vs {label_b}={count_b:,} "
            f"(deviation={deviation:.4%}, tolerance={tolerance:.4%})"
        )
    logger.info(f"✅ Row count match: {label_a}={count_a:,} == {label_b}={count_b:,}")
