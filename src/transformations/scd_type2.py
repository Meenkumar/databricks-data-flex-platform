"""
Slowly Changing Dimension Type 2 implementation.
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, lit, current_timestamp, to_date, when, md5, concat_ws, coalesce
)
from typing import List
from datetime import datetime
from common.logging import get_logger


logger = get_logger(__name__)


class SCDType2:
    """Implements SCD Type 2 logic for dimension tables."""

    def __init__(
        self,
        spark: SparkSession,
        target_table: str,
        business_keys: List[str],
        compare_columns: List[str],
        effective_date_column: str = "effective_date",
        end_date_column: str = "end_date",
        is_current_column: str = "is_current",
        created_date_column: str = "created_date",
        updated_date_column: str = "updated_date"
    ):
        """
        Initialize SCD Type 2 processor.

        Args:
            spark: SparkSession instance
            target_table: Fully qualified target table name
            business_keys: List of business key columns
            compare_columns: Columns to check for changes
            effective_date_column: Column tracking effective start date
            end_date_column: Column tracking effective end date
            is_current_column: Flag for current record
            created_date_column: Record creation timestamp
            updated_date_column: Record update timestamp
        """
        self.spark = spark
        self.target_table = target_table
        self.business_keys = business_keys
        self.compare_columns = compare_columns
        self.effective_date_column = effective_date_column
        self.end_date_column = end_date_column
        self.is_current_column = is_current_column
        self.created_date_column = created_date_column
        self.updated_date_column = updated_date_column

    def apply_scd2(self, source_df: DataFrame) -> dict:
        """
        Apply SCD Type 2 merge logic.

        Args:
            source_df: Source DataFrame with new/updated records

        Returns:
            Dictionary with merge statistics
        """
        logger.info(f"Starting SCD Type 2 processing for: {self.target_table}")

        # Add hash column for change detection
        source_df = self._add_hash_column(source_df)

        # Add SCD columns to source
        source_df = source_df \
            .withColumn(self.effective_date_column, current_timestamp()) \
            .withColumn(self.end_date_column, lit(None).cast("timestamp")) \
            .withColumn(self.is_current_column, lit(True)) \
            .withColumn(self.created_date_column, current_timestamp()) \
            .withColumn(self.updated_date_column, current_timestamp())

        # Load target table
        try:
            target_df = self.spark.table(self.target_table)
            target_df = self._add_hash_column(target_df)
        except Exception:
            logger.info(f"Target table {self.target_table} does not exist, performing initial load")
            source_df.write.format("delta").mode("overwrite").saveAsTable(self.target_table)
            return {
                "inserts": source_df.count(),
                "updates": 0,
                "no_change": 0
            }

        # Identify changes
        joined_df = source_df.alias("src").join(
            target_df.filter(col(self.is_current_column) == True).alias("tgt"),
            on=self.business_keys,
            how="left"
        )

        # New records (not in target)
        new_records = joined_df.filter(col("tgt._hash").isNull()) \
            .select("src.*")

        # Changed records
        changed_records = joined_df.filter(
            (col("tgt._hash").isNotNull()) &
            (col("src._hash") != col("tgt._hash"))
        ).select("src.*")

        # Unchanged records
        unchanged_count = joined_df.filter(
            (col("tgt._hash").isNotNull()) &
            (col("src._hash") == col("tgt._hash"))
        ).count()

        # Expire old records that have changes
        if changed_records.count() > 0:
            changed_keys = changed_records.select(*self.business_keys)

            expire_condition = " AND ".join([
                f"target.{key} = source.{key}" for key in self.business_keys
            ])

            self.spark.sql(f"""
                MERGE INTO {self.target_table} target
                USING ({changed_keys.createOrReplaceTempView("_changed_keys")},
                       SELECT * FROM _changed_keys) source
                ON {expire_condition} AND target.{self.is_current_column} = true
                WHEN MATCHED THEN UPDATE SET
                    target.{self.end_date_column} = current_timestamp(),
                    target.{self.is_current_column} = false,
                    target.{self.updated_date_column} = current_timestamp()
            """)

        # Insert new and changed records
        records_to_insert = new_records.union(changed_records)

        if records_to_insert.count() > 0:
            records_to_insert.write \
                .format("delta") \
                .mode("append") \
                .saveAsTable(self.target_table)

        stats = {
            "inserts": new_records.count(),
            "updates": changed_records.count(),
            "no_change": unchanged_count
        }

        logger.info(f"SCD Type 2 complete", **stats)
        return stats

    def _add_hash_column(self, df: DataFrame) -> DataFrame:
        """Add hash column for change detection."""
        return df.withColumn(
            "_hash",
            md5(concat_ws("||", *[coalesce(col(c), lit("")) for c in self.compare_columns]))
        )
