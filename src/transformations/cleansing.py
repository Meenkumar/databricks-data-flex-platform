"""
Data cleansing and standardization transformations.
"""
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, trim, upper, lower, regexp_replace,
    when, coalesce, to_date, to_timestamp, lit
)
from typing import List, Dict, Optional
from common.logging import get_logger


logger = get_logger(__name__)


class DataCleansing:
    """Data cleansing operations."""

    @staticmethod
    def trim_strings(df: DataFrame, columns: Optional[List[str]] = None) -> DataFrame:
        """
        Trim whitespace from string columns.

        Args:
            df: Input DataFrame
            columns: List of columns to trim (None for all string columns)

        Returns:
            DataFrame with trimmed strings
        """
        if columns is None:
            columns = [field.name for field in df.schema.fields
                      if str(field.dataType) == 'StringType']

        for column in columns:
            df = df.withColumn(column, trim(col(column)))

        logger.info(f"Trimmed {len(columns)} string columns")
        return df

    @staticmethod
    def standardize_case(
        df: DataFrame,
        column: str,
        case_type: str = "upper"
    ) -> DataFrame:
        """
        Standardize case for a column.

        Args:
            df: Input DataFrame
            column: Column name
            case_type: "upper" or "lower"

        Returns:
            DataFrame with standardized case
        """
        if case_type == "upper":
            df = df.withColumn(column, upper(col(column)))
        elif case_type == "lower":
            df = df.withColumn(column, lower(col(column)))

        logger.info(f"Standardized case for column: {column}")
        return df

    @staticmethod
    def remove_special_characters(
        df: DataFrame,
        column: str,
        pattern: str = "[^a-zA-Z0-9\\s]",
        replacement: str = ""
    ) -> DataFrame:
        """
        Remove special characters from column.

        Args:
            df: Input DataFrame
            column: Column name
            pattern: Regex pattern to match
            replacement: Replacement string

        Returns:
            DataFrame with cleaned column
        """
        df = df.withColumn(
            column,
            regexp_replace(col(column), pattern, replacement)
        )

        logger.info(f"Removed special characters from: {column}")
        return df

    @staticmethod
    def standardize_phone(df: DataFrame, column: str) -> DataFrame:
        """
        Standardize phone number format to (XXX) XXX-XXXX.

        Args:
            df: Input DataFrame
            column: Phone column name

        Returns:
            DataFrame with standardized phone numbers
        """
        # Remove all non-numeric characters
        df = df.withColumn(
            column,
            regexp_replace(col(column), "[^0-9]", "")
        )

        # Format as (XXX) XXX-XXXX
        df = df.withColumn(
            column,
            when(
                col(column).isNotNull() & (col(column).rlike("^[0-9]{10}$")),
                regexp_replace(
                    col(column),
                    "([0-9]{3})([0-9]{3})([0-9]{4})",
                    "($1) $2-$3"
                )
            ).otherwise(col(column))
        )

        logger.info(f"Standardized phone numbers in: {column}")
        return df

    @staticmethod
    def standardize_ssn(df: DataFrame, column: str) -> DataFrame:
        """
        Standardize SSN format to XXX-XX-XXXX.

        Args:
            df: Input DataFrame
            column: SSN column name

        Returns:
            DataFrame with standardized SSNs
        """
        # Remove all non-numeric characters
        df = df.withColumn(
            column,
            regexp_replace(col(column), "[^0-9]", "")
        )

        # Format as XXX-XX-XXXX
        df = df.withColumn(
            column,
            when(
                col(column).isNotNull() & (col(column).rlike("^[0-9]{9}$")),
                regexp_replace(
                    col(column),
                    "([0-9]{3})([0-9]{2})([0-9]{4})",
                    "$1-$2-$3"
                )
            ).otherwise(col(column))
        )

        logger.info(f"Standardized SSNs in: {column}")
        return df

    @staticmethod
    def handle_nulls(
        df: DataFrame,
        null_replacements: Dict[str, any]
    ) -> DataFrame:
        """
        Replace null values with defaults.

        Args:
            df: Input DataFrame
            null_replacements: Dict of column: default_value

        Returns:
            DataFrame with nulls replaced
        """
        for column, default_value in null_replacements.items():
            df = df.withColumn(
                column,
                coalesce(col(column), lit(default_value))
            )

        logger.info(f"Handled nulls for {len(null_replacements)} columns")
        return df

    @staticmethod
    def deduplicate(
        df: DataFrame,
        key_columns: List[str],
        order_column: Optional[str] = None,
        ascending: bool = False
    ) -> DataFrame:
        """
        Remove duplicate rows based on key columns.

        Args:
            df: Input DataFrame
            key_columns: Columns to determine duplicates
            order_column: Column to sort by before keeping first
            ascending: Sort order

        Returns:
            Deduplicated DataFrame
        """
        from pyspark.sql.window import Window
        from pyspark.sql.functions import row_number

        initial_count = df.count()

        if order_column:
            window = Window.partitionBy(key_columns).orderBy(
                col(order_column).asc() if ascending else col(order_column).desc()
            )
            df = df.withColumn("_row_num", row_number().over(window))
            df = df.filter(col("_row_num") == 1).drop("_row_num")
        else:
            df = df.dropDuplicates(key_columns)

        final_count = df.count()
        duplicates_removed = initial_count - final_count

        logger.info(
            f"Deduplication complete",
            initial_count=initial_count,
            final_count=final_count,
            duplicates_removed=duplicates_removed
        )

        return df
