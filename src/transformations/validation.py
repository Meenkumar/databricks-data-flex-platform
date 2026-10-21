"""
Data validation and quality checks.
"""
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, when, isnan, sum as spark_sum
from typing import List, Dict, Any, Optional
from common.logging import get_logger
from common.exceptions import DataQualityException


logger = get_logger(__name__)


class DataValidator:
    """Performs data quality validation checks."""

    def __init__(self, df: DataFrame):
        """
        Initialize validator.

        Args:
            df: DataFrame to validate
        """
        self.df = df
        self.validation_results = []

    def check_not_null(self, columns: List[str], threshold: float = 0.0) -> 'DataValidator':
        """
        Check for null values in columns.

        Args:
            columns: Columns to check
            threshold: Maximum allowed null percentage (0.0 - 1.0)

        Returns:
            Self for chaining
        """
        total_rows = self.df.count()

        for column in columns:
            null_count = self.df.filter(col(column).isNull()).count()
            null_percentage = null_count / total_rows if total_rows > 0 else 0

            passed = null_percentage <= threshold

            result = {
                "check": "not_null",
                "column": column,
                "null_count": null_count,
                "null_percentage": round(null_percentage * 100, 2),
                "threshold_percentage": round(threshold * 100, 2),
                "passed": passed
            }

            self.validation_results.append(result)

            if not passed:
                logger.warning(
                    f"Null check failed for {column}",
                    null_percentage=f"{null_percentage*100:.2f}%"
                )

        return self

    def check_unique(self, columns: List[str]) -> 'DataValidator':
        """
        Check for uniqueness of column combination.

        Args:
            columns: Columns that should be unique together

        Returns:
            Self for chaining
        """
        total_rows = self.df.count()
        distinct_rows = self.df.select(columns).distinct().count()

        duplicate_count = total_rows - distinct_rows
        passed = duplicate_count == 0

        result = {
            "check": "unique",
            "columns": columns,
            "total_rows": total_rows,
            "distinct_rows": distinct_rows,
            "duplicate_count": duplicate_count,
            "passed": passed
        }

        self.validation_results.append(result)

        if not passed:
            logger.warning(
                f"Uniqueness check failed for {columns}",
                duplicates=duplicate_count
            )

        return self

    def check_values_in_list(
        self,
        column: str,
        valid_values: List[Any],
        allow_null: bool = False
    ) -> 'DataValidator':
        """
        Check if column values are in allowed list.

        Args:
            column: Column to check
            valid_values: List of valid values
            allow_null: Whether null is acceptable

        Returns:
            Self for chaining
        """
        if allow_null:
            invalid_count = self.df.filter(
                col(column).isNotNull() &
                ~col(column).isin(valid_values)
            ).count()
        else:
            invalid_count = self.df.filter(
                col(column).isNull() |
                ~col(column).isin(valid_values)
            ).count()

        passed = invalid_count == 0

        result = {
            "check": "values_in_list",
            "column": column,
            "valid_values": valid_values,
            "invalid_count": invalid_count,
            "passed": passed
        }

        self.validation_results.append(result)

        if not passed:
            logger.warning(
                f"Value list check failed for {column}",
                invalid_count=invalid_count
            )

        return self

    def check_range(
        self,
        column: str,
        min_value: Optional[Any] = None,
        max_value: Optional[Any] = None
    ) -> 'DataValidator':
        """
        Check if values are within range.

        Args:
            column: Column to check
            min_value: Minimum allowed value
            max_value: Maximum allowed value

        Returns:
            Self for chaining
        """
        conditions = []

        if min_value is not None:
            conditions.append(col(column) < min_value)
        if max_value is not None:
            conditions.append(col(column) > max_value)

        if conditions:
            from functools import reduce
            from pyspark.sql.functions import or_ as spark_or

            invalid_count = self.df.filter(
                reduce(spark_or, conditions)
            ).count()
        else:
            invalid_count = 0

        passed = invalid_count == 0

        result = {
            "check": "range",
            "column": column,
            "min_value": min_value,
            "max_value": max_value,
            "invalid_count": invalid_count,
            "passed": passed
        }

        self.validation_results.append(result)

        if not passed:
            logger.warning(
                f"Range check failed for {column}",
                invalid_count=invalid_count
            )

        return self

    def check_regex_pattern(
        self,
        column: str,
        pattern: str,
        description: str = ""
    ) -> 'DataValidator':
        """
        Check if values match regex pattern.

        Args:
            column: Column to check
            pattern: Regex pattern
            description: Description of the pattern

        Returns:
            Self for chaining
        """
        invalid_count = self.df.filter(
            col(column).isNotNull() &
            ~col(column).rlike(pattern)
        ).count()

        passed = invalid_count == 0

        result = {
            "check": "regex_pattern",
            "column": column,
            "pattern": pattern,
            "description": description,
            "invalid_count": invalid_count,
            "passed": passed
        }

        self.validation_results.append(result)

        if not passed:
            logger.warning(
                f"Regex pattern check failed for {column}",
                pattern=pattern,
                invalid_count=invalid_count
            )

        return self

    def get_results(self) -> List[Dict[str, Any]]:
        """Get all validation results."""
        return self.validation_results

    def assert_valid(self, raise_on_failure: bool = True) -> bool:
        """
        Assert all validations passed.

        Args:
            raise_on_failure: Whether to raise exception on failure

        Returns:
            True if all checks passed

        Raises:
            DataQualityException: If any check failed and raise_on_failure=True
        """
        failed_checks = [r for r in self.validation_results if not r["passed"]]

        if failed_checks:
            logger.error(f"{len(failed_checks)} validation checks failed")

            if raise_on_failure:
                raise DataQualityException(
                    f"Data validation failed: {len(failed_checks)} checks failed",
                    failed_checks=failed_checks
                )
            return False

        logger.info("All validation checks passed")
        return True
