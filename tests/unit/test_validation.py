"""
Unit tests for data validation.
"""
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from transformations.validation import DataValidator
from common.exceptions import DataQualityException


@pytest.fixture(scope="module")
def spark():
    """Create Spark session for testing."""
    return SparkSession.builder \
        .appName("test_validation") \
        .master("local[2]") \
        .getOrCreate()


def test_check_not_null_success(spark):
    """Test null check with valid data."""
    schema = StructType([
        StructField("id", StringType(), True),
        StructField("name", StringType(), True)
    ])

    data = [
        ("1", "John"),
        ("2", "Jane")
    ]

    df = spark.createDataFrame(data, schema)
    validator = DataValidator(df)

    validator.check_not_null(["id", "name"], threshold=0.0)

    assert validator.assert_valid(raise_on_failure=False) is True


def test_check_not_null_failure(spark):
    """Test null check with invalid data."""
    schema = StructType([
        StructField("id", StringType(), True),
        StructField("name", StringType(), True)
    ])

    data = [
        ("1", "John"),
        ("2", None)
    ]

    df = spark.createDataFrame(data, schema)
    validator = DataValidator(df)

    validator.check_not_null(["name"], threshold=0.0)

    with pytest.raises(DataQualityException):
        validator.assert_valid(raise_on_failure=True)


def test_check_unique(spark):
    """Test uniqueness check."""
    schema = StructType([
        StructField("id", StringType(), True)
    ])

    data = [("1",), ("2",), ("3",)]

    df = spark.createDataFrame(data, schema)
    validator = DataValidator(df)

    validator.check_unique(["id"])

    assert validator.assert_valid(raise_on_failure=False) is True


def test_check_values_in_list(spark):
    """Test value list validation."""
    schema = StructType([
        StructField("status", StringType(), True)
    ])

    data = [("ACTIVE",), ("INACTIVE",), ("PENDING",)]

    df = spark.createDataFrame(data, schema)
    validator = DataValidator(df)

    validator.check_values_in_list(
        "status",
        ["ACTIVE", "INACTIVE", "PENDING"]
    )

    assert validator.assert_valid(raise_on_failure=False) is True


def test_check_range(spark):
    """Test range validation."""
    schema = StructType([
        StructField("age", IntegerType(), True)
    ])

    data = [(25,), (30,), (45,)]

    df = spark.createDataFrame(data, schema)
    validator = DataValidator(df)

    validator.check_range("age", min_value=0, max_value=120)

    assert validator.assert_valid(raise_on_failure=False) is True


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
