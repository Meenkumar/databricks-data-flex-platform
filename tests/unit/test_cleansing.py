"""
Unit tests for data cleansing transformations.
"""
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from transformations.cleansing import DataCleansing


@pytest.fixture(scope="module")
def spark():
    """Create Spark session for testing."""
    return SparkSession.builder \
        .appName("test_cleansing") \
        .master("local[2]") \
        .getOrCreate()


def test_trim_strings(spark):
    """Test string trimming."""
    schema = StructType([
        StructField("name", StringType(), True),
        StructField("city", StringType(), True)
    ])

    data = [
        ("  John Doe  ", "  New York  "),
        ("Jane Smith", "Boston   ")
    ]

    df = spark.createDataFrame(data, schema)
    result_df = DataCleansing.trim_strings(df)

    results = result_df.collect()

    assert results[0]["name"] == "John Doe"
    assert results[0]["city"] == "New York"
    assert results[1]["name"] == "Jane Smith"
    assert results[1]["city"] == "Boston"


def test_standardize_phone(spark):
    """Test phone number standardization."""
    schema = StructType([
        StructField("phone", StringType(), True)
    ])

    data = [
        ("1234567890",),
        ("(123) 456-7890",),
        ("123-456-7890",),
        ("invalid",)
    ]

    df = spark.createDataFrame(data, schema)
    result_df = DataCleansing.standardize_phone(df, "phone")

    results = result_df.collect()

    assert results[0]["phone"] == "(123) 456-7890"
    assert results[1]["phone"] == "(123) 456-7890"
    assert results[2]["phone"] == "(123) 456-7890"
    assert results[3]["phone"] == "invalid"  # Should remain unchanged


def test_standardize_ssn(spark):
    """Test SSN standardization."""
    schema = StructType([
        StructField("ssn", StringType(), True)
    ])

    data = [
        ("123456789",),
        ("123-45-6789",),
        ("12345-6789",)
    ]

    df = spark.createDataFrame(data, schema)
    result_df = DataCleansing.standardize_ssn(df, "ssn")

    results = result_df.collect()

    assert results[0]["ssn"] == "123-45-6789"
    assert results[1]["ssn"] == "123-45-6789"
    assert results[2]["ssn"] == "123-45-6789"


def test_deduplicate(spark):
    """Test deduplication logic."""
    schema = StructType([
        StructField("id", StringType(), True),
        StructField("value", StringType(), True),
        StructField("timestamp", StringType(), True)
    ])

    data = [
        ("1", "old", "2024-01-01"),
        ("1", "new", "2024-01-02"),
        ("2", "unique", "2024-01-01")
    ]

    df = spark.createDataFrame(data, schema)
    result_df = DataCleansing.deduplicate(
        df,
        key_columns=["id"],
        order_column="timestamp",
        ascending=False
    )

    results = result_df.collect()

    assert len(results) == 2
    # Should keep the newest record for id=1
    id_1_record = [r for r in results if r["id"] == "1"][0]
    assert id_1_record["value"] == "new"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
