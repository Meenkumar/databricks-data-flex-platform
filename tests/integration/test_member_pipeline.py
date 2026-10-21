"""
Integration tests for member pipeline.
"""
import pytest
from pyspark.sql import SparkSession
from pipelines.member_pipeline import MemberPipeline


@pytest.fixture(scope="module")
def spark():
    """Create Spark session for testing."""
    return SparkSession.builder \
        .appName("test_member_pipeline") \
        .master("local[2]") \
        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") \
        .getOrCreate()


@pytest.fixture
def pipeline(spark):
    """Create pipeline instance for testing."""
    return MemberPipeline(spark, environment="test")


def test_pipeline_initialization(pipeline):
    """Test pipeline initializes correctly."""
    assert pipeline is not None
    assert pipeline.environment == "test"
    assert pipeline.config is not None


def test_pipeline_config_loading(pipeline):
    """Test configuration loading."""
    assert "source_system" in pipeline.config
    assert "target_table" in pipeline.config


# Add more integration tests as needed


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
