"""
SQL Server data reader for ingesting data into Databricks.
"""
from typing import Optional, Dict, Any
from pyspark.sql import SparkSession, DataFrame
from common.logging import get_logger
from common.exceptions import IngestionException, ConnectionException


logger = get_logger(__name__)


class SQLServerReader:
    """Reads data from SQL Server databases."""

    def __init__(
        self,
        spark: SparkSession,
        jdbc_url: str,
        username: str,
        password: str,
        driver: str = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    ):
        """
        Initialize SQL Server reader.

        Args:
            spark: SparkSession instance
            jdbc_url: JDBC connection URL
            username: Database username
            password: Database password
            driver: JDBC driver class
        """
        self.spark = spark
        self.jdbc_url = jdbc_url
        self.username = username
        self.password = password
        self.driver = driver

    def read_table(
        self,
        table_name: str,
        schema: Optional[str] = "dbo",
        fetch_size: int = 10000,
        num_partitions: int = 4,
        partition_column: Optional[str] = None,
        lower_bound: Optional[int] = None,
        upper_bound: Optional[int] = None
    ) -> DataFrame:
        """
        Read entire table from SQL Server.

        Args:
            table_name: Name of the table
            schema: Database schema name
            fetch_size: Number of rows to fetch per round trip
            num_partitions: Number of Spark partitions
            partition_column: Column to use for partitioning
            lower_bound: Lower bound for partition column
            upper_bound: Upper bound for partition column

        Returns:
            DataFrame with table data
        """
        try:
            full_table_name = f"{schema}.{table_name}"

            options = {
                "url": self.jdbc_url,
                "dbtable": full_table_name,
                "user": self.username,
                "password": self.password,
                "driver": self.driver,
                "fetchsize": str(fetch_size)
            }

            if partition_column and lower_bound is not None and upper_bound is not None:
                options.update({
                    "partitionColumn": partition_column,
                    "lowerBound": str(lower_bound),
                    "upperBound": str(upper_bound),
                    "numPartitions": str(num_partitions)
                })

            logger.info(f"Reading SQL Server table: {full_table_name}")

            df = self.spark.read \
                .format("jdbc") \
                .options(**options) \
                .load()

            row_count = df.count()
            logger.info(f"Successfully read {row_count} rows from {full_table_name}")

            return df

        except Exception as e:
            raise IngestionException(
                f"Failed to read table {table_name}",
                error_code="SQL_SERVER_READ_ERROR",
                table=table_name
            ) from e

    def read_query(
        self,
        query: str,
        fetch_size: int = 10000
    ) -> DataFrame:
        """
        Execute custom SQL query and return results.

        Args:
            query: SQL query to execute
            fetch_size: Number of rows to fetch per round trip

        Returns:
            DataFrame with query results
        """
        try:
            logger.info("Executing custom SQL query")

            df = self.spark.read \
                .format("jdbc") \
                .option("url", self.jdbc_url) \
                .option("query", query) \
                .option("user", self.username) \
                .option("password", self.password) \
                .option("driver", self.driver) \
                .option("fetchsize", str(fetch_size)) \
                .load()

            row_count = df.count()
            logger.info(f"Query executed successfully, returned {row_count} rows")

            return df

        except Exception as e:
            raise IngestionException(
                "Failed to execute SQL query",
                error_code="SQL_QUERY_ERROR"
            ) from e

    def read_incremental(
        self,
        table_name: str,
        watermark_column: str,
        watermark_value: Any,
        schema: str = "dbo"
    ) -> DataFrame:
        """
        Read incremental data based on watermark.

        Args:
            table_name: Name of the table
            watermark_column: Column to use for incremental logic
            watermark_value: Last watermark value
            schema: Database schema name

        Returns:
            DataFrame with incremental data
        """
        query = f"""
        (SELECT * FROM {schema}.{table_name}
         WHERE {watermark_column} > '{watermark_value}') as incremental
        """

        logger.info(
            f"Reading incremental data from {table_name}",
            watermark_column=watermark_column,
            watermark_value=watermark_value
        )

        return self.read_query(query)
