"""
Audit logging functionality for tracking data lineage and operations.
"""
from datetime import datetime
from typing import Optional, Dict, Any
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType
from common.logging import get_logger


logger = get_logger(__name__)


class AuditLogger:
    """Logs audit information for data operations."""

    def __init__(self, spark: SparkSession, catalog: str = "main", schema: str = "audit"):
        """
        Initialize audit logger.

        Args:
            spark: SparkSession instance
            catalog: Unity Catalog name
            schema: Schema name for audit tables
        """
        self.spark = spark
        self.catalog = catalog
        self.schema = schema
        self.audit_table = f"{catalog}.{schema}.pipeline_audit"
        self._ensure_audit_table()

    def _ensure_audit_table(self):
        """Create audit table if it doesn't exist."""
        ddl = f"""
        CREATE TABLE IF NOT EXISTS {self.audit_table} (
            audit_id STRING,
            pipeline_name STRING,
            source_system STRING,
            target_table STRING,
            operation STRING,
            start_time TIMESTAMP,
            end_time TIMESTAMP,
            status STRING,
            records_read LONG,
            records_written LONG,
            records_rejected LONG,
            error_message STRING,
            run_id STRING,
            user_name STRING,
            metadata STRING
        )
        USING DELTA
        PARTITIONED BY (DATE(start_time))
        TBLPROPERTIES (
            'delta.enableChangeDataFeed' = 'true',
            'delta.autoOptimize.optimizeWrite' = 'true',
            'delta.autoOptimize.autoCompact' = 'true'
        )
        """
        self.spark.sql(ddl)
        logger.info(f"Audit table ensured: {self.audit_table}")

    def log_pipeline_start(
        self,
        pipeline_name: str,
        source_system: str,
        target_table: str,
        run_id: Optional[str] = None
    ) -> str:
        """
        Log pipeline start.

        Args:
            pipeline_name: Name of the pipeline
            source_system: Source system name
            target_table: Target table name
            run_id: Optional run ID

        Returns:
            Audit ID for this run
        """
        import uuid

        audit_id = str(uuid.uuid4())
        now = datetime.now()

        audit_data = [(
            audit_id,
            pipeline_name,
            source_system,
            target_table,
            "START",
            now,
            None,
            "RUNNING",
            0,
            0,
            0,
            None,
            run_id or audit_id,
            self.spark.sparkContext.sparkUser(),
            None
        )]

        schema = StructType([
            StructField("audit_id", StringType(), False),
            StructField("pipeline_name", StringType(), False),
            StructField("source_system", StringType(), True),
            StructField("target_table", StringType(), True),
            StructField("operation", StringType(), False),
            StructField("start_time", TimestampType(), False),
            StructField("end_time", TimestampType(), True),
            StructField("status", StringType(), False),
            StructField("records_read", IntegerType(), True),
            StructField("records_written", IntegerType(), True),
            StructField("records_rejected", IntegerType(), True),
            StructField("error_message", StringType(), True),
            StructField("run_id", StringType(), True),
            StructField("user_name", StringType(), True),
            StructField("metadata", StringType(), True)
        ])

        df = self.spark.createDataFrame(audit_data, schema)
        df.write.format("delta").mode("append").saveAsTable(self.audit_table)

        logger.info(f"Pipeline started: {pipeline_name}", audit_id=audit_id)
        return audit_id

    def log_pipeline_complete(
        self,
        audit_id: str,
        records_read: int = 0,
        records_written: int = 0,
        records_rejected: int = 0,
        metadata: Optional[Dict[str, Any]] = None
    ):
        """Log successful pipeline completion."""
        import json

        update_sql = f"""
        UPDATE {self.audit_table}
        SET
            end_time = current_timestamp(),
            status = 'SUCCESS',
            records_read = {records_read},
            records_written = {records_written},
            records_rejected = {records_rejected},
            metadata = '{json.dumps(metadata) if metadata else ''}'
        WHERE audit_id = '{audit_id}'
        """

        self.spark.sql(update_sql)
        logger.info(f"Pipeline completed successfully", audit_id=audit_id)

    def log_pipeline_failure(
        self,
        audit_id: str,
        error_message: str,
        records_read: int = 0
    ):
        """Log pipeline failure."""
        update_sql = f"""
        UPDATE {self.audit_table}
        SET
            end_time = current_timestamp(),
            status = 'FAILED',
            records_read = {records_read},
            error_message = '{error_message.replace("'", "''")}'
        WHERE audit_id = '{audit_id}'
        """

        self.spark.sql(update_sql)
        logger.error(f"Pipeline failed", audit_id=audit_id, error=error_message)
