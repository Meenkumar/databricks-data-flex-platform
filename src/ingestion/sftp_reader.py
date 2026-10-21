"""
SFTP file reader for ingesting files into Databricks.
"""
import paramiko
from typing import Optional, List
from pathlib import Path
from pyspark.sql import SparkSession, DataFrame
from common.logging import get_logger
from common.exceptions import IngestionException, ConnectionException


logger = get_logger(__name__)


class SFTPReader:
    """Reads files from SFTP servers."""

    def __init__(
        self,
        host: str,
        port: int = 22,
        username: str = None,
        password: str = None,
        private_key_path: str = None
    ):
        """
        Initialize SFTP reader.

        Args:
            host: SFTP server hostname
            port: SFTP server port
            username: Username for authentication
            password: Password for authentication
            private_key_path: Path to private key file
        """
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.private_key_path = private_key_path
        self.client = None
        self.sftp = None

    def connect(self):
        """Establish SFTP connection."""
        try:
            self.client = paramiko.SSHClient()
            self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

            if self.private_key_path:
                key = paramiko.RSAKey.from_private_key_file(self.private_key_path)
                self.client.connect(
                    self.host,
                    port=self.port,
                    username=self.username,
                    pkey=key
                )
            else:
                self.client.connect(
                    self.host,
                    port=self.port,
                    username=self.username,
                    password=self.password
                )

            self.sftp = self.client.open_sftp()
            logger.info(f"Connected to SFTP server: {self.host}")

        except Exception as e:
            raise ConnectionException(
                f"Failed to connect to SFTP server: {self.host}",
                error_code="SFTP_CONNECTION_ERROR"
            ) from e

    def disconnect(self):
        """Close SFTP connection."""
        if self.sftp:
            self.sftp.close()
        if self.client:
            self.client.close()
        logger.info("SFTP connection closed")

    def list_files(self, remote_path: str, pattern: str = "*") -> List[str]:
        """
        List files in remote directory.

        Args:
            remote_path: Remote directory path
            pattern: File pattern to match

        Returns:
            List of file paths
        """
        try:
            if not self.sftp:
                self.connect()

            files = self.sftp.listdir(remote_path)

            from fnmatch import fnmatch
            matching_files = [
                f"{remote_path}/{f}" for f in files
                if fnmatch(f, pattern)
            ]

            logger.info(f"Found {len(matching_files)} files matching pattern: {pattern}")
            return matching_files

        except Exception as e:
            raise IngestionException(
                f"Failed to list files in {remote_path}",
                error_code="SFTP_LIST_ERROR"
            ) from e

    def download_file(self, remote_path: str, local_path: str):
        """
        Download file from SFTP server.

        Args:
            remote_path: Remote file path
            local_path: Local destination path
        """
        try:
            if not self.sftp:
                self.connect()

            logger.info(f"Downloading: {remote_path} -> {local_path}")
            self.sftp.get(remote_path, local_path)
            logger.info("Download complete")

        except Exception as e:
            raise IngestionException(
                f"Failed to download file: {remote_path}",
                error_code="SFTP_DOWNLOAD_ERROR"
            ) from e

    def read_csv_to_dataframe(
        self,
        spark: SparkSession,
        remote_path: str,
        local_staging_path: str = "/dbfs/tmp/sftp_staging",
        header: bool = True,
        delimiter: str = ",",
        infer_schema: bool = True
    ) -> DataFrame:
        """
        Download CSV from SFTP and load into DataFrame.

        Args:
            spark: SparkSession instance
            remote_path: Remote CSV file path
            local_staging_path: Local staging area
            header: Whether CSV has header
            delimiter: Field delimiter
            infer_schema: Whether to infer schema

        Returns:
            DataFrame with CSV data
        """
        import os

        os.makedirs(local_staging_path, exist_ok=True)

        filename = Path(remote_path).name
        local_file = f"{local_staging_path}/{filename}"

        self.download_file(remote_path, local_file)

        df = spark.read \
            .option("header", header) \
            .option("delimiter", delimiter) \
            .option("inferSchema", infer_schema) \
            .csv(local_file)

        logger.info(f"Loaded CSV into DataFrame: {filename}")

        # Cleanup staging file
        os.remove(local_file)

        return df

    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.disconnect()
