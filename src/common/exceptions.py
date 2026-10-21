"""
Custom exceptions for the Databricks data platform.
"""


class DataPlatformException(Exception):
    """Base exception for all data platform errors."""

    def __init__(self, message: str, error_code: str = None, **kwargs):
        self.message = message
        self.error_code = error_code
        self.context = kwargs
        super().__init__(self.message)


class IngestionException(DataPlatformException):
    """Exception raised during data ingestion."""
    pass


class TransformationException(DataPlatformException):
    """Exception raised during data transformation."""
    pass


class ValidationException(DataPlatformException):
    """Exception raised during data validation."""
    pass


class ConfigurationException(DataPlatformException):
    """Exception raised for configuration errors."""
    pass


class SchemaException(DataPlatformException):
    """Exception raised for schema-related errors."""
    pass


class ConnectionException(DataPlatformException):
    """Exception raised for connection failures."""
    pass


class DataQualityException(DataPlatformException):
    """Exception raised for data quality check failures."""

    def __init__(self, message: str, failed_checks: list = None, **kwargs):
        super().__init__(message, **kwargs)
        self.failed_checks = failed_checks or []
