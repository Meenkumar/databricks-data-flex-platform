"""
Centralized logging configuration for Databricks pipelines.
"""
import logging
import sys
from datetime import datetime
from typing import Optional


class DatabricksLogger:
    """Custom logger for Databricks with structured logging support."""

    def __init__(self, name: str, log_level: str = "INFO"):
        """
        Initialize logger with Databricks-specific configuration.

        Args:
            name: Logger name (typically module or pipeline name)
            log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        """
        self.logger = logging.getLogger(name)
        self.logger.setLevel(getattr(logging, log_level.upper()))

        # Avoid duplicate handlers
        if not self.logger.handlers:
            handler = logging.StreamHandler(sys.stdout)
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            )
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)

    def info(self, message: str, **kwargs):
        """Log info message with optional structured data."""
        self._log_with_context("info", message, **kwargs)

    def warning(self, message: str, **kwargs):
        """Log warning message with optional structured data."""
        self._log_with_context("warning", message, **kwargs)

    def error(self, message: str, **kwargs):
        """Log error message with optional structured data."""
        self._log_with_context("error", message, **kwargs)

    def debug(self, message: str, **kwargs):
        """Log debug message with optional structured data."""
        self._log_with_context("debug", message, **kwargs)

    def _log_with_context(self, level: str, message: str, **kwargs):
        """Internal method to log with additional context."""
        log_func = getattr(self.logger, level)

        if kwargs:
            context_str = " | ".join([f"{k}={v}" for k, v in kwargs.items()])
            message = f"{message} | {context_str}"

        log_func(message)


def get_logger(name: str, log_level: str = "INFO") -> DatabricksLogger:
    """
    Factory function to create a logger instance.

    Args:
        name: Logger name
        log_level: Logging level

    Returns:
        DatabricksLogger instance
    """
    return DatabricksLogger(name, log_level)
