"""Common utilities and shared code."""

from .logging import get_logger, DatabricksLogger
from .exceptions import (
    DataPlatformException,
    IngestionException,
    TransformationException,
    ValidationException,
    ConfigurationException,
    SchemaException,
    ConnectionException,
    DataQualityException,
)
from .config_loader import ConfigLoader
from .audit import AuditLogger
from .date_utils import calculate_card_issue_dates, format_date_for_sql, is_date_in_range
from .plaid_verification import (
    get_latest_plaid_member_status,
    get_plaid_verified_members,
    check_plaid_verification_status
)
from .card_issue_functions import (
    get_client_benefit_rules,
    get_eligible_members_for_card_issue,
    apply_card_issue_business_rules,
    apply_load_threshold,
    insert_benefit_card_mapping,
    insert_card_benefit_load_ci
)
from .functions import read_delta_table

__all__ = [
    # Logging
    "get_logger",
    "DatabricksLogger",
    # Exceptions
    "DataPlatformException",
    "IngestionException",
    "TransformationException",
    "ValidationException",
    "ConfigurationException",
    "SchemaException",
    "ConnectionException",
    "DataQualityException",
    # Configuration & Audit
    "ConfigLoader",
    "AuditLogger",
    # Date utilities
    "calculate_card_issue_dates",
    "format_date_for_sql",
    "is_date_in_range",
    # Plaid verification
    "get_latest_plaid_member_status",
    "get_plaid_verified_members",
    "check_plaid_verification_status",
    # Card issue functions
    "get_client_benefit_rules",
    "get_eligible_members_for_card_issue",
    "apply_card_issue_business_rules",
    "apply_load_threshold",
    "insert_benefit_card_mapping",
    "insert_card_benefit_load_ci",
    # Generic functions
    "read_delta_table",
]
