"""
Date utility functions for pipeline processing.
"""
from datetime import date, datetime, timedelta
from typing import Dict
from pyspark.sql.functions import current_date, lit, to_date, date_add
from common.logging import get_logger


logger = get_logger(__name__)


def calculate_card_issue_dates(benefit_year: int, offset_days: int = 30) -> Dict[str, date]:
    """
    Calculate date parameters for card issuance pipeline.

    Args:
        benefit_year: Target benefit year
        offset_days: Number of days to add for eligible as of date

    Returns:
        Dictionary containing current_date, processing_date, and eligible_as_of_date
    """
    current_date = datetime.now().date()
    current_year = current_date.year

    # Adjust processing date if benefit year is in the future
    if benefit_year > current_year:
        processing_date = date(benefit_year, 1, 1)
    else:
        processing_date = current_date

    # Calculate eligible as of date (offset days from processing date)
    eligible_as_of_date = processing_date + timedelta(days=offset_days)

    # Adjust if eligible date crosses into next year - set to last day of processing year
    if eligible_as_of_date.year > processing_date.year:
        eligible_as_of_date = date(eligible_as_of_date.year, 1, 1) - timedelta(days=1)

    logger.info(
        f"Date parameters calculated: processing_date={processing_date}, "
        f"eligible_as_of_date={eligible_as_of_date}, benefit_year={benefit_year}"
    )

    return {
        "current_date": current_date,
        "processing_date": processing_date,
        "eligible_as_of_date": eligible_as_of_date,
        "current_year": current_year
    }


def format_date_for_sql(dt: date) -> str:
    """
    Format date for SQL queries.

    Args:
        dt: Python date object

    Returns:
        Formatted date string (YYYY-MM-DD)
    """
    return dt.strftime("%Y-%m-%d")


def is_date_in_range(check_date: date, start_date: date, end_date: date) -> bool:
    """
    Check if a date falls within a date range (inclusive).

    Args:
        check_date: Date to check
        start_date: Range start date
        end_date: Range end date

    Returns:
        True if date is within range
    """
    return start_date <= check_date <= end_date
