"""
REST API reader for ingesting data from HTTP endpoints.
"""
import requests
from typing import Optional, Dict, Any, List
from pyspark.sql import SparkSession, DataFrame
from common.logging import get_logger
from common.exceptions import IngestionException


logger = get_logger(__name__)


class APIReader:
    """Reads data from REST APIs."""

    def __init__(
        self,
        base_url: str,
        auth_token: Optional[str] = None,
        api_key: Optional[str] = None,
        headers: Optional[Dict[str, str]] = None,
        timeout: int = 30
    ):
        """
        Initialize API reader.

        Args:
            base_url: Base URL for the API
            auth_token: Bearer token for authentication
            api_key: API key for authentication
            headers: Additional headers
            timeout: Request timeout in seconds
        """
        self.base_url = base_url.rstrip('/')
        self.timeout = timeout

        self.headers = headers or {}
        if auth_token:
            self.headers['Authorization'] = f'Bearer {auth_token}'
        if api_key:
            self.headers['X-API-Key'] = api_key

    def get(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Make GET request to API.

        Args:
            endpoint: API endpoint path
            params: Query parameters

        Returns:
            JSON response as dictionary
        """
        url = f"{self.base_url}/{endpoint.lstrip('/')}"

        try:
            logger.info(f"API GET request: {url}")
            response = requests.get(
                url,
                headers=self.headers,
                params=params,
                timeout=self.timeout
            )
            response.raise_for_status()

            logger.info(f"API request successful: {response.status_code}")
            return response.json()

        except requests.exceptions.RequestException as e:
            raise IngestionException(
                f"API request failed: {url}",
                error_code="API_REQUEST_ERROR"
            ) from e

    def post(
        self,
        endpoint: str,
        data: Optional[Dict[str, Any]] = None,
        json_data: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Make POST request to API.

        Args:
            endpoint: API endpoint path
            data: Form data
            json_data: JSON payload

        Returns:
            JSON response as dictionary
        """
        url = f"{self.base_url}/{endpoint.lstrip('/')}"

        try:
            logger.info(f"API POST request: {url}")
            response = requests.post(
                url,
                headers=self.headers,
                data=data,
                json=json_data,
                timeout=self.timeout
            )
            response.raise_for_status()

            logger.info(f"API request successful: {response.status_code}")
            return response.json()

        except requests.exceptions.RequestException as e:
            raise IngestionException(
                f"API POST request failed: {url}",
                error_code="API_POST_ERROR"
            ) from e

    def fetch_paginated(
        self,
        endpoint: str,
        page_param: str = "page",
        page_size_param: str = "page_size",
        page_size: int = 100,
        max_pages: Optional[int] = None,
        data_key: str = "data"
    ) -> List[Dict[str, Any]]:
        """
        Fetch all pages from paginated API.

        Args:
            endpoint: API endpoint path
            page_param: Query parameter for page number
            page_size_param: Query parameter for page size
            page_size: Number of records per page
            max_pages: Maximum pages to fetch (None for all)
            data_key: Key in response containing data array

        Returns:
            List of all records across pages
        """
        all_data = []
        page = 1

        while True:
            params = {
                page_param: page,
                page_size_param: page_size
            }

            response = self.get(endpoint, params=params)

            if data_key in response:
                page_data = response[data_key]
            else:
                page_data = response

            if not page_data:
                break

            all_data.extend(page_data)
            logger.info(f"Fetched page {page}, total records: {len(all_data)}")

            if max_pages and page >= max_pages:
                break

            page += 1

        logger.info(f"Completed pagination: {len(all_data)} total records")
        return all_data

    def to_dataframe(
        self,
        spark: SparkSession,
        data: List[Dict[str, Any]]
    ) -> DataFrame:
        """
        Convert API response data to DataFrame.

        Args:
            spark: SparkSession instance
            data: List of dictionaries from API

        Returns:
            Spark DataFrame
        """
        if not data:
            raise IngestionException(
                "No data to convert to DataFrame",
                error_code="EMPTY_API_RESPONSE"
            )

        df = spark.createDataFrame(data)
        logger.info(f"Created DataFrame with {df.count()} rows")

        return df
