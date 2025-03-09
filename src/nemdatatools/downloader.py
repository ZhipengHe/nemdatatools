"""
Module for downloading data from AEMO.

This module provides functions to download various types of data from
the Australian Energy Market Operator (AEMO).
"""

import logging

import pandas as pd

# from typing import Dict, List, Optional


logger = logging.getLogger(__name__)


def fetch_data(
    data_type: str,
    start_date: str,
    end_date: str,
    regions: list[str] | None = None,
    cache_path: str | None = None,
) -> pd.DataFrame:
    """
    Download data from AEMO for specified parameters.

    Args:
        data_type: Type of data (e.g., 'DISPATCHPRICE')
        start_date: Start date in format YYYY/MM/DD
        end_date: End date in format YYYY/MM/DD
        regions: List of regions to include (optional)
        cache_path: Path to cache downloaded data (optional)

    Returns:
        DataFrame with requested data
    """
    # Skeleton implementation
    logger.info(f"Fetching {data_type} data from {start_date} to {end_date}")
    return pd.DataFrame()  # Empty dataframe as placeholder


def get_available_data_types() -> list[str]:
    """
    Get list of available data types supported by this package.

    Returns:
        List of supported data types
    """
    return ["DISPATCHPRICE", "DISPATCHREGIONSUM", "PREDISPATCH", "P5MIN"]
