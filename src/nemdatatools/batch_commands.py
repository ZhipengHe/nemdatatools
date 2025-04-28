"""NEMDataTools - Module for batch download commands.

This module provides functions for parallel and batch downloading operations.
"""

import concurrent.futures
import logging
import os

import pandas as pd
from tqdm import tqdm

from nemdatatools.downloader import (
    DEFAULT_DELAY,
    fetch_data,
)

logger = logging.getLogger(__name__)


def download_yearly_data(
    years: list[int],
    tables: list[str],
    cache_path: str = "data/aemo_data",
    max_workers: int = 3,
    delay: int = DEFAULT_DELAY,
    overwrite: bool = False,
) -> dict[int, dict[str, pd.DataFrame]]:
    """Download data for multiple years in parallel using fetch_data().

    Args:
        years: List of years to download
        tables: List of table names to download
        cache_path: Base directory to save downloaded files
        max_workers: Maximum number of parallel workers
        delay: Delay between requests in seconds
        overwrite: Whether to overwrite existing files

    Returns:
        Nested dictionary mapping years to table results (DataFrames)

    """
    results: dict[int, dict[str, pd.DataFrame]] = {}

    # Calculate total tasks for progress bar
    total_tasks = len(years) * len(tables)

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Create futures for each year and table combination
        future_to_key = {}
        for year in years:
            results[year] = {}
            for table in tables:
                # Create year-specific output directory
                year_dir = os.path.join(cache_path, str(year))
                os.makedirs(year_dir, exist_ok=True)

                # Submit download task
                future = executor.submit(
                    fetch_data,
                    data_type=table,
                    start_date=f"{year}/01/01",
                    end_date=f"{year}/12/31",
                    cache_path=year_dir,
                    delay=delay,
                    overwrite=overwrite,
                )
                future_to_key[future] = (year, table)

        # Process results as they complete with progress bar
        with tqdm(total=total_tasks, desc="Overall progress") as pbar:
            for future in concurrent.futures.as_completed(future_to_key):
                year, table = future_to_key[future]
                try:
                    df = future.result()
                    results[year][table] = df
                    logger.info(f"Completed download for {year} - {table}")
                except Exception as e:
                    logger.error(f"Failed to download {year} - {table}: {e}")
                    results[year][table] = None
                pbar.update(1)

    return results


def download_multiple_tables(
    table_names: list[str],
    start_date: str,
    end_date: str,
    regions: list[str] | None = None,
    cache_path: str = "data/aemo_data",
    delay: int = DEFAULT_DELAY,
    overwrite: bool = False,
) -> dict[str, pd.DataFrame]:
    """Download multiple tables of data using fetch_data().

    Args:
        table_names: List of table names to download
        start_date: Start date in format YYYY/MM/DD
        end_date: End date in format YYYY/MM/DD
        regions: List of regions to include (optional)
        cache_path: Directory to save downloaded files
        delay: Delay between requests in seconds
        overwrite: Whether to overwrite existing files

    Returns:
        Dictionary mapping table names to their DataFrames

    """
    results = {}

    for table in table_names:
        try:
            df = fetch_data(
                data_type=table,
                start_date=start_date,
                end_date=end_date,
                regions=regions,
                cache_path=cache_path,
                delay=delay,
                overwrite=overwrite,
            )
            results[table] = df
        except Exception as e:
            logger.error(f"Failed to download {table}: {e}")
            results[table] = None

    return results


def download_parallel_years(
    years: list[int],
    tables: list[str],
    cache_path: str = "data/aemo_data",
    max_workers: int = 3,
    delay: int = DEFAULT_DELAY,
    overwrite: bool = False,
) -> dict[int, dict[str, dict[str, list[str] | pd.DataFrame]]]:
    """Alias for download_yearly_data for backward compatibility."""
    return download_yearly_data(
        years=years,
        tables=tables,
        cache_path=cache_path,
        max_workers=max_workers,
        delay=delay,
        overwrite=overwrite,
    )
