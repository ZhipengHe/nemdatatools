"""NEMDataTools - Module for batch download commands.

This module provides functions for parallel and batch downloading operations.
"""

import concurrent.futures
import logging

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
    max_workers: int = 1,
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
    months = [f"{m:02d}" for m in range(1, 13)]  # ['01', '02', ..., '12']

    # Create mapping of days in each month (accounting for leap years)
    def get_days_in_month(year: int, month: str) -> int:
        """Return the number of days in a month, accounting for leap years."""
        if month not in months:
            raise ValueError(f"Invalid month: {month}. Must be '01' to '12'.")
        if month == "02":
            if (year % 4 == 0 and year % 100 != 0) or (year % 400 == 0):
                return 29
            else:
                return 28
        elif month in ["04", "06", "09", "11"]:
            return 30
        else:
            return 31

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Create futures for each year, month and table combination
        future_to_key = {}
        # Store monthly DataFrames before concatenation
        monthly_results: dict[int, dict[str, list[pd.DataFrame]]] = {}

        for year in years:
            results[year] = {}
            monthly_results[year] = {table: [] for table in tables}

            # Create progress bar for each year's downloads
            total_tasks = len(tables) * len(months)
            with tqdm(total=total_tasks, desc=f"Year {year}") as year_pbar:
                for table in tables:
                    for month in months:
                        # Calculate proper end date based on actual days in month
                        last_day = get_days_in_month(year, month)
                        start_date = f"{year}/{month}/01"
                        end_date = f"{year}/{month}/{last_day}"

                        future = executor.submit(
                            fetch_data,
                            data_type=table,
                            start_date=start_date,
                            end_date=end_date,
                            cache_path=cache_path,
                            delay=delay,
                            overwrite=overwrite,
                        )
                        future_to_key[future] = (year, month, table)

                # Process monthly results as they complete
                for future in concurrent.futures.as_completed(future_to_key):
                    year, month, table = future_to_key.pop(future)
                    try:
                        df = future.result()
                        if df is not None and not df.empty:
                            monthly_results[year][table].append(df)
                            logger.info(
                                f"Completed download for {year}-{month} - {table}",
                            )
                        else:
                            logger.warning(f"Empty data for {year}-{month} - {table}")
                    except Exception as e:
                        error_details = str(e)
                        # Try to get more detailed error info if available
                        if hasattr(e, "response"):
                            try:
                                error_details += (
                                    f"\nStatus Code: {e.response.status_code}"
                                )
                                error_details += f"\nResponse: {e.response.text[:200]}"
                            except Exception as inner_e:
                                error_details += (
                                    f"\nFailed to retrieve error details: {inner_e}"
                                )
                        logger.error(
                            f"Failed to download {year}-{month} - {table}: "
                            f"{error_details}",
                        )

                    year_pbar.update(1)

                # Concatenate monthly results for each table
                for table in tables:
                    if monthly_results[year][table]:
                        try:
                            results[year][table] = pd.concat(
                                monthly_results[year][table],
                                ignore_index=True,
                            )
                            logger.info(
                                f"Successfully concatenated "
                                f"{len(monthly_results[year][table])} months for "
                                f"{year} - {table}",
                            )
                        except Exception as e:
                            logger.error(
                                f"Failed to concatenate data for {year} - {table}: "
                                f"{e!s}",
                            )
                    else:
                        logger.warning(
                            f"No data available to concatenate for {year} - {table}",
                        )

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

    # progress bar for download completion
    total_tasks = len(table_names)
    with tqdm(total=total_tasks, desc="Downloading tables") as pbar:
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
            pbar.update(1)

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
