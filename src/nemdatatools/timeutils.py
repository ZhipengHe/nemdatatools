"""
NEMDataTools - Time utilities for handling AEMO data timeframes.

This module provides functions for working with time periods, dates,
and forecast horizons relevant to AEMO data.
"""

import datetime
import logging

import pandas as pd

logger = logging.getLogger(__name__)


# Constants - Put these in a separate file in the future

# AEMO data timezone
# Australian Eastern Standard Time offset
AEST_OFFSET = datetime.timezone(datetime.timedelta(hours=10))

# Date format used in AEMO data
AEMO_DATE_FORMAT = "%Y/%m/%d %H:%M:%S"

# Data type specific interval settings
DATA_TYPE_INTERVALS = {
    "DISPATCHPRICE": "5min",
    "DISPATCHREGIONSUM": "5min",
    "PREDISPATCH": "30min",
    "P5MIN": "5min",
}


def parse_date(date_str: str) -> datetime.datetime:
    """
    Parse date string to datetime object.

    Args:
        date_str: Date string in format YYYY/MM/DD or YYYY/MM/DD HH:MM:SS

    Returns:
        Datetime object

    Raises:
        ValueError: If date format is invalid
    """
    # Skeleton implementation
    try:
        if " " in date_str:
            # Has time component
            return datetime.datetime.strptime(date_str, "%Y/%m/%d %H:%M:%S")
        else:
            # Date only
            return datetime.datetime.strptime(date_str, "%Y/%m/%d")
    except ValueError as e:
        raise ValueError(f"Invalid date format: {e}")


def format_date(date: datetime.datetime, include_time: bool = True) -> str:
    """
    Format datetime object to string.

    Args:
        date: Datetime object
        include_time: Whether to include time component

    Returns:
        Formatted date string
    """
    if include_time:
        return date.strftime(AEMO_DATE_FORMAT)
    else:
        return date.strftime("%Y/%m/%d")


def generate_time_periods(
    start_date: datetime.datetime, end_date: datetime.datetime, data_type: str
) -> list[dict[str, str]]:
    """
    Generate list of time periods (days) between start and end dates.

    Args:
        start_date: Start date
        end_date: End date
        data_type: Type of data (to determine appropriate intervals)

    Returns:
        List of dictionaries with year, month, date for each period
    """
    periods = []
    current_date = start_date

    while current_date <= end_date:
        period = {
            "year": current_date.strftime("%Y"),
            "month": current_date.strftime("%m"),
            "date": current_date.strftime("%Y%m%d"),
        }
        periods.append(period)
        current_date += datetime.timedelta(days=1)

    return periods


def generate_intervals(
    start_date: datetime.datetime, end_date: datetime.datetime, interval: str = "5min"
) -> pd.DatetimeIndex:
    """
    Generate time intervals between start and end dates.

    Args:
        start_date: Start date
        end_date: End date
        interval: Interval frequency (default: 5min)

    Returns:
        DatetimeIndex with intervals
    """
    # Ensure correct timezone
    start_with_tz = start_date.replace(tzinfo=AEST_OFFSET)
    end_with_tz = end_date.replace(tzinfo=AEST_OFFSET)

    # Generate time range with specified interval
    return pd.date_range(start=start_with_tz, end=end_with_tz, freq=interval)


def get_forecast_horizon(
    run_time: datetime.datetime, target_time: datetime.datetime
) -> datetime.timedelta:
    """
    Calculate forecast horizon between run time and target time.

    Args:
        run_time: Time when forecast was made
        target_time: Time being forecasted

    Returns:
        Timedelta representing forecast horizon

    Raises:
        ValueError: If target_time is before run_time
    """
    if target_time < run_time:
        raise ValueError("Target time must be after run time")

    return target_time - run_time


def get_data_type_interval(data_type: str) -> str:
    """
    Get the appropriate interval for a data type.

    Args:
        data_type: Type of data

    Returns:
        Interval frequency string

    Raises:
        ValueError: If data_type is not supported
    """
    if data_type not in DATA_TYPE_INTERVALS:
        supported_types = ", ".join(DATA_TYPE_INTERVALS.keys())
        raise ValueError(
            f"Unsupported data type: {data_type}. Supported types: {supported_types}"
        )

    return DATA_TYPE_INTERVALS[data_type]


def is_dispatch_interval(dt: datetime.datetime) -> bool:
    """
    Check if datetime is on a dispatch interval boundary.

    Dispatch intervals are every 5 minutes.

    Args:
        dt: Datetime to check

    Returns:
        True if dt is on a dispatch interval, False otherwise
    """
    return dt.minute % 5 == 0 and dt.second == 0


def get_next_interval(
    dt: datetime.datetime, interval: str = "5min"
) -> datetime.datetime:
    """
    Get next interval time after given datetime.

    Args:
        dt: Reference datetime
        interval: Interval frequency (must be "5min" or "30min")

    Returns:
        Next interval datetime

    Raises:
        ValueError: If interval is not "5min" or "30min"
    """
    if interval == "5min":
        # Find next 5-minute interval
        minutes = dt.minute
        next_5min = ((minutes // 5) + 1) * 5

        if next_5min == 60:
            # Next hour
            return dt.replace(minute=0, second=0, microsecond=0) + datetime.timedelta(
                hours=1
            )
        else:
            # Next 5-minute interval within current hour
            return dt.replace(minute=next_5min, second=0, microsecond=0)

    elif interval == "30min":
        # Find next 30-minute interval
        if dt.minute < 30:
            # Next half hour
            return dt.replace(minute=30, second=0, microsecond=0)
        else:
            # Next hour
            return dt.replace(minute=0, second=0, microsecond=0) + datetime.timedelta(
                hours=1
            )

    else:
        raise ValueError("Interval must be either '5min' or '30min'")


def get_interval_boundaries(
    start_date: datetime.datetime, end_date: datetime.datetime, interval: str = "5min"
) -> tuple[datetime.datetime, datetime.datetime]:
    """
    Adjust start and end dates to interval boundaries.

    Args:
        start_date: Start date
        end_date: End date
        interval: Interval frequency

    Returns:
        Tuple of adjusted start and end dates

    Raises:
        ValueError: If interval is not "5min" or "30min"
    """
    if interval == "5min":
        # Adjust start date to nearest interval boundary
        minute_remainder = start_date.minute % 5
        if minute_remainder > 0:
            # Round up to next 5-minute interval
            start_adj = start_date + datetime.timedelta(minutes=5 - minute_remainder)
            start_adj = start_adj.replace(second=0, microsecond=0)
        else:
            # Already on a 5-minute boundary
            start_adj = start_date.replace(second=0, microsecond=0)

        # Adjust end date to nearest interval boundary
        minute_remainder = end_date.minute % 5
        if minute_remainder > 0:
            # Round down to previous 5-minute interval
            end_adj = end_date - datetime.timedelta(minutes=minute_remainder)
            end_adj = end_adj.replace(second=0, microsecond=0)
        else:
            # Already on a 5-minute boundary
            end_adj = end_date.replace(second=0, microsecond=0)

    elif interval == "30min":
        # Adjust start date to nearest interval boundary
        if start_date.minute == 0:
            # Already on hour boundary
            start_adj = start_date.replace(second=0, microsecond=0)
        elif start_date.minute <= 30:
            # Round up to nearest half hour
            start_adj = start_date.replace(minute=30, second=0, microsecond=0)
        else:
            # Round up to next hour
            start_adj = (
                start_date.replace(second=0, microsecond=0)
                + datetime.timedelta(hours=1)
            ).replace(minute=0)

        # Adjust end date to nearest interval boundary
        if end_date.minute >= 30:
            # Round down to nearest half hour
            end_adj = end_date.replace(minute=30, second=0, microsecond=0)
        else:
            # Round down to previous hour
            end_adj = end_date.replace(minute=0, second=0, microsecond=0)

    else:
        raise ValueError("Interval must be either '5min' or '30min'")

    return (start_adj, end_adj)
