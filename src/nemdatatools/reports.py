"""Reports CURRENT and ARCHIVE tiers.

Reports CURRENT holds individual event files with a per-package rolling
retention of days to weeks; Reports ARCHIVE repackages the same files as
daily zip-of-zips bundles kept for roughly thirteen months. File names
embed a publish timestamp; rows are always re-filtered by settlement time
afterwards, so timestamp matching only needs to be generous, not exact.
"""

from __future__ import annotations

import datetime
import logging
import re

import pandas as pd
import requests

from nemdatatools.cache import Cache
from nemdatatools.catalog import TableSpec
from nemdatatools.listing import BASE_URL, list_directory

logger = logging.getLogger(__name__)

# Files are stamped with publish time, which can trail the settlement
# intervals they carry (daily reports publish the following day).
_STAMP_SLACK = datetime.timedelta(days=1)

_TIMESTAMP = re.compile(r"_(\d{8,14})(?=[_.])")


def _parse_stamp(digits: str) -> datetime.datetime | None:
    """Parse one embedded date/datetime stamp."""
    formats = {8: "%Y%m%d", 12: "%Y%m%d%H%M", 14: "%Y%m%d%H%M%S"}
    fmt = formats.get(len(digits))
    if fmt is None:
        return None
    try:
        return datetime.datetime.strptime(digits, fmt)
    except ValueError:
        return None


def _stamp_range(
    name: str,
) -> tuple[datetime.datetime, datetime.datetime] | None:
    """Extract the coverage range stamped in a payload filename.

    CURRENT payloads carry one stamp; some ARCHIVE packages bundle weekly
    with two (``PUBLIC_TRADINGIS_20260621_20260627.zip``), where the pair
    bounds the days the bundle covers.
    """
    stamps = [
        parsed
        for digits in _TIMESTAMP.findall(name)
        if (parsed := _parse_stamp(digits)) is not None
    ]
    if not stamps:
        return None
    return (min(stamps), max(stamps))


def _fetch_window(
    spec: TableSpec,
    base: str,
    package: str,
    start: datetime.datetime,
    end: datetime.datetime,
    cache: Cache,
) -> pd.DataFrame:
    """Fetch every payload of a package stamped inside the window."""
    if spec.report is None:
        raise ValueError(f"{spec.name} has no Reports location")
    url = f"{base}/{package}/"
    entries = list_directory(url, session=cache.session)
    frames: list[pd.DataFrame] = []
    for entry in entries:
        if entry.is_dir or not entry.name.startswith(spec.report.file_prefix):
            continue
        if not entry.name.lower().endswith(".zip"):
            continue
        stamps = _stamp_range(entry.name)
        if stamps is None:
            logger.debug("skipping unstamped file %s", entry.name)
            continue
        covers_from, covers_to = stamps
        if covers_to + _STAMP_SLACK < start or covers_from - _STAMP_SLACK > end:
            continue
        try:
            frame = cache.load_table(entry.url, spec.cid_key)
        except requests.RequestException as exc:
            # Files at the edge of the rolling retention window can vanish
            # between listing and download, and single payloads can hit
            # transient connection failures; skip rather than fail the
            # whole request (retention-edge rows come from ARCHIVE anyway).
            logger.warning("skipping %s: %s", entry.name, exc)
            continue
        if not frame.empty:
            frames.append(frame)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def fetch_current(
    spec: TableSpec,
    start: datetime.datetime,
    end: datetime.datetime,
    cache: Cache,
) -> pd.DataFrame:
    """Fetch a table from Reports CURRENT for a time window.

    Args:
        spec: Table to fetch; ``spec.report`` must be set.
        start: Window start (naive NEM time).
        end: Window end (naive NEM time).
        cache: Download/parse cache.

    Returns:
        Rows from every matching payload; row-level time filtering is the
        router's responsibility.

    """
    if spec.report is None:
        raise ValueError(f"{spec.name} has no Reports location")
    return _fetch_window(
        spec,
        f"{BASE_URL}/Reports/Current",
        spec.report.package,
        start,
        end,
        cache,
    )


def latest_archive_stamp(
    spec: TableSpec,
    cache: Cache,
) -> datetime.datetime | None:
    """Return the newest daily-bundle date stamp in the ARCHIVE package.

    Used by the router to avoid re-downloading days from CURRENT that the
    ARCHIVE tier already covers.

    Args:
        spec: Table whose archive package to inspect.
        cache: Provides the HTTP session.

    Returns:
        The newest bundle stamp, or None when the package is empty or the
        table has no archive location.

    """
    if spec.report is None or spec.report.archive_package is None:
        return None
    url = f"{BASE_URL}/Reports/Archive/{spec.report.archive_package}/"
    coverage_ends = [
        stamps[1]
        for entry in list_directory(url, session=cache.session)
        if entry.name.startswith(spec.report.file_prefix)
        and (stamps := _stamp_range(entry.name)) is not None
    ]
    return max(coverage_ends, default=None)


def fetch_archive(
    spec: TableSpec,
    start: datetime.datetime,
    end: datetime.datetime,
    cache: Cache,
) -> pd.DataFrame:
    """Fetch a table from Reports ARCHIVE daily bundles for a time window.

    Args:
        spec: Table to fetch; ``spec.report.archive_package`` must be set.
        start: Window start (naive NEM time).
        end: Window end (naive NEM time).
        cache: Download/parse cache.

    Returns:
        Rows from every bundle whose date stamp intersects the window.

    """
    if spec.report is None or spec.report.archive_package is None:
        raise ValueError(f"{spec.name} has no Reports ARCHIVE location")
    return _fetch_window(
        spec,
        f"{BASE_URL}/Reports/Archive",
        spec.report.archive_package,
        start,
        end,
        cache,
    )
