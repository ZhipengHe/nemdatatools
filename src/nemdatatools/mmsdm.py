"""MMSDM monthly-archive tier.

The MMSDM Data Archive publishes one consolidated snapshot per month from
2009 onward, lagging the present by roughly six weeks. Files are located
by listing the month's directory and pattern-matching — never by
constructing filenames — which absorbs both filename eras and enumerates
every FILEnn part of multi-part tables.
"""

from __future__ import annotations

import datetime
import logging
import re

import pandas as pd
import requests

from nemdatatools.cache import Cache
from nemdatatools.catalog import ARCHIVE_ERA_START, TableSpec
from nemdatatools.listing import BASE_URL, ListingEntry, list_directory

logger = logging.getLogger(__name__)

MMSDM_ROOT = f"{BASE_URL}/Data_Archive/Wholesale_Electricity/MMSDM"


def months_between(
    start: datetime.datetime,
    end: datetime.datetime,
) -> list[tuple[int, int]]:
    """List ``(year, month)`` pairs covering ``[start, end]`` inclusive."""
    months = []
    year, month = start.year, start.month
    while (year, month) <= (end.year, end.month):
        months.append((year, month))
        year, month = (year, month + 1) if month < 12 else (year + 1, 1)
    return months


def is_archive_era(year: int, month: int) -> bool:
    """Tell whether a month is published in the PUBLIC_ARCHIVE format."""
    return (year, month) >= ARCHIVE_ERA_START


def month_url(year: int, month: int, subdir: str) -> str:
    """URL of one subdirectory of a monthly snapshot."""
    return (
        f"{MMSDM_ROOT}/{year}/MMSDM_{year}_{month:02d}/"
        f"MMSDM_Historical_Data_SQLLoader/{subdir}/"
    )


def match_table_zips(
    entries: list[ListingEntry],
    spec: TableSpec,
    year: int,
    month: int,
) -> list[ListingEntry]:
    """Select the zip parts belonging to ``spec`` from a month listing.

    Args:
        entries: Listing of the snapshot subdirectory.
        spec: Table to locate; ``spec.mmsdm`` must be set.
        year: Snapshot year.
        month: Snapshot month.

    Returns:
        Every matching part (DVD-era split names, or ARCHIVE-era FILEnn
        parts), possibly empty when the table is absent that month.

    """
    if spec.mmsdm is None:
        raise ValueError(f"{spec.name} has no MMSDM location")
    if is_archive_era(year, month):
        if spec.mmsdm.archive_name is None:
            return []
        pattern = re.compile(
            rf"^PUBLIC_ARCHIVE#{re.escape(spec.mmsdm.archive_name)}"
            rf"#(?:ALL#)?FILE\d+#\d{{12}}\.zip$",
        )
    else:
        if not spec.mmsdm.dvd_names:
            return []
        alternatives = "|".join(re.escape(n) for n in spec.mmsdm.dvd_names)
        pattern = re.compile(rf"^PUBLIC_DVD_(?:{alternatives})_\d{{12}}\.zip$")
    return [e for e in entries if pattern.match(e.name)]


def fetch_month(
    spec: TableSpec,
    year: int,
    month: int,
    cache: Cache,
) -> pd.DataFrame:
    """Fetch one table for one snapshot month, all parts concatenated.

    Args:
        spec: Table to fetch; ``spec.mmsdm`` must be set.
        year: Snapshot year.
        month: Snapshot month.
        cache: Download/parse cache.

    Returns:
        All rows of the table for that month; empty when the table has no
        files in the snapshot.

    """
    if spec.mmsdm is None:
        raise ValueError(f"{spec.name} has no MMSDM location")
    entries = list_directory(
        month_url(year, month, spec.mmsdm.subdir),
        session=cache.session,
    )
    zips = match_table_zips(entries, spec, year, month)
    if not zips:
        logger.warning("table %s has no files in MMSDM %d-%02d", spec.name, year, month)
        return pd.DataFrame()
    frames = [cache.load_table(entry.url, spec.cid_key) for entry in zips]
    frames = [f for f in frames if not f.empty]
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def latest_month(session: requests.Session) -> tuple[int, int]:
    """Discover the newest published MMSDM month from live listings."""
    years = [
        int(e.name)
        for e in list_directory(f"{MMSDM_ROOT}/", session=session)
        if e.is_dir and e.name.isdigit()
    ]
    year = max(years)
    months = [
        int(match.group(1))
        for e in list_directory(f"{MMSDM_ROOT}/{year}/", session=session)
        if (match := re.fullmatch(rf"MMSDM_{year}_(\d{{2}})", e.name))
    ]
    if not months:  # A new year directory may exist before its first month.
        year -= 1
        months = [
            int(match.group(1))
            for e in list_directory(f"{MMSDM_ROOT}/{year}/", session=session)
            if (match := re.fullmatch(rf"MMSDM_{year}_(\d{{2}})", e.name))
        ]
    return (year, max(months))
