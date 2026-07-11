"""Date-range routing across AEMO's publication tiers.

A request for a table over ``[start, end]`` is served by stitching tiers by
age: MMSDM monthly snapshots for everything they already cover, then
Reports ARCHIVE daily bundles, then Reports CURRENT for the freshest days.
Tier overlap is resolved by row-identity de-duplication rather than by
trusting tier boundaries to be exact.
"""

from __future__ import annotations

import datetime
import logging

import pandas as pd

from nemdatatools import mmsdm, reports
from nemdatatools.cache import Cache
from nemdatatools.catalog import TableSpec, get_table
from nemdatatools.errors import AvailabilityGapError, CoverageError
from nemdatatools.timeutils import AEMO_DATETIME_FORMAT, parse_date

logger = logging.getLogger(__name__)


def fetch(
    table: str,
    start: str | datetime.datetime,
    end: str | datetime.datetime,
    regions: list[str] | None = None,
    cache: Cache | None = None,
) -> pd.DataFrame:
    """Fetch one table over a date range, stitching tiers as needed.

    Args:
        table: Curated table name (see :func:`nemdatatools.tables`).
        start: Range start, inclusive, naive NEM time.
        end: Range end, inclusive, naive NEM time.
        regions: Optional NEM region filter, for tables with a region
            column.
        cache: Download/parse cache; a default one under
            ``~/.nemdatatools`` is used when omitted.

    Returns:
        Rows sorted by the table's time column, which becomes the index.
        De-duplicated on the table's identity columns where tiers overlap.

    Raises:
        AvailabilityGapError: If the range crosses a known hole in the
            table's history (the message names the substitute table).
        CoverageError: If part of the range cannot be served by any tier.
        KeyError: If the table is not in the curated catalog.

    """
    spec = get_table(table)
    start_dt, end_dt = parse_date(start), parse_date(end)
    if end_dt < start_dt:
        raise ValueError(f"end {end!r} is before start {start!r}")
    _check_gaps(spec, start_dt, end_dt)

    frames: list[pd.DataFrame] = []
    cache = cache or Cache()
    mmsdm_end: datetime.datetime | None = None

    if spec.mmsdm is not None:
        year, month = mmsdm.latest_month(cache.session)
        mmsdm_end = _end_of_month(year, month)
        for y, m in mmsdm.months_between(start_dt, min(end_dt, mmsdm_end)):
            frames.append(mmsdm.fetch_month(spec, y, m, cache))

    needs_reports = mmsdm_end is None or end_dt > mmsdm_end
    if needs_reports:
        rem_start = start_dt
        if mmsdm_end is not None:
            rem_start = max(start_dt, mmsdm_end + datetime.timedelta(seconds=1))
        if spec.report is None:
            raise CoverageError(
                f"{spec.name}: the range after "
                f"{mmsdm_end:%Y-%m-%d} is not yet in the MMSDM archive and "
                "the table has no Reports package; retry once AEMO "
                "publishes the next monthly snapshot.",
            )
        if spec.report.archive_package is not None:
            frames.append(reports.fetch_archive(spec, rem_start, end_dt, cache))
        frames.append(reports.fetch_current(spec, rem_start, end_dt, cache))

    frames = [f for f in frames if not f.empty]
    if not frames:
        return pd.DataFrame()
    data = pd.concat(frames, ignore_index=True)
    return _finalise(spec, data, start_dt, end_dt, regions)


def _check_gaps(
    spec: TableSpec,
    start: datetime.datetime,
    end: datetime.datetime,
) -> None:
    """Raise when the requested range crosses a known availability gap."""
    for gap in spec.gaps:
        gap_start = datetime.datetime(*gap.start, 1)
        gap_end = (
            _end_of_month(*gap.end) if gap.end is not None else datetime.datetime.max
        )
        if start <= gap_end and end >= gap_start:
            bounds = (
                f"{gap.start[0]}-{gap.start[1]:02d} onward"
                if gap.end is None
                else f"{gap.start[0]}-{gap.start[1]:02d} to "
                f"{gap.end[0]}-{gap.end[1]:02d}"
            )
            raise AvailabilityGapError(
                f"{spec.name} is unavailable {bounds}: {gap.reason} "
                "Re-request around the gap for the covered part.",
            )


def _end_of_month(year: int, month: int) -> datetime.datetime:
    """Last representable instant of a month."""
    if month == 12:
        year, month = year + 1, 1
    else:
        month += 1
    return datetime.datetime(year, month, 1) - datetime.timedelta(seconds=1)


def _finalise(
    spec: TableSpec,
    data: pd.DataFrame,
    start: datetime.datetime,
    end: datetime.datetime,
    regions: list[str] | None,
) -> pd.DataFrame:
    """Filter, de-duplicate, sort, and index the stitched rows."""
    if spec.time_column not in data.columns:
        raise CoverageError(
            f"{spec.name}: fetched payloads lack the expected time column "
            f"{spec.time_column!r}; AEMO may have changed the schema.",
        )
    data[spec.time_column] = pd.to_datetime(
        data[spec.time_column],
        format=AEMO_DATETIME_FORMAT,
    )
    mask = (data[spec.time_column] >= start) & (data[spec.time_column] <= end)
    data = data.loc[mask]
    if regions is not None:
        if spec.region_column is None:
            raise ValueError(f"{spec.name} has no region column to filter on")
        data = data.loc[data[spec.region_column].isin(regions)]
    identity = [spec.time_column, *[c for c in spec.key_columns if c in data.columns]]
    data = data.drop_duplicates(subset=identity, keep="last")
    return data.sort_values(spec.time_column).set_index(spec.time_column)
