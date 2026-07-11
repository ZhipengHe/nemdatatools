"""Public API of nemdatatools."""

from __future__ import annotations

import datetime
import logging

import pandas as pd

from nemdatatools import mmsdm as _mmsdm
from nemdatatools.cache import Cache
from nemdatatools.catalog import TABLES, TableSpec
from nemdatatools.errors import CoverageError
from nemdatatools.listing import list_directory
from nemdatatools.router import fetch
from nemdatatools.timeutils import AEMO_DATETIME_FORMAT, parse_date

__all__ = ["availability", "fetch", "fetch_mmsdm_table", "tables"]

logger = logging.getLogger(__name__)


def tables() -> list[str]:
    """List the curated table names accepted by :func:`fetch`."""
    return sorted(TABLES)


def availability(table: str) -> dict[str, object]:
    """Describe where and when a curated table is available.

    Args:
        table: Curated table name.

    Returns:
        A mapping with the table's tier locations and known MMSDM gaps,
        suitable for printing.

    """
    from nemdatatools.catalog import get_table

    spec = get_table(table)
    return {
        "table": spec.name,
        "time_column": spec.time_column,
        "mmsdm": (
            None
            if spec.mmsdm is None
            else {
                "subdir": spec.mmsdm.subdir,
                "dvd_names": list(spec.mmsdm.dvd_names),
                "archive_name": spec.mmsdm.archive_name,
            }
        ),
        "reports_current": None if spec.report is None else spec.report.package,
        "reports_archive": (
            None if spec.report is None else spec.report.archive_package
        ),
        "gaps": [
            {
                "from": f"{g.start[0]}-{g.start[1]:02d}",
                "to": ("ongoing" if g.end is None else f"{g.end[0]}-{g.end[1]:02d}"),
                "reason": g.reason,
            }
            for g in spec.gaps
        ],
    }


def fetch_mmsdm_table(
    table: str,
    start: str | datetime.datetime,
    end: str | datetime.datetime,
    subdir: str = "DATA",
    cache: Cache | None = None,
) -> pd.DataFrame:
    """Fetch any MMSDM table by name, without curated metadata.

    Escape hatch for the ~236 archive tables outside the curated catalog:
    files are still discovered by listing-and-matching in both filename
    eras and all FILEnn parts are fetched, but no Reports stitching, gap
    checking, or de-duplication is applied. The time column is
    auto-detected for range filtering; rows are returned unfiltered when
    none is recognised.

    Args:
        table: MMSDM filename table token, e.g. ``"GENCONDATA"``.
        start: Range start, inclusive, naive NEM time.
        end: Range end, inclusive, naive NEM time.
        subdir: Snapshot subdirectory (``DATA``, ``P5MIN_ALL_DATA`` or
            ``PREDISP_ALL_DATA``).
        cache: Download/parse cache; a default one is used when omitted.

    Returns:
        All rows of the table across the months covering the range.

    Raises:
        CoverageError: If no month in the range carries the table.

    """
    from nemdatatools.catalog import MmsdmLocation

    start_dt, end_dt = parse_date(start), parse_date(end)
    cache = cache or Cache()
    name = table.upper()
    frames: list[pd.DataFrame] = []
    for year, month in _mmsdm.months_between(start_dt, end_dt):
        url = _mmsdm.month_url(year, month, subdir)
        entries = list_directory(url, session=cache.session)
        probe = TableSpec(
            name=name,
            cid_key=("", ""),
            time_column="",
            key_columns=(),
            mmsdm=MmsdmLocation(subdir, (name, f"{name}_ALL"), name),
        )
        for entry in _mmsdm.match_table_zips(entries, probe, year, month):
            frames.append(_load_all_segments(cache, entry.url))
    frames = [f for f in frames if not f.empty]
    if not frames:
        raise CoverageError(
            f"No MMSDM files matched table {name!r} under {subdir} for the "
            "requested range; check the name against the month listing.",
        )
    data = pd.concat(frames, ignore_index=True)
    return _filter_by_detected_time(data, start_dt, end_dt)


def _load_all_segments(cache: Cache, url: str) -> pd.DataFrame:
    """Load and concatenate every C/I/D segment of a payload."""
    from nemdatatools.cid import parse_cid_zip

    path = cache.download(url)
    tables_ = parse_cid_zip(path)
    if not tables_:
        return pd.DataFrame()
    frames = [t.frame for t in tables_]
    return pd.concat(frames, ignore_index=True)


def _filter_by_detected_time(
    data: pd.DataFrame,
    start: datetime.datetime,
    end: datetime.datetime,
) -> pd.DataFrame:
    """Range-filter on the first recognised AEMO time column, if any."""
    for column in ("SETTLEMENTDATE", "INTERVAL_DATETIME", "DATETIME"):
        if column in data.columns:
            stamps = pd.to_datetime(
                data[column],
                format=AEMO_DATETIME_FORMAT,
                errors="coerce",
            )
            mask = (stamps >= start) & (stamps <= end)
            filtered = data.loc[mask].copy()
            filtered[column] = stamps[mask]
            return filtered.sort_values(column).set_index(column)
    logger.warning(
        "no recognised time column; returning all rows of the fetched months",
    )
    return data
