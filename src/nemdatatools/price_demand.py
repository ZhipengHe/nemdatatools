"""Aggregated price-and-demand CSVs from aemo.com.au.

Unlike everything else in this package, these monthly per-region CSVs come
from AEMO's visualisation service rather than nemweb, and they are plain
CSV (no C/I/D framing). They provide a convenient long history of 5-minute
regional price and demand without MMSDM's bulk.
"""

from __future__ import annotations

import datetime
import logging

import pandas as pd

from nemdatatools.cache import Cache
from nemdatatools.catalog import NEM_REGIONS
from nemdatatools.mmsdm import months_between
from nemdatatools.timeutils import parse_date

logger = logging.getLogger(__name__)

_URL = (
    "https://aemo.com.au/aemo/data/nem/priceanddemand/"
    "PRICE_AND_DEMAND_{year}{month:02d}_{region}.csv"
)


def fetch_price_and_demand(
    start: str | datetime.datetime,
    end: str | datetime.datetime,
    regions: list[str] | None = None,
    cache: Cache | None = None,
) -> pd.DataFrame:
    """Fetch aggregated 5-minute price and demand by region.

    Args:
        start: Range start, inclusive, naive NEM time.
        end: Range end, inclusive, naive NEM time.
        regions: NEM regions to include; all five when omitted.
        cache: Download cache; a default one is used when omitted.

    Returns:
        Columns ``REGION``, ``TOTALDEMAND``, ``RRP``, ``PERIODTYPE`` indexed
        by ``SETTLEMENTDATE``, sorted, covering ``[start, end]``.

    """
    start_dt, end_dt = parse_date(start), parse_date(end)
    cache = cache or Cache()
    frames = []
    for year, month in months_between(start_dt, end_dt):
        for region in regions or NEM_REGIONS:
            url = _URL.format(year=year, month=month, region=region)
            path = cache.download(url)
            frames.append(pd.read_csv(path))
    data = pd.concat(frames, ignore_index=True)
    data["SETTLEMENTDATE"] = pd.to_datetime(data["SETTLEMENTDATE"])
    mask = (data["SETTLEMENTDATE"] >= start_dt) & (data["SETTLEMENTDATE"] <= end_dt)
    return (
        data.loc[mask]
        .drop_duplicates(subset=["SETTLEMENTDATE", "REGION"], keep="last")
        .sort_values("SETTLEMENTDATE")
        .set_index("SETTLEMENTDATE")
    )
