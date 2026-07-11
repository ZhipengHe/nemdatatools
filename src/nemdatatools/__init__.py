"""NEMDataTools: access and preprocess AEMO National Electricity Market data.

The package models AEMO's publication system directly: one request for a
table over a date range is served by stitching the MMSDM monthly archive,
Reports ARCHIVE daily bundles, and Reports CURRENT files, with known
availability gaps failing loudly. All timestamps are naive NEM time
(fixed UTC+10).
"""

try:
    from ._version import __version__
except ImportError:  # pragma: no cover - version file written at build time
    __version__ = "unknown"

from nemdatatools.api import availability, fetch, fetch_mmsdm_table, tables
from nemdatatools.cache import Cache
from nemdatatools.catalog import NEM_REGIONS
from nemdatatools.errors import (
    AvailabilityGapError,
    CoverageError,
    NemDataError,
)
from nemdatatools.price_demand import fetch_price_and_demand
from nemdatatools.transform import resample

__all__ = [
    "NEM_REGIONS",
    "AvailabilityGapError",
    "Cache",
    "CoverageError",
    "NemDataError",
    "__version__",
    "availability",
    "fetch",
    "fetch_mmsdm_table",
    "fetch_price_and_demand",
    "resample",
    "tables",
]
