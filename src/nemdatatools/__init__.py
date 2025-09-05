"""NEMDataTools: Tools for accessing and preprocessing AEMO data."""

try:
    from ._version import __version__
except ImportError:
    __version__ = "unknown"

from nemdatatools.batch_commands import (
    download_multiple_tables,
    download_yearly_data,
)
from nemdatatools.data_source import NEM_REGIONS, DataSource
from nemdatatools.downloader import (
    check_connection,
    fetch_data,
    get_available_data_types,
)
from nemdatatools.processor import (
    calculate_demand_statistics,
    calculate_price_statistics,
    create_time_windows,
    resample_data,
)

# Define what's accessible via import *
__all__ = [
    "NEM_REGIONS",
    "DataSource",
    "calculate_demand_statistics",
    "calculate_price_statistics",
    "check_connection",
    "create_time_windows",
    "download_multiple_tables",
    "download_yearly_data",
    "fetch_data",
    "get_available_data_types",
    "resample_data",
]
