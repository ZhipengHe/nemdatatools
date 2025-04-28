"""NEMDataTools: Tools for accessing and preprocessing AEMO data."""

__version__ = "0.1.0"

from nemdatatools.batch_commands import (
    download_multiple_tables,
    download_yearly_data,
)
from nemdatatools.data_source import NEM_REGIONS, DataSource

# Import key modules for easy access
from nemdatatools.downloader import (
    check_connection,
    fetch_data,
    get_available_data_types,
)

# Define what's accessible via import *
__all__ = [
    "NEM_REGIONS",
    "DataSource",
    "check_connection",
    "download_multiple_tables",
    "download_yearly_data",
    "fetch_data",
    "get_available_data_types",
]
