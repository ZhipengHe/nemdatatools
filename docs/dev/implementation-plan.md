# NEMDataTools Implementation Status

This document describes the current implementation status of the NEMDataTools package. The project has achieved production readiness with comprehensive functionality.

## Core Components

### 1. Downloader Module

The downloader module will be responsible for retrieving data from AEMO's public data sources.

**Key functionalities:**
- Determine the correct URLs for different data types
- Handle authentication if required by AEMO
- Manage HTTP requests with proper error handling
- Support different data formats (CSV, XML, JSON)
- Implement retry logic for failed requests
- Provide flexible date range handling

**Implementation approach:**
```python
def fetch_data(data_type, start_date, end_date, regions=None, cache_path=None):
    """
    Main function to fetch data from AEMO.

    1. Validate input parameters
    2. Map data_type to appropriate endpoint
    3. Check cache for existing data
    4. If not cached, download from AEMO
    5. Parse response
    6. Cache results if cache_path provided
    7. Return as DataFrame
    """
```

### 2. Cache Manager ✅ **IMPLEMENTED**

The cache manager handles local storage of downloaded data to avoid unnecessary requests.

**Implemented functionalities:**
- ✅ Store downloaded data in structured format with metadata indexing
- ✅ Intelligent cache lookup with exact and partial date range matching
- ✅ Handle partial cache hits seamlessly
- ✅ Configurable cache TTL and invalidation
- ✅ Automatic disk space management

**Implementation approach:**
```python
class CacheManager:
    """
    Manages local caching of AEMO data.
    """

    def __init__(self, cache_dir):
        """Initialize with cache directory."""

    def get_cached_data(self, data_type, start_date, end_date, regions):
        """Check if data is cached and return it if available."""

    def cache_data(self, data_type, start_date, end_date, regions, data):
        """Store data in cache."""

    def clear_cache(self, older_than=None):
        """Clear cache entries older than specified date."""
```

### 3. Time Utilities ✅ **IMPLEMENTED**

The time utilities module handles all time-related operations needed for AEMO data.

**Implemented functionalities:**
- ✅ Convert between different time formats with robust parsing
- ✅ Generate time periods for queries and analysis
- ✅ Proper AEST timezone handling for NEM data
- ✅ Support for dispatch intervals (5-minute, 30-minute, etc.)
- ✅ Forecast horizon calculations for pre-dispatch data

**Implementation approach:**
```python
def generate_intervals(start_date, end_date, interval="5min"):
    """Generate time intervals between dates."""

def convert_nem_datetime(date_string):
    """Convert AEMO datetime format to Python datetime."""

def get_forecast_horizon(run_time, target_time):
    """Calculate forecast horizon between run time and target time."""
```

### 4. Data Processor

The data processor will standardize and clean the raw data from AEMO.

**Key functionalities:**
- Normalize column names
- Convert data types
- Handle missing values
- Reshape data if needed
- Provide consistent output format

**Implementation approach:**
```python
def standardize(data, format_type=None):
    """
    Standardize raw AEMO data.

    1. Normalize column names
    2. Set appropriate index
    3. Convert data types
    4. Handle missing values
    5. Apply any specific formatting
    """

def merge_datasets(datasets, on=None):
    """Merge multiple datasets into one."""
```

### 5. Data Type Handlers ✅ **IMPLEMENTED**

Specialized handlers for different AEMO data types with specific processing requirements.

**Implemented data types:**
- ✅ DISPATCHPRICE - with price validation and intervention handling
- ✅ DISPATCHREGIONSUM - with demand aggregation and regional indexing
- ✅ PREDISPATCHPRICE - with forecast horizon calculations
- ✅ DISPATCH_UNIT_SCADA - with generator output processing
- ✅ Multiple additional MMSDM and static data types

**Implementation approach:**
```python
class DispatchPriceHandler:
    """Handler for DISPATCHPRICE data type."""

    @staticmethod
    def process(data):
        """Process DISPATCHPRICE data."""

class PredispatchHandler:
    """Handler for PREDISPATCH data type."""

    @staticmethod
    def process(data):
        """Process PREDISPATCH data."""
```

### 6. Batch Commands ✅ **IMPLEMENTED**

The batch commands module provides efficient parallel downloading capabilities for bulk data operations.

#### Implemented Functionalities:
- ✅ Parallel downloads using ThreadPoolExecutor for efficiency
- ✅ Progress tracking with tqdm for user feedback
- ✅ Multi-table batch downloading
- ✅ Multi-year data fetching capabilities
- ✅ Error handling and retry logic for failed downloads
- Configurable delays between requests (respecting AEMO rate limits)
- Comprehensive error handling and logging
- Flexible caching options
- Backward compatibility

#### Implementation Details:

```python
def download_yearly_data(years, tables, max_workers=3, delay=2):
    """
    Download multiple years/tables in parallel.

    Implementation:
    1. Validate input parameters
    2. Initialize ThreadPoolExecutor with max_workers
    3. Create futures for each year/table combination
    4. Track progress with tqdm progress bar
    5. Handle results and errors as they complete
    6. Return nested dictionary of results
    """
```

```python
def download_multiple_tables(tables, start_date, end_date):
    """
    Download multiple tables sequentially.

    Implementation:
    1. Validate input parameters
    2. Iterate through requested tables
    3. Call fetch_data for each table
    4. Collect results in dictionary
    5. Return dictionary of DataFrames
    """
```

#### Example Usage:

```python
# Parallel yearly downloads
from nemdatatools.batch_commands import download_yearly_data

results = download_yearly_data(
    years=[2022, 2023, 2024],
    tables=["DISPATCHPRICE", "PREDISPATCHPRICE"],
    max_workers=4,  # Number of parallel downloads
    delay=1,       # Minimum delay between requests (seconds)
    overwrite=False # Skip existing files
)

# Multiple table downloads
from nemdatatools.batch_commands import download_multiple_tables

results = download_multiple_tables(
    table_names=["DISPATCHPRICE", "DISPATCHREGIONSUM"],
    start_date="2024/01/01",
    end_date="2024/01/31",
    regions=["NSW1", "QLD1"]  # Optional region filter
)
```

#### Key Parameters:

| Parameter | Type | Description | Default |
|-----------|------|-------------|---------|
| max_workers | int | Maximum parallel downloads | 3 |
| delay | int | Minimum delay between requests (seconds) | 2 |
| overwrite | bool | Force re-download existing files | False |
| cache_path | str | Custom download directory | "data/aemo_data" |

#### Error Handling Strategy:
- Failed downloads are logged with full error details
- None is returned for failed downloads
- Other downloads continue unaffected
- Progress bar continues tracking completed tasks

## Development Timeline

See the [Project Board](./project-structure.md) for a detailed breakdown of tasks and milestones.

## Implementation Details

### AEMO Data Access

AEMO provides data through several mechanisms:
1. MMS Data Model
2. Public data files
3. NEMWeb portal

Our implementation will focus on accessing publicly available data without requiring special credentials. We'll use the following approach:

1. Map data types to appropriate public URLs
2. Use standard HTTP requests to fetch data
3. Parse returned data (typically CSV or XML)
4. Convert to pandas DataFrames for easy analysis

### Caching Strategy

Our caching strategy will be:

1. Create a directory structure based on data_type, regions, and date ranges
2. Store data in parquet format for efficiency
3. Implement metadata for each cache entry
4. Support partial cache hits by combining cached and newly fetched data
5. Provide cache management functions to control disk usage

### Error Handling

We'll implement robust error handling:

1. Validate all input parameters
2. Handle HTTP errors with appropriate retries and backoffs
3. Provide clear error messages for common issues
4. Log detailed diagnostic information
5. Support graceful degradation when services are unavailable

## Future Extensions

After the initial implementation, we plan to extend the package with:

1. Support for more AEMO data types
2. Advanced visualization tools
3. Integration with other energy data sources
4. Time series analysis utilities
5. Forecasting tools

These extensions will be prioritized based on user feedback after the initial release.
