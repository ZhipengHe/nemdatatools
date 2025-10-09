"""NEMDataTools - Module for downloading data from AEMO.

This module provides functions to download various types of data from
the Australian Energy Market Operator (AEMO).
"""

import logging
import os
import re
import secrets
import time
import zipfile
from datetime import timedelta

import pandas as pd
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

from nemdatatools import cache, processor, timeutils
from nemdatatools.data_source import (
    BASE_URLS,
    DATA_CONFIG,
    NEM_REGIONS,
    URL_TEMPLATES,
    DataSource,
)

logger = logging.getLogger(__name__)

# Maximum retries for failed requests
MAX_RETRIES = 3
# Default timeout for HTTP requests (seconds)
REQUEST_TIMEOUT = 30
# Default delay between requests (seconds)
DEFAULT_DELAY = 1

# List of common user agents for rotating
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/92.0.4515.159 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 "
    "(KHTML, like Gecko) Version/15.0 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:90.0) Gecko/20100101 Firefox/90.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Edge/92.0.902.84",
]


def get_random_headers() -> dict[str, str]:
    """Get random headers to mimic a browser request.

    Returns:
        dict: Headers dictionary with randomized user agent

    """
    return {
        "User-Agent": secrets.choice(USER_AGENTS),
        "Accept": (
            "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8"
        ),
        "Accept-Language": "en-US,en;q=0.5",
        "Connection": "keep-alive",
    }


def build_price_and_demand_url(year: int, month: int, region: str) -> str:
    """Build URL for Price and Demand data.

    Args:
        year: Year
        month: Month
        region: Region code (e.g., 'NSW1')

    Returns:
        str: URL for the data

    """
    month_str = f"{year}{month:02d}"
    return URL_TEMPLATES[DataSource.PRICE_AND_DEMAND].format(
        yearmonth=month_str,
        region=region,
    )


def download_file(
    url: str,
    output_path: str,
    headers: dict | None = None,
    timeout: int = REQUEST_TIMEOUT,
    chunk_size: int = 8192,
    max_retries: int = MAX_RETRIES,
    delay: float = DEFAULT_DELAY,
) -> bool:
    """Download a file from a URL with retries and error handling.

    Args:
        url: URL to download
        output_path: Path to save the file
        headers: HTTP headers to use
        timeout: Timeout for the request
        chunk_size: Size of chunks for streaming downloads
        max_retries: Maximum number of retries
        delay: Delay between retries

    Returns:
        bool: True if download successful, False otherwise

    """
    if headers is None:
        headers = get_random_headers()

    # Create directory if it doesn't exist
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    for attempt in range(max_retries):
        try:
            # Add delay between attempts (not on first attempt)
            if attempt > 0:
                time.sleep(delay * (2**attempt))  # Exponential backoff

            # Check if file is large enough to stream
            with requests.head(url, headers=headers, timeout=timeout) as head:
                head.raise_for_status()
                content_length = int(head.headers.get("content-length", 0))

                # If file is small or content length unknown, download without streaming
                if content_length < 10 * 1024 * 1024:  # Less than 10MB
                    logger.info(f"Downloading {url} to {output_path}")
                    with requests.get(
                        url,
                        headers=headers,
                        timeout=timeout,
                    ) as response:
                        response.raise_for_status()
                        with open(output_path, "wb") as f:
                            f.write(response.content)
                    logger.info(f"Downloaded {output_path}")
                    return True

                # For larger files, stream the download
                logger.info(
                    (
                        f"Streaming download of {url} "
                        f"({content_length / 1024 / 1024:.2f} MB) to {output_path}"
                    ),
                )
                with requests.get(
                    url,
                    headers=headers,
                    timeout=timeout,
                    stream=True,
                ) as response:
                    response.raise_for_status()
                    downloaded = 0

                    with open(output_path, "wb") as f:
                        for chunk in response.iter_content(chunk_size=chunk_size):
                            if chunk:
                                f.write(chunk)
                                downloaded += len(chunk)
                                if content_length > 0:
                                    percent = (downloaded / content_length) * 100
                                    if downloaded % (1024 * 1024) == 0:  # Log every 1MB
                                        logger.debug(
                                            (
                                                f"Download progress: {percent:.1f}% "
                                                f"({downloaded / 1024 / 1024:.1f} MB)"
                                            ),
                                        )

                logger.info(f"Downloaded {output_path}")
                return True

        except requests.exceptions.RequestException as e:
            logger.warning(f"Attempt {attempt + 1}/{max_retries} failed for {url}: {e}")
            if attempt == max_retries - 1:
                logger.error(f"Failed to download {url} after {max_retries} attempts")
                return False

    return False


def extract_zip(
    zip_path: str,
    extract_dir: str,
    specific_file: str | None = None,
) -> str | None:
    """Extract a zip file to a directory.

    Args:
        zip_path: Path to the zip file
        extract_dir: Directory to extract to
        specific_file: If specified, extract only this file

    Returns:
        str: Path to the extracted file if specific_file is specified, otherwise None

    """
    try:
        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            if specific_file:
                zip_ref.extract(specific_file, extract_dir)
                return os.path.join(extract_dir, specific_file)
            else:
                zip_ref.extractall(extract_dir)
                return None
    except zipfile.BadZipFile as e:
        logger.error(f"Bad zip file {zip_path}: {e}")
        return None
    except Exception as e:
        logger.error(f"Error extracting {zip_path}: {e}")
        return None


def download_mmsdm_data(
    table_name: str,
    start_date: str,
    end_date: str,
    output_dir: str = "data/aemo_data",
    extract: bool = True,
    delay: float = DEFAULT_DELAY,
    overwrite: bool = False,
) -> list[str]:
    """Download MMSDM data for a specific table and date range.

    Args:
        table_name: Name of the MMSDM table
        start_date: Start date (YYYY/MM/DD)
        end_date: End date (YYYY/MM/DD)
        output_dir: Directory to save files
        extract: Whether to extract the zip files
        delay: Delay between requests in seconds
        overwrite: Whether to overwrite existing files

    Returns:
        list: List of downloaded file paths

    """
    # Import here to avoid circular imports
    from nemdatatools.mmsdm_helper import (
        build_mmsdm_url,
        determine_mmsdm_data_source,
        extract_mmsdm_file,
    )

    # Parse dates
    start_dt = timeutils.parse_date(start_date)
    end_dt = timeutils.parse_date(end_date)

    # Determine the appropriate data source for this table
    data_source = determine_mmsdm_data_source(table_name)

    # Create output directory
    os.makedirs(output_dir, exist_ok=True)

    # Generate list of year/month combinations to download
    date_list = []
    current_dt = start_dt.replace(day=1)  # Start at beginning of month
    end_month_dt = end_dt.replace(day=1)  # End at beginning of month

    while current_dt <= end_month_dt:
        date_list.append((current_dt.year, current_dt.month))
        # Move to next month
        if current_dt.month == 12:
            current_dt = current_dt.replace(year=current_dt.year + 1, month=1)
        else:
            current_dt = current_dt.replace(month=current_dt.month + 1)

    downloaded_files = []

    # Download each month
    for year, month in date_list:
        # Build URL
        url = build_mmsdm_url(table_name, year, month, data_source)

        # Define output paths
        zipname = os.path.join(output_dir, f"{table_name}_{year}{month:02d}.zip")
        csvname = os.path.join(output_dir, f"{table_name}_{year}{month:02d}.csv")

        # Skip if already exists and not overwriting
        if not overwrite and (
            (extract and os.path.exists(csvname))
            or (not extract and os.path.exists(zipname))
        ):
            logger.info(f"Skipping {year}-{month:02d} - already exists")
            if extract:
                downloaded_files.append(csvname)
            else:
                downloaded_files.append(zipname)
            continue

        # Download zip file
        if download_file(url, zipname, delay=delay):
            downloaded_files.append(zipname)

            # Extract if needed
            if extract:
                # Extract and rename
                extracted = extract_mmsdm_file(
                    zip_path=zipname,
                    output_dir=output_dir,
                    table_name=table_name,
                    year=year,
                    month=month,
                    data_source=data_source,
                )
                if extracted:
                    downloaded_files.append(extracted)

                    # Optionally remove zip after extraction
                    # os.remove(zipname)

        # Be polite to the server
        time.sleep(delay)

    return downloaded_files


def download_price_and_demand(
    start_date: str,
    end_date: str,
    regions: list[str] | None = None,
    output_dir: str = "data/aemo_data",
    delay: float = DEFAULT_DELAY,
    overwrite: bool = False,
) -> list[str]:
    """Download Price and Demand data for specified regions and date range.

    Args:
        start_date: Start date (YYYY/MM/DD)
        end_date: End date (YYYY/MM/DD)
        regions: List of regions (defaults to all regions)
        output_dir: Directory to save files
        delay: Delay between requests in seconds
        overwrite: Whether to overwrite existing files

    Returns:
        list: List of downloaded file paths

    """
    # Parse dates
    start_dt = timeutils.parse_date(start_date)
    end_dt = timeutils.parse_date(end_date)

    # Default to all regions if none specified
    if regions is None:
        regions = NEM_REGIONS

    # Create output directory
    os.makedirs(output_dir, exist_ok=True)

    # Generate list of year/month combinations to download
    date_list = []
    current_dt = start_dt.replace(day=1)  # Start at beginning of month
    end_month_dt = end_dt.replace(day=1)  # End at beginning of month

    while current_dt <= end_month_dt:
        date_list.append((current_dt.year, current_dt.month))
        # Move to next month
        if current_dt.month == 12:
            current_dt = current_dt.replace(year=current_dt.year + 1, month=1)
        else:
            current_dt = current_dt.replace(month=current_dt.month + 1)

    downloaded_files = []

    # Download data for each region and month
    for year, month in date_list:
        month_str = f"{year}{month:02d}"

        for region in regions:
            # Define output filename
            filename = os.path.join(
                output_dir,
                f"PRICE_AND_DEMAND_{month_str}_{region}.csv",
            )

            # Skip if already exists and not overwriting
            if not overwrite and os.path.exists(filename):
                logger.info(f"Skipping {filename} - already exists")
                downloaded_files.append(filename)
                continue

            # Build URL
            url = build_price_and_demand_url(year, month, region)

            # Download file
            if download_file(url, filename, delay=delay):
                downloaded_files.append(filename)

            # Be polite to the server
            time.sleep(delay)

    return downloaded_files


def download_static_data(
    data_type: str,
    output_dir: str = "data/aemo_data",
    overwrite: bool = False,
) -> str | None:
    """Download static reference data from AEMO.

    Args:
        data_type: Type of static data to download
        output_dir: Directory to save files
        overwrite: Whether to overwrite existing files

    Returns:
        str: Path to the downloaded file or None if failed

    """
    if (
        data_type not in DATA_CONFIG
        or DATA_CONFIG[data_type]["source"] != DataSource.STATIC
    ):
        logger.error(f"Invalid static data type: {data_type}")
        return None

    # Create output directory
    os.makedirs(output_dir, exist_ok=True)

    # Get URL and format
    url = DATA_CONFIG[data_type]["url"]
    file_format = DATA_CONFIG[data_type]["format"]

    # Define output filename
    filename = os.path.join(output_dir, f"{data_type}.{file_format}")

    # Skip if already exists and not overwriting
    if not overwrite and os.path.exists(filename):
        logger.info(f"Skipping {filename} - already exists")
        return filename

    # Download file
    if download_file(url, filename):
        return filename

    return None


def fetch_data(
    data_type: str,
    start_date: str,
    end_date: str,
    regions: list[str] | None = None,
    cache_path: str | None = None,
    download_dir: str = "data/aemo_data",
    delay: float = DEFAULT_DELAY,
    overwrite: bool = False,
    days: int = 14,
) -> pd.DataFrame:
    """Download and process data from AEMO.

    Args:
        data_type: Type of data to download
        start_date: Start date in format YYYY/MM/DD (not used for REPORTS_CURRENT)
        end_date: End date in format YYYY/MM/DD (not used for REPORTS_CURRENT)
        regions: List of regions to include (optional)
        cache_path: Path to cache downloaded data (optional)
        download_dir: Directory to save downloaded files
        delay: Delay between requests in seconds
        overwrite: Whether to overwrite existing files
        days: Number of days to look back for REPORTS_CURRENT (default 14)

    Returns:
        DataFrame with requested data

    """
    # Validate data type
    if data_type not in DATA_CONFIG:
        supported_types = ", ".join(DATA_CONFIG.keys())
        raise ValueError(
            f"Unsupported data type: {data_type}. Supported types: {supported_types}",
        )

    # Parse dates
    start_dt = timeutils.parse_date(start_date)
    end_dt = timeutils.parse_date(end_date)

    # Use all regions if none specified
    if regions is None:
        regions = NEM_REGIONS

    # Check cache first if a cache path is provided
    if cache_path:
        cache_mgr = cache.CacheManager(cache_path)
        cached_data = cache_mgr.get_cached_data(data_type, start_dt, end_dt, regions)
        if cached_data is not None:
            logger.info(f"Retrieved {data_type} data from cache")
            return cached_data

    # Initialize empty dataframe for results
    data = pd.DataFrame()

    # Download based on data source type
    source = DATA_CONFIG[data_type]["source"]

    # Type-specific output directory
    type_dir = os.path.join(download_dir, data_type)
    os.makedirs(type_dir, exist_ok=True)

    if source in [DataSource.MMSDM, DataSource.MMSDM_PREDISP, DataSource.MMSDM_P5MIN]:
        # Download MMSDM data
        # TODO: Refactor to use _parse_aemo_csv() instead of
        # mmsdm_helper.read_mmsdm_csv() for consistent C/I/D format parsing
        downloaded_files = download_mmsdm_data(
            data_type,
            start_date,
            end_date,
            output_dir=type_dir,
            extract=True,
            delay=delay,
            overwrite=overwrite,
        )

        # Process downloaded files
        # Import here to avoid circular imports
        from nemdatatools.mmsdm_helper import combine_mmsdm_files, filter_mmsdm_data

        csv_files = [f for f in downloaded_files if f.endswith(".csv")]
        if csv_files:
            data = combine_mmsdm_files(csv_files)
            data = filter_mmsdm_data(data, start_dt, end_dt, regions)

    elif source == DataSource.PRICE_AND_DEMAND:
        # Download Price and Demand data
        # TODO: Evaluate if PRICE_AND_DEMAND uses C/I/D format and needs
        # _parse_aemo_csv()
        downloaded_files = download_price_and_demand(
            start_date,
            end_date,
            regions,
            output_dir=type_dir,
            delay=delay,
            overwrite=overwrite,
        )

        # Process downloaded files
        all_data = []
        for file_path in downloaded_files:
            if file_path.endswith(".csv"):
                try:
                    df = pd.read_csv(file_path)
                    all_data.append(df)
                except Exception as e:
                    logger.error(f"Error processing {file_path}: {e}")

        if all_data:
            data = pd.concat(all_data, ignore_index=True)

            # Filter by date range if appropriate column exists
            if "SETTLEMENTDATE" in data.columns:
                data["SETTLEMENTDATE"] = pd.to_datetime(data["SETTLEMENTDATE"])
                data = data[
                    (data["SETTLEMENTDATE"] >= start_dt)
                    & (data["SETTLEMENTDATE"] <= end_dt)
                ]

    elif source == DataSource.REPORTS_CURRENT:
        # Download REPORTS/CURRENT data (rolling window)
        # Note: start_date/end_date not applicable, uses 'days' parameter
        downloaded_files = download_reports_current(
            data_type,
            output_dir=type_dir,
            days=days,
            delay=delay,
            overwrite=overwrite,
        )

        # Process downloaded files using new C/I/D format parser
        all_data = []
        for file_path in downloaded_files:
            if file_path.endswith(".zip"):
                # Extract and read CSV from zip
                extract_dir = os.path.join(type_dir, "extracted")
                os.makedirs(extract_dir, exist_ok=True)

                try:
                    with zipfile.ZipFile(file_path, "r") as zip_ref:
                        # Get CSV file from zip (usually first CSV file)
                        csv_files = [
                            f for f in zip_ref.namelist() if f.lower().endswith(".csv")
                        ]
                        if csv_files:
                            csv_file = csv_files[0]
                            csv_path = zip_ref.extract(csv_file, extract_dir)

                            # Use new parser to handle multi-table C/I/D format
                            tables = _parse_aemo_csv(csv_path)

                            # Combine all tables into single DataFrame
                            # (e.g., TradingIS has PRICE + INTERCONNECTORRES)
                            if tables:
                                for table_name, table_df in tables.items():
                                    # Add table identifier for reference
                                    table_df["TABLE_NAME"] = table_name
                                    all_data.append(table_df)
                except Exception as e:
                    logger.error(f"Error processing {file_path}: {e}")

        if all_data:
            # Concatenate all tables from all files
            data = pd.concat(all_data, ignore_index=True)

            # Filter by date range if appropriate column exists
            if "SETTLEMENTDATE" in data.columns:
                data["SETTLEMENTDATE"] = pd.to_datetime(data["SETTLEMENTDATE"])
                data = data[
                    (data["SETTLEMENTDATE"] >= start_dt)
                    & (data["SETTLEMENTDATE"] <= end_dt)
                ]

    elif source == DataSource.STATIC:
        # Download static data
        file_path_or_none = download_static_data(
            data_type,
            output_dir=type_dir,
            overwrite=overwrite,
        )

        if file_path_or_none:
            file_path = file_path_or_none
            file_format = DATA_CONFIG[data_type]["format"]
            try:
                if file_format == "xlsx" or file_format == "xls":
                    data = pd.read_excel(file_path)
                elif file_format == "csv":
                    data = pd.read_csv(file_path)
                else:
                    logger.error(f"Unsupported file format: {file_format}")
            except Exception as e:
                logger.error(f"Error processing {file_path}: {e}")

    else:
        logger.error(f"Unsupported data source: {source}")
        return pd.DataFrame()

    # Process the data according to its type
    if not data.empty:
        data = processor.standardize(data, data_type)

        # Cache the processed data if a cache path is provided
        if cache_path:
            cache_mgr = cache.CacheManager(cache_path)
            cache_mgr.cache_data(data_type, start_dt, end_dt, regions, data)

    return data


def get_available_data_types() -> list[str]:
    """Get list of available data types supported by this package.

    Returns:
        List of supported data types

    """
    return list(DATA_CONFIG.keys())


def check_connection() -> bool:
    """Check connection to AEMO data sources.

    Returns:
        True if connection is successful, False otherwise

    """
    try:
        response = requests.get(BASE_URLS["MMSDM"], timeout=5)
        return bool(response.status_code == 200)
    except requests.exceptions.Timeout:
        logging.error("Connection timed out while accessing AEMO data sources.")
    except requests.exceptions.ConnectionError:
        logging.error("Failed to connect to AEMO data sources.")
    except requests.exceptions.RequestException as e:
        logging.error(f"Request error: {e}")

    return False


def download_all_regions(
    data_type: str,
    start_date: str,
    end_date: str,
    download_dir: str = "data/aemo_data",
    delay: float = DEFAULT_DELAY,
    overwrite: bool = False,
) -> dict[str, list[str]]:
    """Download data for all regions and return dictionary of file paths.

    Args:
        data_type: Type of data to download
        start_date: Start date in format YYYY/MM/DD
        end_date: End date in format YYYY/MM/DD
        download_dir: Directory to save downloaded files
        delay: Delay between requests in seconds
        overwrite: Whether to overwrite existing files

    Returns:
        Dict mapping region to list of downloaded files

    """
    result = {}

    for region in NEM_REGIONS:
        if data_type == "PRICE_AND_DEMAND":
            files = download_price_and_demand(
                start_date,
                end_date,
                [region],
                output_dir=download_dir,
                delay=delay,
                overwrite=overwrite,
            )
        else:
            # For other data types that may be filtered by region later
            type_dir = os.path.join(download_dir, data_type)
            os.makedirs(type_dir, exist_ok=True)

            if DATA_CONFIG[data_type]["source"] in [
                DataSource.MMSDM,
                DataSource.MMSDM_PREDISP,
                DataSource.MMSDM_P5MIN,
            ]:
                files = download_mmsdm_data(
                    data_type,
                    start_date,
                    end_date,
                    output_dir=type_dir,
                    extract=True,
                    delay=delay,
                    overwrite=overwrite,
                )
            else:
                # Other data sources would be handled here
                files = []

        result[region] = files

    return result


def _parse_aemo_csv(csv_path: str) -> dict[str, pd.DataFrame]:
    """Parse AEMO CSV file containing one or more tables in C/I/D format.

    AEMO CSV files use a standard format:
    C,<comment/header line>
    I,<record_type>,<table_name>,<version>,<col1>,<col2>,...
    D,<record_type>,<table_name>,<version>,<val1>,<val2>,...

    This handles two situations:
    - Single table: Returns dict with one table (e.g., most MMSDM files)
    - Multiple tables: Returns dict with multiple tables (e.g., TradingIS reports)

    Args:
        csv_path: Path to the CSV file

    Returns:
        Dictionary mapping table names to DataFrames
        For single table files, dict will have one entry
        For multi-table files (like TradingIS), dict will have multiple entries

    """
    tables = {}
    current_table = None
    current_columns = None
    current_rows: list[list[str]] = []

    try:
        with open(csv_path) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue

                parts = line.split(",")
                record_type = parts[0]

                if record_type == "C":
                    # Comment/header line - skip
                    continue

                elif record_type == "I":
                    # Column definition line
                    # Save previous table if exists
                    if current_table and current_rows:
                        tables[current_table] = pd.DataFrame(
                            current_rows,
                            columns=current_columns,
                        )

                    # Start new table
                    # Format: I,<record_type>,<table_name>,<version>,<columns...>
                    current_table = parts[2]  # Table name
                    current_columns = parts[4:]  # Column names start at index 4
                    current_rows = []

                elif record_type == "D":
                    # Data line
                    if current_columns:
                        # Format: D,<record_type>,<table_name>,<version>,<values...>
                        # Extract data values (skip first 4 fields)
                        values = parts[4 : 4 + len(current_columns)]
                        # Remove quotes from values
                        values = [v.strip('"') for v in values]
                        current_rows.append(values)

        # Save last table
        if current_table and current_rows:
            tables[current_table] = pd.DataFrame(current_rows, columns=current_columns)

        logger.info(
            f"Parsed {len(tables)} table(s) from {csv_path}: {list(tables.keys())}",
        )
        return tables

    except Exception as e:
        logger.error(f"Error parsing AEMO CSV {csv_path}: {e}")
        return {}


def scrape_reports_current_directory(
    url: str,
    days: int = 14,
    file_pattern: str | None = None,
) -> list[dict]:
    """Scrape a REPORTS/CURRENT directory listing to get file URLs.

    Args:
        url: URL of the directory listing page
        days: Number of days to look back (default 14)
        file_pattern: Optional regex pattern to filter files

    Returns:
        List of dicts with 'filename', 'url', and 'datetime' keys

    """
    try:
        headers = get_random_headers()
        response = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, "html.parser")

        logger.info(f"Scraping {url} for files from last {days} days")

        all_links = soup.find_all("a")
        logger.info(f"Found {len(all_links)} total links on page")

        # First pass: extract all file timestamps to find the most recent
        from datetime import datetime as dt

        file_timestamps: list[tuple[str, dt]] = []

        # Debug: show sample hrefs
        sample_shown = 0
        for link in all_links:
            href = link.get("href")
            if href and "TRADINGIS" in href and sample_shown < 5:
                logger.info(f"Sample href: {href!r}")
                sample_shown += 1

        matched_count = 0
        for link in all_links:
            href = link.get("href")
            # Type guard: ensure href is a string
            if not isinstance(href, str):
                continue
            if not href or href.startswith("?") or href == "/REPORTS/CURRENT/":
                continue

            # Extract datetime from filename
            # Format is YYYYMMDDHHMM (12 digits), not YYYYMMDDHHMMSS (14 digits)
            datetime_match = re.search(r"PUBLIC_TRADINGIS_(\d{12})_", href)
            if datetime_match:
                matched_count += 1
                if matched_count <= 3:
                    logger.info(
                        f"Regex matched! href={href!r}, "
                        f"extracted={datetime_match.group(1)}",
                    )
                datetime_str = datetime_match.group(1)
                try:
                    file_datetime = dt.strptime(datetime_str, "%Y%m%d%H%M")
                    file_timestamps.append((href, file_datetime))
                except (ValueError, AttributeError) as e:
                    logger.warning(f"Failed to parse datetime {datetime_str}: {e}")
                    continue

        logger.info(f"Total files with matching timestamps: {matched_count}")

        if not file_timestamps:
            logger.warning("No TradingIS files found in directory listing")
            return []

        # Find the most recent file timestamp
        newest_file_datetime = max(ts[1] for ts in file_timestamps)
        logger.info(f"Most recent file timestamp: {newest_file_datetime}")

        # Calculate cutoff based on newest file
        cutoff_date = newest_file_datetime - timedelta(days=days)
        logger.info(f"Cutoff date (newest - {days} days): {cutoff_date}")

        # Second pass: filter files within the rolling window
        files: list[dict[str, str | dt]] = []
        for href, file_datetime in file_timestamps:
            # Check if file is within the rolling window
            if file_datetime >= cutoff_date:
                # Apply file pattern filter if specified
                if file_pattern is None or re.search(file_pattern, href):
                    # href is already an absolute path starting with /
                    # Extract base URL from the directory URL
                    from urllib.parse import urlparse

                    parsed = urlparse(url)
                    file_url = f"{parsed.scheme}://{parsed.netloc}{href}"

                    files.append(
                        {
                            "filename": href,
                            "url": file_url,
                            "datetime": file_datetime,
                        },
                    )
                else:
                    logger.debug(f"Skipped (pattern mismatch): {href}")

        # Sort by datetime (newest first)
        from typing import cast

        files.sort(key=lambda x: cast(dt, x["datetime"]), reverse=True)

        logger.info(f"Found {len(files)} files in the last {days} days")
        return files

    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to scrape directory {url}: {e}")
        return []
    except Exception as e:
        logger.error(f"Error parsing directory listing: {e}")
        import traceback

        logger.error(traceback.format_exc())
        return []


def download_reports_current(
    report_type: str,
    output_dir: str = "data/aemo_data",
    days: int = 14,
    delay: float = DEFAULT_DELAY,
    overwrite: bool = False,
) -> list[str]:
    """Download REPORTS/CURRENT data for a specific report type.

    Args:
        report_type: Type of report (e.g., 'PUBLIC_TRADINGIS')
        output_dir: Directory to save files
        days: Number of days to look back (default 14)
        delay: Delay between requests in seconds
        overwrite: Whether to overwrite existing files

    Returns:
        List of downloaded file paths

    """
    # Define report-specific configurations
    report_configs = {
        "PUBLIC_TRADINGIS": {
            "url": "https://www.nemweb.com.au/REPORTS/CURRENT/TradingIS_Reports/",
            "pattern": r"PUBLIC_TRADINGIS_\d{12}_\d+\.zip",  # 12 digits: YYYYMMDDHHMM
        },
    }

    if report_type not in report_configs:
        logger.error(f"Unsupported report type: {report_type}")
        return []

    config = report_configs[report_type]

    # Create output directory (output_dir already includes report_type from caller)
    os.makedirs(output_dir, exist_ok=True)

    # Scrape directory listing
    files = scrape_reports_current_directory(
        config["url"],
        days=days,
        file_pattern=config.get("pattern"),
    )

    downloaded_files = []

    # Count existing files first
    existing_count = sum(
        1
        for f in files
        if os.path.exists(os.path.join(output_dir, os.path.basename(f["filename"])))
    )
    files_to_download = len(files) - existing_count

    logger.info(
        f"Found {existing_count} existing files, "
        f"downloading {files_to_download} new files...",
    )

    # Download files sequentially with progress bar
    skipped = 0
    downloaded = 0
    failed = 0

    with tqdm(total=len(files), desc="Processing files", unit="file") as pbar:
        for file_info in files:
            filename = file_info["filename"]
            file_url = file_info["url"]

            # Extract just the base filename (remove path)
            base_filename = os.path.basename(filename)
            output_path = os.path.join(output_dir, base_filename)

            # Skip if already exists and not overwriting
            if not overwrite and os.path.exists(output_path):
                downloaded_files.append(output_path)
                skipped += 1
                pbar.set_postfix(
                    {"downloaded": downloaded, "skipped": skipped, "failed": failed},
                )
                pbar.update(1)
                continue

            # Download file
            if download_file(file_url, output_path, delay=0):
                downloaded_files.append(output_path)
                downloaded += 1
            else:
                failed += 1

            pbar.set_postfix(
                {"downloaded": downloaded, "skipped": skipped, "failed": failed},
            )
            pbar.update(1)

            # Be polite to the server
            if delay > 0:
                time.sleep(delay)

    logger.info(
        f"Successfully downloaded/found {len(downloaded_files)}/{len(files)} files",
    )
    return downloaded_files
