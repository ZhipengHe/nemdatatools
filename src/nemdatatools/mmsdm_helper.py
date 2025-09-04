"""NEMDataTools - Helper functions for working with AEMO MMS Data Model (MMSDM) files.

This module provides specialized functions for building filenames, URLs,
and handling the specific format requirements of MMSDM data.
"""

import logging
import os
import zipfile

import pandas as pd

from nemdatatools.data_source import URL_TEMPLATES, DataSource

logger = logging.getLogger(__name__)


def build_mmsdm_filename(table_name: str, year: int, month: int) -> str:
    """Build the correct filename format for MMSDM data.

    The format changed in August 2024 from PUBLIC_DVD_* to PUBLIC_ARCHIVE#*

    Args:
        table_name: Name of the table
        year: Year
        month: Month

    Returns:
        str: Properly formatted filename

    """
    month_str = f"{year}{month:02d}"

    # Format changed in August 2024
    if (year == 2024 and month >= 8) or year >= 2025:
        return f"PUBLIC_ARCHIVE%23{table_name}%23FILE01%23{month_str}010000"
    else:
        return f"PUBLIC_DVD_{table_name}_{month_str}010000"


def build_mmsdm_predisp_filename(table_name: str, year: int, month: int) -> str:
    """Build the correct filename format for MMSDM pre-dispatch data.

    Args:
        table_name: Name of the table
        year: Year
        month: Month

    Returns:
        str: Properly formatted filename

    """
    month_str = f"{year}{month:02d}"

    # Format changed in August 2024
    if (year == 2024 and month >= 8) or year >= 2025:
        return f"PUBLIC_ARCHIVE%23{table_name}%23ALL%23FILE01%23{month_str}010000"
    else:
        return f"PUBLIC_DVD_{table_name}_{month_str}010000"


def build_mmsdm_url(
    table_name: str,
    year: int,
    month: int,
    data_source: DataSource,
) -> str:
    """Build URL for MMSDM data.

    Args:
        table_name: Name of the table
        year: Year
        month: Month
        data_source: Source data type (MMSDM, MMSDM_PREDISP, or MMSDM_P5MIN)

    Returns:
        str: URL for the data

    """
    # Validate data source
    if data_source not in [
        DataSource.MMSDM,
        DataSource.MMSDM_PREDISP,
        DataSource.MMSDM_P5MIN,
    ]:
        raise ValueError(f"Invalid data source for MMSDM: {data_source}")

    if data_source == DataSource.MMSDM_PREDISP:
        filename = build_mmsdm_predisp_filename(table_name, year, month)
    else:
        filename = build_mmsdm_filename(table_name, year, month)
    template = URL_TEMPLATES[data_source]

    return template.format(year=year, month=f"{month:02d}", filename=filename)


def determine_mmsdm_data_source(table_name: str) -> DataSource:
    """Determine the appropriate MMSDM data source based on table name.

    Args:
        table_name: Name of the table

    Returns:
        DataSource: The appropriate data source enum

    """
    from nemdatatools.data_source import DATA_CONFIG

    if table_name in DATA_CONFIG:
        return DATA_CONFIG[table_name]["source"]

    # Default mapping based on common patterns
    if table_name.startswith("PREDISPATCH"):
        return DataSource.MMSDM_PREDISP
    elif table_name.startswith("P5MIN"):
        return DataSource.MMSDM_P5MIN
    else:
        return DataSource.MMSDM


def extract_mmsdm_file(
    zip_path: str,
    output_dir: str,
    table_name: str,
    year: int,
    month: int,
    data_source: DataSource = DataSource.MMSDM,
) -> str | None:
    """Extract CSV file from MMSDM ZIP file and rename to standardized format.

    Args:
        zip_path: Path to the ZIP file
        output_dir: Directory to extract to
        table_name: Name of the table
        year: Year
        month: Month
        data_source: Data source type for determining filename format

    Returns:
        str: Path to the extracted CSV file or None if extraction failed

    """
    try:
        # Get the filename in the zip (it depends on the format)
        if data_source == DataSource.MMSDM_PREDISP:
            filename = build_mmsdm_predisp_filename(table_name, year, month)
        else:
            filename = build_mmsdm_filename(table_name, year, month)
        filename = filename.replace("%23", "#")  # Replace URL encoding with actual #
        csv_in_zip = f"{filename}.CSV"

        # Define output CSV filename
        month_str = f"{year}{month:02d}"
        csvname = os.path.join(output_dir, f"{table_name}_{month_str}.csv")

        # Check if output file already exists
        if os.path.exists(csvname):
            logger.info(f"Output file {csvname} already exists")
            return csvname

        # Extract from ZIP
        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            # Check if the expected file exists in the ZIP
            file_list = zip_ref.namelist()
            if csv_in_zip not in file_list:
                logger.error(f"Expected file {csv_in_zip} not found in {zip_path}")
                logger.debug(f"Files in ZIP: {file_list}")
                return None

            # Extract the file
            zip_ref.extract(csv_in_zip, output_dir)
            extracted_path = os.path.join(output_dir, csv_in_zip)

            # Rename to standardized name
            os.rename(extracted_path, csvname)
            logger.info(f"Extracted and renamed to {csvname}")

            return csvname

    except zipfile.BadZipFile as e:
        logger.error(f"Bad zip file {zip_path}: {e}")
    except Exception as e:
        logger.error(f"Error extracting {zip_path}: {e}")

    return None


def read_mmsdm_csv(csv_path: str) -> pd.DataFrame:
    """Read MMSDM CSV file with appropriate handling of format.

    MMSDM CSV files typically have:
    - A metadata header on line 1
    - Data rows
    - A count summary on the last line

    Args:
        csv_path: Path to the CSV file

    Returns:
        DataFrame: Data from the CSV file

    """
    try:
        # Skip first line (header) and last line (footer)
        df = pd.read_csv(csv_path, skiprows=1, skipfooter=1, engine="python")

        # Apply specific processing for datetime columns
        date_columns = [
            "SETTLEMENTDATE",
            "DATETIME",
            "INTERVAL_DATETIME",
            "RUN_DATETIME",
            "PREDISPATCH_RUN_DATETIME",
        ]

        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce")

        return df

    except Exception as e:
        logger.error(f"Error reading MMSDM CSV {csv_path}: {e}")
        return pd.DataFrame()


def combine_mmsdm_files(file_paths: list[str]) -> pd.DataFrame:
    """Combine multiple MMSDM CSV files into a single DataFrame.

    Args:
        file_paths: List of paths to MMSDM CSV files

    Returns:
        DataFrame: Combined data from all files

    """
    if not file_paths:
        return pd.DataFrame()

    all_data = []

    for file_path in file_paths:
        if not os.path.exists(file_path):
            logger.warning(f"File does not exist: {file_path}")
            continue

        if not file_path.endswith(".csv"):
            logger.warning(f"Not a CSV file: {file_path}")
            continue

        df = read_mmsdm_csv(file_path)
        if not df.empty:
            all_data.append(df)

    if not all_data:
        logger.warning("No valid data found in any of the input files")
        return pd.DataFrame()

    # Combine all dataframes
    result = pd.concat(all_data, ignore_index=True)

    # Sort by datetime column if it exists
    for date_col in ["SETTLEMENTDATE", "DATETIME", "INTERVAL_DATETIME"]:
        if date_col in result.columns:
            result = result.sort_values(date_col)
            break

    return result


def filter_mmsdm_data(
    df: pd.DataFrame,
    start_date: pd.Timestamp,
    end_date: pd.Timestamp,
    regions: list[str] | None = None,
) -> pd.DataFrame:
    """Filter MMSDM data by date range and regions.

    Args:
        df: Input DataFrame
        start_date: Start date
        end_date: End date
        regions: List of regions to include (optional)

    Returns:
        DataFrame: Filtered data

    """
    if df.empty:
        return df

    # Find appropriate date column
    date_cols = ["SETTLEMENTDATE", "DATETIME", "INTERVAL_DATETIME", "RUN_DATETIME"]
    date_col = None

    for col in date_cols:
        if col in df.columns:
            date_col = col
            break

    # Filter by date if date column found
    if date_col:
        df = df[(df[date_col] >= start_date) & (df[date_col] <= end_date)]
    else:
        logger.warning("No date column found for filtering")

    # Filter by region if needed
    if regions:
        region_cols = ["REGIONID", "REGION"]
        region_col = None

        for col in region_cols:
            if col in df.columns:
                region_col = col
                break

        if region_col:
            df = df[df[region_col].isin([r.upper() for r in regions])]
        else:
            logger.warning("No region column found for filtering")

    return df


def get_table_metadata(table_name: str) -> tuple[list[str], list[str]]:
    """Get metadata about an MMSDM table.

    This is a mapping of of important columns including primary
    key columns and important value columns.
    Foreach table type based on AEMO's MMS Data Model documentation

    Args:
        table_name: Name of the MMSDM table

    Returns:
        tuple: (key_columns, value_columns)

    """
    table_metadata = {
        "DISPATCHPRICE": {
            "key_columns": ["SETTLEMENTDATE", "REGIONID"],
            "value_columns": ["RRP", "LOSS_FACTOR", "LASTCHANGED"],
        },
        "DISPATCHREGIONSUM": {
            "key_columns": ["SETTLEMENTDATE", "REGIONID"],
            "value_columns": ["TOTALDEMAND", "AVAILABLEGENERATION", "LASTCHANGED"],
        },
        "DISPATCH_UNIT_SCADA": {
            "key_columns": ["SETTLEMENTDATE", "DUID"],
            "value_columns": ["SCADAVALUE", "LASTCHANGED"],
        },
        "DISPATCHLOAD": {
            "key_columns": ["SETTLEMENTDATE", "DUID"],
            "value_columns": [
                "INITIALMW",
                "TOTALCLEARED",
                "RAMPDOWNRATE",
                "RAMPUPRATE",
                "LASTCHANGED",
            ],
        },
        "DISPATCHINTERCONNECTORRES": {
            "key_columns": ["SETTLEMENTDATE", "INTERCONNECTORID"],
            "value_columns": ["MWFLOW", "METEREDMWFLOW", "LASTCHANGED"],
        },
        "BIDDAYOFFER_D": {
            "key_columns": ["SETTLEMENTDATE", "DUID"],
            "value_columns": [
                "PRICEBAND1",
                "PRICEBAND2",
                "PRICEBAND3",
                "PRICEBAND4",
                "PRICEBAND5",
                "PRICEBAND6",
                "PRICEBAND7",
                "PRICEBAND8",
                "PRICEBAND9",
                "PRICEBAND10",
                "LASTCHANGED",
            ],
        },
        "PREDISPATCHPRICE": {
            "key_columns": ["DATETIME", "REGIONID", "PREDISPATCH_RUN_DATETIME"],
            "value_columns": ["RRP", "LASTCHANGED"],
        },
        "PREDISPATCHREGIONSUM": {
            "key_columns": ["DATETIME", "REGIONID", "PREDISPATCH_RUN_DATETIME"],
            "value_columns": ["TOTALDEMAND", "AVAILABLEGENERATION", "LASTCHANGED"],
        },
        "P5MIN_REGIONSOLUTION": {
            "key_columns": ["INTERVAL_DATETIME", "REGIONID", "RUN_DATETIME"],
            "value_columns": ["RRP", "TOTALDEMAND", "LASTCHANGED"],
        },
        # Default for any table not specifically defined
        "DEFAULT": {
            "key_columns": ["SETTLEMENTDATE"],
            "value_columns": ["LASTCHANGED"],
        },
    }

    if table_name in table_metadata:
        return (
            table_metadata[table_name]["key_columns"],
            table_metadata[table_name]["value_columns"],
        )
    else:
        return (
            table_metadata["DEFAULT"]["key_columns"],
            table_metadata["DEFAULT"]["value_columns"],
        )
