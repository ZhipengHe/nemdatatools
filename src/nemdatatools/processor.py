"""NEMDataTools - Data processing utilities for AEMO data.

This module provides functions for processing and standardizing
data retrieved from AEMO.
"""

import logging

import numpy as np
import pandas as pd

from nemdatatools.data_source import DATA_CONFIG, DataSource

logger = logging.getLogger(__name__)


def standardize(data: pd.DataFrame, data_type: str) -> pd.DataFrame:
    """Standardize raw AEMO data.

    Args:
        data: Raw DataFrame
        data_type: Type of data

    Returns:
        Standardized DataFrame

    """
    if data.empty:
        logger.warning("Empty DataFrame provided for standardization")
        return data

    # Create a copy to avoid modifying the input
    df = data.copy()

    # Apply specific standardization based on data type
    if data_type not in DATA_CONFIG:
        logger.warning(
            f"Unknown data type: {data_type}, applying general standardization",
        )
        return _standardize_general(df)

    # Get the data source type
    source = DATA_CONFIG[data_type]["source"]

    # Apply standardization based on data source and type
    if source == DataSource.MMSDM:
        if data_type == "DISPATCHPRICE":
            df = _standardize_dispatch_price(df)
        elif data_type == "DISPATCHREGIONSUM":
            df = _standardize_dispatch_region_sum(df)
        elif data_type == "DISPATCH_UNIT_SCADA":
            df = _standardize_dispatch_unit_scada(df)
        elif data_type == "DISPATCHLOAD":
            df = _standardize_dispatch_load(df)
        elif data_type == "DISPATCHINTERCONNECTORRES":
            df = _standardize_dispatch_interconnector_res(df)
        elif data_type == "BIDDAYOFFER_D":
            df = _standardize_bid_day_offer(df)
        elif data_type == "DUDETAILSUMMARY":
            df = _standardize_du_detail_summary(df)
        else:
            # Apply general MMSDM standardization
            df = _standardize_mmsdm_general(df)

    elif source == DataSource.MMSDM_PREDISP:
        if data_type == "PREDISPATCHPRICE":
            df = _standardize_predispatch_price(df)
        elif data_type == "PREDISPATCHREGIONSUM":
            df = _standardize_predispatch_region_sum(df)
        elif data_type == "PREDISPATCHLOAD":
            df = _standardize_predispatch_load(df)
        else:
            # Apply general predispatch standardization
            df = _standardize_predispatch_general(df)

    elif source == DataSource.MMSDM_P5MIN:
        if data_type == "P5MIN_REGIONSOLUTION":
            df = _standardize_p5min_region_solution(df)
        elif data_type == "P5MIN_INTERCONNECTORSOLN":
            df = _standardize_p5min_interconnector_soln(df)
        else:
            # Apply general P5MIN standardization
            df = _standardize_p5min_general(df)

    elif source == DataSource.PRICE_AND_DEMAND:
        df = _standardize_price_and_demand(df)

    elif source == DataSource.STATIC:
        if data_type == "NEM_REG_AND_EXEMPTION":
            df = _standardize_nem_reg_and_exemption(df)
        elif data_type == "REGION_BOUNDARIES":
            df = _standardize_region_boundaries(df)
        else:
            # Apply general static data standardization
            df = _standardize_static_general(df)

    else:
        logger.warning(
            f"Unknown data source: {source}, applying general standardization",
        )
        df = _standardize_general(df)

    return df


def _standardize_general(df: pd.DataFrame) -> pd.DataFrame:
    """Apply general standardization to any AEMO data.

    Args:
        df: Raw DataFrame

    Returns:
        Standardized DataFrame

    """
    # Handle common date columns
    date_columns = [
        "SETTLEMENTDATE",
        "DATETIME",
        "INTERVAL_DATETIME",
        "RUN_DATETIME",
        "PREDISPATCH_RUN_DATETIME",
        "LASTCHANGED",
    ]

    for col in date_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    # Handle numeric columns
    numeric_columns = [
        "RRP",
        "TOTALDEMAND",
        "AVAILABLEGENERATION",
        "FORECASTED_DEMAND",
        # "PRICE", # This is a generic name, not specific to any data type
        "DEMAND",
        "SCADAVALUE",
        "INITIALMW",
        "TOTALCLEARED",
        "MWFLOW",
        "METEREDMWFLOW",
        "CAPACITY",
    ]

    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Remove duplicates if present
    df = df.drop_duplicates()

    return df


def _standardize_mmsdm_general(df: pd.DataFrame) -> pd.DataFrame:
    """Apply general standardization to MMSDM data.

    Args:
        df: Raw DataFrame

    Returns:
        Standardized DataFrame

    """
    # Apply general standardization first
    df = _standardize_general(df)

    # Handle MMSDM-specific fields
    if "LASTCHANGED" in df.columns:
        df["LASTCHANGED"] = pd.to_datetime(df["LASTCHANGED"], errors="coerce")

    # Set index to settlementdate if present
    if "SETTLEMENTDATE" in df.columns:
        df = df.set_index("SETTLEMENTDATE").sort_index()

    return df


def _standardize_dispatch_price(df: pd.DataFrame) -> pd.DataFrame:
    """Standardize DISPATCHPRICE data.

    Args:
        df: Raw DataFrame

    Returns:
        Standardized DataFrame

    """
    # Apply general MMSDM standardization
    df = _standardize_mmsdm_general(df)

    # Handle specific columns for this data type
    if "RRP" in df.columns:
        # Clean RRP values
        df["RRP"] = pd.to_numeric(df["RRP"], errors="coerce")

        # Cap values at market price cap if necessary
        # Note: 15,100 is the 2023 market price cap
        price_cap = 15100
        price_floor = -1000
        df.loc[df["RRP"] > price_cap, "RRP"] = price_cap
        df.loc[df["RRP"] < price_floor, "RRP"] = price_floor

    return df


def _standardize_dispatch_region_sum(df: pd.DataFrame) -> pd.DataFrame:
    """Standardize DISPATCHREGIONSUM data.

    Args:
        df: Raw DataFrame

    Returns:
        Standardized DataFrame

    """
    # Apply general MMSDM standardization
    df = _standardize_mmsdm_general(df)

    # Handle specific columns for this data type
    demand_columns = ["TOTALDEMAND", "DEMAND"]
    for col in demand_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

            # Remove negative demand values (likely errors)
            df.loc[df[col] < 0, col] = np.nan

    return df


def _standardize_dispatch_unit_scada(df: pd.DataFrame) -> pd.DataFrame:
    """Standardize DISPATCH_UNIT_SCADA data.

    Args:
        df: Raw DataFrame

    Returns:
        Standardized DataFrame

    """
    # Apply general MMSDM standardization
    df = _standardize_mmsdm_general(df)

    # Set multi-index if scadavalue present
    if "SCADAVALUE" in df.columns and "DUID" in df.columns:
        df["SCADAVALUE"] = pd.to_numeric(df["SCADAVALUE"], errors="coerce")
        if df.index.name == "SETTLEMENTDATE":
            df = df.reset_index()
        # Now set the multi-index
        df = df.set_index(["SETTLEMENTDATE", "DUID"]).sort_index()

    return df


def _standardize_dispatch_load(df: pd.DataFrame) -> pd.DataFrame:
    """Standardize DISPATCHLOAD data.

    Args:
        df: Raw DataFrame

    Returns:
        Standardized DataFrame

    """
    # Apply general MMSDM standardization
    df = _standardize_mmsdm_general(df)

    # Handle specific columns for this data type
    mw_columns = ["INITIALMW", "TOTALCLEARED"]
    for col in mw_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    rate_columns = ["RAMPUPRATE", "RAMPDOWNRATE"]
    for col in rate_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Set multi-index if duid present
    if "DUID" in df.columns:
        # Reset index if it's already set
        if df.index.name:
            df = df.reset_index()
        df = df.set_index(["SETTLEMENTDATE", "DUID"]).sort_index()

    return df


def _standardize_dispatch_interconnector_res(df: pd.DataFrame) -> pd.DataFrame:
    """Standardize DISPATCHINTERCONNECTORRES data.

    Args:
        df: Raw DataFrame

    Returns:
        Standardized DataFrame

    """
    # Apply general MMSDM standardization
    df = _standardize_mmsdm_general(df)

    # Handle specific columns for this data type
    flow_columns = ["MWFLOW", "METEREDMWFLOW"]
    for col in flow_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Set multi-index if interconnectorid present
    if "INTERCONNECTORID" in df.columns:
        # Reset index if it's already set
        if df.index.name:
            df = df.reset_index()
        df = df.set_index(["SETTLEMENTDATE", "INTERCONNECTORID"]).sort_index()

    return df


def _standardize_bid_day_offer(df: pd.DataFrame) -> pd.DataFrame:
    """Standardize BIDDAYOFFER_D data.

    Args:
        df: Raw DataFrame

    Returns:
        Standardized DataFrame

    """
    # Apply general MMSDM standardization
    df = _standardize_mmsdm_general(df)

    # Handle specific columns for this data type
    # Convert all priceband columns to numeric
    for i in range(1, 11):
        col = f"PRICEBAND{i}"
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Set multi-index if appropriate columns present
    if "DUID" in df.columns and "BIDTYPE" in df.columns:
        # Reset index if it's already set
        if df.index.name:
            df = df.reset_index()
        df = df.set_index(["SETTLEMENTDATE", "DUID", "BIDTYPE"]).sort_index()

    return df


def _standardize_du_detail_summary(df: pd.DataFrame) -> pd.DataFrame:
    """Standardize DUDETAILSUMMARY data.

    Args:
        df: Raw DataFrame

    Returns:
        Standardized DataFrame

    """
    # Apply general MMSDM standardization
    df = _standardize_mmsdm_general(df)

    # Handle specific columns for this data type
    if "MAXCAPACITY" in df.columns:
        df["MAXCAPACITY"] = pd.to_numeric(df["MAXCAPACITY"], errors="coerce")

    if "STARTTYPE" in df.columns:
        # Convert starttype to uppercase for consistency
        df["STARTTYPE"] = df["STARTTYPE"].str.upper()

    # Set duid as index if not already indexed
    if "DUID" in df.columns and not df.index.name:
        df = df.set_index("DUID")

    return df


def _standardize_predispatch_general(df: pd.DataFrame) -> pd.DataFrame:
    """Apply general standardization to PREDISPATCH data.

    Args:
        df: Raw DataFrame

    Returns:
        Standardized DataFrame

    """
    # Apply general standardization
    df = _standardize_general(df)

    # Handle PREDISPATCH-specific fields
    if "LASTCHANGED" in df.columns:
        df["LASTCHANGED"] = pd.to_datetime(df["LASTCHANGED"], errors="coerce")

    # Set multi-index for forecasted time and run time if present
    if "DATETIME" in df.columns and "PREDISPATCH_RUN_DATETIME" in df.columns:
        if not df.index.name:  # Only set if not already indexed
            df = df.set_index(["PREDISPATCH_RUN_DATETIME", "DATETIME"]).sort_index()

        # Calculate forecast horizon if both datetime columns are present
        df["FORECAST_HORIZON_HOURS"] = (
            df.index.get_level_values("DATETIME")
            - df.index.get_level_values("PREDISPATCH_RUN_DATETIME")
        ).total_seconds() / 3600

    return df


def _standardize_predispatch_price(df: pd.DataFrame) -> pd.DataFrame:
    """Standardize PREDISPATCHPRICE data.

    Args:
        df: Raw DataFrame

    Returns:
        Standardized DataFrame

    """
    # Apply general predispatch standardization
    df = _standardize_predispatch_general(df)

    # Handle specific columns for this data type
    if "RRP" in df.columns:
        df["RRP"] = pd.to_numeric(df["RRP"], errors="coerce")

        # Cap values at market price cap if necessary
        price_cap = 15100
        price_floor = -1000
        df.loc[df["RRP"] > price_cap, "RRP"] = price_cap
        df.loc[df["RRP"] < price_floor, "RRP"] = price_floor

    # Add REGIONID to index if present
    if "REGIONID" in df.columns and df.index.names and "REGIONID" not in df.index.names:
        df = (
            df.reset_index()
            .set_index(["PREDISPATCH_RUN_DATETIME", "DATETIME", "REGIONID"])
            .sort_index()
        )

    return df


def _standardize_predispatch_region_sum(df: pd.DataFrame) -> pd.DataFrame:
    """Standardize PREDISPATCHREGIONSUM data.

    Args:
        df: Raw DataFrame

    Returns:
        Standardized DataFrame

    """
    # Apply general predispatch standardization
    df = _standardize_predispatch_general(df)

    # Handle specific columns for this data type
    demand_columns = ["TOTALDEMAND", "DEMAND"]
    for col in demand_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

            # Remove negative demand values (likely errors)
            df.loc[df[col] < 0, col] = np.nan

    # Add REGIONID to index if present
    if "REGIONID" in df.columns and df.index.names and "REGIONID" not in df.index.names:
        df = (
            df.reset_index()
            .set_index(["PREDISPATCH_RUN_DATETIME", "DATETIME", "REGIONID"])
            .sort_index()
        )

    return df


def _standardize_predispatch_load(df: pd.DataFrame) -> pd.DataFrame:
    """Standardize PREDISPATCHLOAD data.

    Args:
        df: Raw DataFrame

    Returns:
        Standardized DataFrame

    """
    # Apply general predispatch standardization
    df = _standardize_predispatch_general(df)

    # Handle specific columns for this data type
    mw_columns = ["INITIALMW", "TOTALCLEARED"]
    for col in mw_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Add duid to index if present
    if "DUID" in df.columns and df.index.names and "DUID" not in df.index.names:
        df = (
            df.reset_index()
            .set_index(["PREDISPATCH_RUN_DATETIME", "DATETIME", "DUID"])
            .sort_index()
        )

    return df


def _standardize_p5min_general(df: pd.DataFrame) -> pd.DataFrame:
    """Apply general standardization to P5MIN data.

    Args:
        df: Raw DataFrame

    Returns:
        Standardized DataFrame

    """
    # Apply general standardization
    df = _standardize_general(df)

    # Handle P5MIN-specific fields
    if "LASTCHANGED" in df.columns:
        df["LASTCHANGED"] = pd.to_datetime(df["LASTCHANGED"], errors="coerce")

    # Set multi-index for forecasted time and run time if present
    if "INTERVAL_DATETIME" in df.columns and "RUN_DATETIME" in df.columns:
        if not df.index.name:  # Only set if not already indexed
            df = df.set_index(["RUN_DATETIME", "INTERVAL_DATETIME"]).sort_index()

        # Calculate forecast horizon if both datetime columns are present
        df["FORECAST_HORIZON_MINUTES"] = (
            df.index.get_level_values("INTERVAL_DATETIME")
            - df.index.get_level_values("RUN_DATETIME")
        ).total_seconds() / 60

    return df


def _standardize_p5min_region_solution(df: pd.DataFrame) -> pd.DataFrame:
    """Standardize P5MIN_REGIONSOLUTION data.

    Args:
        df: Raw DataFrame

    Returns:
        Standardized DataFrame

    """
    # Apply general P5MIN standardization
    df = _standardize_p5min_general(df)

    # Handle specific columns for this data type
    if "RRP" in df.columns:
        df["RRP"] = pd.to_numeric(df["RRP"], errors="coerce")

        # Cap values at market price cap if necessary
        price_cap = 15100
        price_floor = -1000
        df.loc[df["RRP"] > price_cap, "RRP"] = price_cap
        df.loc[df["RRP"] < price_floor, "RRP"] = price_floor

    if "TOTALDEMAND" in df.columns:
        df["TOTALDEMAND"] = pd.to_numeric(df["TOTALDEMAND"], errors="coerce")

        # Remove negative demand values (likely errors)
        df.loc[df["TOTALDEMAND"] < 0, "TOTALDEMAND"] = np.nan

    # Add REGIONID to index if present
    if "REGIONID" in df.columns and df.index.names and "REGIONID" not in df.index.names:
        df = (
            df.reset_index()
            .set_index(["RUN_DATETIME", "INTERVAL_DATETIME", "REGIONID"])
            .sort_index()
        )

    return df


def _standardize_p5min_interconnector_soln(df: pd.DataFrame) -> pd.DataFrame:
    """Standardize P5MIN_INTERCONNECTORSOLN data.

    Args:
        df: Raw DataFrame

    Returns:
        Standardized DataFrame

    """
    # Apply general P5MIN standardization
    df = _standardize_p5min_general(df)

    # Handle specific columns for this data type
    flow_columns = [
        "FLOW",
        "METEREDFLOW",
        "LIMITRESULT",
    ]  # Note: may differ from dispatch field names
    for col in flow_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Add interconnectorid to index if present
    if (
        "INTERCONNECTORID" in df.columns
        and df.index.names
        and "INTERCONNECTORID" not in df.index.names
    ):
        df = (
            df.reset_index()
            .set_index(["RUN_DATETIME", "INTERVAL_DATETIME", "INTERCONNECTORID"])
            .sort_index()
        )

    return df


def _standardize_price_and_demand(df: pd.DataFrame) -> pd.DataFrame:
    """Standardize PRICE_AND_DEMAND data.

    Args:
        df: Raw DataFrame

    Returns:
        Standardized DataFrame

    """
    # Apply general standardization
    df = _standardize_general(df)

    # Handle PRICE_AND_DEMAND-specific fields
    if "SETTLEMENTDATE" in df.columns:
        # Ensure settlementdate is datetime
        df["SETTLEMENTDATE"] = pd.to_datetime(df["SETTLEMENTDATE"], errors="coerce")

        # Set index to settlementdate
        if not df.index.name:
            df = df.set_index("SETTLEMENTDATE").sort_index()

    # Process price and demand columns
    if "RRP" in df.columns:
        df["RRP"] = pd.to_numeric(df["RRP"], errors="coerce")

        # Cap values at market price cap if necessary
        price_cap = 15100
        price_floor = -1000
        df.loc[df["RRP"] > price_cap, "RRP"] = price_cap
        df.loc[df["RRP"] < price_floor, "RRP"] = price_floor

    if "TOTALDEMAND" in df.columns:
        df["TOTALDEMAND"] = pd.to_numeric(df["TOTALDEMAND"], errors="coerce")

        # Remove negative demand values (likely errors)
        df.loc[df["TOTALDEMAND"] < 0, "TOTALDEMAND"] = np.nan

    # Parse PERIODTYPE if present
    if "PERIODTYPE" in df.columns:
        # Ensure consistent case
        df["PERIODTYPE"] = df["PERIODTYPE"].str.upper()

    return df


def _standardize_static_general(df: pd.DataFrame) -> pd.DataFrame:
    """Apply general standardization to static reference data.

    Args:
        df: Raw DataFrame

    Returns:
        Standardized DataFrame

    """
    # Apply general standardization
    df = _standardize_general(df)

    # Clean up column names - remove spaces, special chars
    df.columns = [col.strip().replace(" ", "_").replace("-", "_") for col in df.columns]

    # Remove duplicates
    df = df.drop_duplicates()

    return df


def _standardize_nem_reg_and_exemption(df: pd.DataFrame) -> pd.DataFrame:
    """Standardize NEM_REG_AND_EXEMPTION data.

    Args:
        df: Raw DataFrame

    Returns:
        Standardized DataFrame

    """
    # Apply general static data standardization
    df = _standardize_static_general(df)

    # Handle specific columns that might be present
    # Convert capacity to numeric if present
    if "CAPACITY" in df.columns:
        df["CAPACITY"] = pd.to_numeric(df["CAPACITY"], errors="coerce")

    # Standardize station names and participant names if present
    for col in ["STATION_NAME", "PARTICIPANT_NAME", "DISPATCH_TYPE"]:
        if col in df.columns:
            # Convert to title case for consistency
            df[col] = df[col].str.title()

    # Handle classification if present
    if "CLASSIFICATION" in df.columns:
        # Ensure uppercase for consistency
        df["CLASSIFICATION"] = df["CLASSIFICATION"].str.upper()

    return df


def _standardize_region_boundaries(df: pd.DataFrame) -> pd.DataFrame:
    """Standardize REGION_BOUNDARIES data.

    Args:
        df: Raw DataFrame

    Returns:
        Standardized DataFrame

    """
    # Apply general static data standardization
    df = _standardize_static_general(df)

    # Process any date columns
    date_columns = [col for col in df.columns if "DATE" in col]
    for col in date_columns:
        df[col] = pd.to_datetime(df[col], errors="coerce")

    return df


def filter_by_regions(df: pd.DataFrame, regions: list[str]) -> pd.DataFrame:
    """Filter DataFrame to include only specified regions.

    Args:
        df: DataFrame to filter
        regions: List of region codes to include

    Returns:
        Filtered DataFrame

    """
    region_cols = ["REGIONID", "REGION"]

    for col in region_cols:
        if col in df.columns:
            return df[df[col].str.upper().isin([r.upper() for r in regions])]

    logger.warning("No region column found in DataFrame")
    return df


def calculate_price_statistics(
    price_data: pd.DataFrame,
    interval: str = "1D",
) -> pd.DataFrame:
    """Calculate price statistics over specified interval.

    Args:
        price_data: Price DataFrame with columns including 'RRP'
        interval: Resampling interval (default: daily)

    Returns:
        DataFrame with price statistics

    """
    if "RRP" not in price_data.columns:
        logger.error("No 'RRP' column found in price data")
        return pd.DataFrame()

    # Ensure price_data has a datetime index
    if not isinstance(price_data.index, pd.DatetimeIndex):
        date_cols = ["SETTLEMENTDATE", "DATETIME", "INTERVAL_DATETIME"]
        date_col = None
        for col in date_cols:
            if col in price_data.columns:
                date_col = col
                break

        if date_col:
            price_data = price_data.set_index(date_col)
        else:
            logger.error("Cannot calculate statistics: No datetime index or column")
            return pd.DataFrame()

    agg_funcs = [
        ("min", "min"),
        ("max", "max"),
        ("mean", "mean"),
        ("median", "median"),
        ("std", "std"),
        ("count", "count"),
    ]

    # Calculate statistics
    region_col = None
    for col in ["REGIONID", "REGION"]:
        if col in price_data.columns:
            region_col = col
            break

    if region_col:
        stats = price_data.groupby(region_col)["RRP"].resample(interval).agg(agg_funcs)
        # Flatten MultiIndex columns and rename
        stats.columns = [f"RRP_{col[1].upper()}" for col in stats.columns]
    else:
        stats = price_data["RRP"].resample(interval).agg(agg_funcs)
        stats.columns = [f"RRP_{col.upper()}" for col in stats.columns]

    return stats.reset_index()


def calculate_demand_statistics(
    demand_data: pd.DataFrame,
    interval: str = "1D",
) -> pd.DataFrame:
    """Calculate demand statistics over specified interval.

    Args:
        demand_data: Demand DataFrame with columns including 'totaldemand'
        interval: Resampling interval (default: daily)

    Returns:
        DataFrame with demand statistics

    """
    demand_col = None
    for col in ["TOTALDEMAND", "DEMAND"]:
        if col in demand_data.columns:
            demand_col = col
            break

    if demand_col is None:
        logger.error("No demand column found in demand data")
        return pd.DataFrame()

    # Ensure demand_data has a datetime index
    if not isinstance(demand_data.index, pd.DatetimeIndex):
        date_cols = ["SETTLEMENTDATE", "DATETIME", "INTERVAL_DATETIME"]
        date_col = None
        for col in date_cols:
            if col in demand_data.columns:
                date_col = col
                break

        if date_col:
            demand_data = demand_data.set_index(date_col)
        else:
            logger.error("Cannot calculate statistics: No datetime index or column")
            return pd.DataFrame()

    agg_funcs = [
        ("min", "min"),
        ("max", "max"),
        ("mean", "mean"),
        ("median", "median"),
        ("std", "std"),
        ("count", "count"),
    ]

    # Calculate statistics
    region_col = None
    for col in ["REGIONID", "REGION"]:
        if col in demand_data.columns:
            region_col = col
            break

    if region_col:
        stats = (
            demand_data.groupby(region_col)[demand_col]
            .resample(interval)
            .agg(agg_funcs)
        )
        # Flatten MultiIndex columns and rename
        stats.columns = [f"{demand_col}_{col[1].upper()}" for col in stats.columns]
    else:
        stats = demand_data[demand_col].resample(interval).agg(agg_funcs)
        # Rename columns
        stats.columns = [f"{demand_col}_{col.upper()}" for col in stats.columns]

    return stats.reset_index()


def merge_datasets(
    datasets: list[pd.DataFrame],
    on: list[str] | None = None,
    how: str = "outer",
) -> pd.DataFrame:
    """Merge multiple datasets into one.

    Args:
        datasets: List of DataFrames to merge
        on: Columns to merge on
        how: Merge method (inner, outer, left, right)

    Returns:
        Merged DataFrame

    """
    if not datasets:
        return pd.DataFrame()

    if len(datasets) == 1:
        return datasets[0]

    # Start with the first dataset
    result = datasets[0]

    # Merge with each subsequent dataset
    for df in datasets[1:]:
        result = pd.merge(result, df, on=on, how=how)

    return result
