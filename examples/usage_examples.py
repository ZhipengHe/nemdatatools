"""Example usage of NEMDataTools.

This script demonstrates how to use the NEMDataTools package to download
and process data from AEMO.
"""

import os

import matplotlib.pyplot as plt
import pandas as pd

import nemdatatools as ndt


def example_1_basic_download() -> None:
    """Download price data for a specific period.

    This example demonstrates the basic usage of the download functionality.
    """
    print("Example 1: Basic download of dispatch price data")

    # Download price data for a specific period
    data = ndt.fetch_data(
        data_type="DISPATCHPRICE",
        start_date="2023/01/01",
        end_date="2023/01/07",
        regions=["NSW1", "VIC1"],
        cache_path="./cache",
    )
    print(data.columns)
    data.to_csv("./examples/dispatch_price_example.csv")

    print(f"Downloaded {len(data)} records")
    print(data.head())

    # Plot the data
    plt.figure(figsize=(12, 6))
    for region in ["NSW1", "VIC1"]:
        region_data = data[data["REGIONID"] == region]
        plt.plot(region_data.index, region_data["RRP"], label=region)

    plt.title("Dispatch Price: Jan 1-7, 2023")
    plt.xlabel("Settlement Date")
    plt.ylabel("RRP ($/MWh)")
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.savefig("./examples/dispatch_price_example.png")
    print("Plot saved to dispatch_price_example.png")


def example_2_price_and_demand() -> None:
    """Download and analyze price and demand data.

    This example demonstrates working with both price and demand datasets.
    """
    print("\nExample 2: Price and demand analysis")

    # Download price and demand data
    data = ndt.fetch_data(
        data_type="PRICE_AND_DEMAND",
        start_date="2023/01/01",
        end_date="2023/01/31",
        regions=["NSW1"],
        cache_path="./cache",
    )

    print(f"Downloaded {len(data)} records")

    # Calculate daily statistics
    daily_stats = data.groupby(data.index).agg(
        {"RRP": ["min", "max", "mean"], "TOTALDEMAND": ["min", "max", "mean"]},
    )

    print("\nDaily Price and Demand Statistics:")
    print(daily_stats.head())

    # Create a scatter plot of price vs demand
    plt.figure(figsize=(10, 6))
    plt.scatter(data["TOTALDEMAND"], data["RRP"], alpha=0.5)
    plt.title("NSW Price vs Demand: January 2023")
    plt.xlabel("Total Demand (MW)")
    plt.ylabel("Regional Reference Price ($/MWh)")
    plt.grid(True)
    plt.tight_layout()
    plt.savefig("./examples/price_vs_demand_example.png")
    print("Plot saved to price_vs_demand_example.png")


def example_3_batch_download() -> None:
    """Batch download multiple tables.

    This example demonstrates how to efficiently download multiple data tables
    in a batch.
    """
    print("\nExample 3: Batch download of multiple tables")

    # Download multiple tables for a specific period
    result = ndt.download_multiple_tables(
        table_names=["DISPATCHPRICE", "DISPATCHREGIONSUM"],
        start_date="2023/01/01",
        end_date="2023/03/01",
        cache_path="./cache",
    )

    print("Download results:")
    for table, df in result.items():
        if df is not None:
            print(f"  {table}: {len(df)} records downloaded")
        else:
            print(f"  {table}: download failed")


def example_4_parallel_yearly_download() -> None:
    """Download data for multiple years in parallel.

    This example demonstrates parallel processing for retrieving data across
    multiple years.
    """
    print("\nExample 4: Downloading data for multiple years")

    # Download data for multiple years
    result = ndt.download_yearly_data(
        years=[2022, 2023, 2024],
        tables=["PRICE_AND_DEMAND", "DISPATCHPRICE"],
        cache_path="./cache",
        max_workers=1,
    )

    print("\nYearly download results:")
    for year, year_result in result.items():
        print(f"\nYear {year}:")
        for table, df in year_result.items():
            if df is not None:
                print(f"  {table}: {len(df)} records downloaded")
            else:
                print(f"  {table}: download failed")


def example_5_process_downloaded_data() -> None:
    """Process previously downloaded data.

    This example demonstrates how to work with data that has already been downloaded.
    Need to be run after example_4.
    """
    print("\nExample 5: Processing previously downloaded data")

    # Load previously downloaded data
    data = ndt.fetch_data(
        data_type="PRICE_AND_DEMAND",
        start_date="2023/01/01",
        end_date="2023/01/31",
        regions=["NSW1", "VIC1"],
        cache_path="./cache",
    )

    if data is not None:
        print(f"Loaded {len(data)} records from previously downloaded files")

        # Verify data structure before processing (case-insensitive)
        required_columns = {"RRP", "TOTALDEMAND"}
        data_columns = {
            col.upper(): col for col in data.columns
        }  # Map uppercase to original

        # Check for required columns
        missing = [col for col in required_columns if col not in data_columns]
        if missing:
            print(f"\nWarning: Missing required columns: {missing}")
            print(f"Available columns: {list(data.columns)}")
            return

        # Find region column (supports both REGION and REGIONID)
        region_col = next(
            (
                data_columns[col]
                for col in ["REGION", "REGIONID"]
                if col in data_columns
            ),
            None,
        )

        # Calculate and print price statistics
        try:
            price_stats = ndt.calculate_price_statistics(data)
            if not price_stats.empty:
                print("\nPrice Statistics:")
                # Handle column names safely
                if hasattr(price_stats.columns, "levels"):
                    price_stats.columns = [
                        "_".join(map(str, col)).upper()
                        for col in price_stats.columns.values
                    ]
                print(price_stats.head())
        except Exception as e:
            print(f"\nError calculating price statistics: {e!s}")

        # Calculate and print demand statistics
        try:
            demand_stats = ndt.calculate_demand_statistics(data)
            if not demand_stats.empty:
                print("\nDemand Statistics:")
                # Handle column names safely
                if hasattr(demand_stats.columns, "levels"):
                    demand_stats.columns = [
                        "_".join(map(str, col)).upper()
                        for col in demand_stats.columns.values
                    ]
                print(demand_stats.head())
        except Exception as e:
            print(f"\nError calculating demand statistics: {e!s}")

        # Create plots only if we have region information
        if region_col:
            try:
                plt.figure(figsize=(12, 8))

                # Price plot
                plt.subplot(2, 1, 1)
                for region in ["NSW1", "VIC1"]:
                    region_data = data[data[region_col].str.upper() == region.upper()]
                    if not region_data.empty:
                        plt.plot(region_data.index, region_data["RRP"], label=region)
                plt.title("Price: Jan 1-31, 2023")
                plt.ylabel("RRP ($/MWh)")
                plt.legend()
                plt.grid(True)

                # Demand plot
                plt.subplot(2, 1, 2)
                for region in ["NSW1", "VIC1"]:
                    region_data = data[data[region_col].str.upper() == region.upper()]
                    if not region_data.empty:
                        plt.plot(
                            region_data.index,
                            region_data["TOTALDEMAND"],
                            label=region,
                        )
                plt.title("Demand: Jan 1-31, 2023")
                plt.xlabel("Settlement Date")
                plt.ylabel("Demand (MW)")
                plt.legend()
                plt.grid(True)

                plt.tight_layout()
                plt.savefig("./examples/price_and_demand_processed.png")
                print("Plot saved to price_and_demand_processed.png")
            except Exception as e:
                print(f"\nError creating plots: {e!s}")
        else:
            print(
                "\nSkipping plots - region column not found "
                "(looking for 'REGION' or 'REGIONID')",
            )
    else:
        print("No data found. Please run example_4 first.")


def example_6_time_windows(
    window_size_in_day: int = 14,
    step_size_in_min: int = 5,
    resample_interval: str = "5min",
) -> None:
    """Calculate sliding time windows for price data.

    This example demonstrates:
    - Fetching DISPATCHPRICE data for QLD1 for 2023
    - Creating 14-day sliding windows every 5 minutes
    - Counting the number of valid windows

    Args:
        window_size_in_day: Size of the sliding window in days
        step_size_in_min: Step size between windows in minutes
        resample_interval: Data resampling interval ('5min', '30min', or '1h')

    """
    print("\nExample 6: Time window analysis")

    # Fetch full year of QLD1 price data
    data = ndt.fetch_data(
        data_type="DISPATCHPRICE",
        start_date="2023/01/01",
        end_date="2023/12/31",
        regions=["QLD1"],
        cache_path="./cache",
    )

    if data is None or data.empty:
        print("Error: No data fetched")
        return

    # Ensure data is sorted chronologically
    data = data.sort_index()

    # Resample data to desired interval
    if resample_interval not in ["5min", "30min", "1h"]:
        print(
            "Error: Invalid resample_interval. Must be one of ['5min', '30min', '1h']",
        )
        return

    print(f"\nResampling data to {resample_interval} intervals")
    # Only resample numeric columns
    numeric_columns = data.select_dtypes(include=["float64", "int64"]).columns
    if len(numeric_columns) == 0:
        print("Error: No numeric columns found in the data")
        return

    # Create a copy of the data with only numeric columns for resampling
    numeric_data = data[numeric_columns].copy()
    resampled_numeric = numeric_data.resample(resample_interval).mean()

    # Get the first value of non-numeric columns for each resampled interval
    non_numeric_columns = data.select_dtypes(exclude=["float64", "int64"]).columns
    if len(non_numeric_columns) > 0:
        non_numeric_data = data[non_numeric_columns].resample(resample_interval).first()
        # Combine numeric and non-numeric data
        data = pd.concat([resampled_numeric, non_numeric_data], axis=1)
    else:
        data = resampled_numeric

    # Calculate window parameters
    window_size = pd.Timedelta(days=window_size_in_day)

    # Convert resample interval to minutes for comparison
    if resample_interval == "5min":
        interval_minutes = 5
    elif resample_interval == "30min":
        interval_minutes = 30
    else:  # 1h
        interval_minutes = 60

    # Use the larger of the resampling interval and requested step size
    step_size = pd.Timedelta(minutes=max(interval_minutes, step_size_in_min))

    # Verify index is datetime
    if not isinstance(data.index, pd.DatetimeIndex):
        print("Error: Data index is not datetime type")
        return

    # Calculate number of possible windows
    total_duration = data.index[-1] - data.index[0]
    num_windows = int((total_duration - window_size) / step_size) + 1

    # Calculate points per window based on resampling interval
    if resample_interval == "5min":
        points_per_window = window_size_in_day * 24 * 12  # 12 points per hour
    elif resample_interval == "30min":
        points_per_window = window_size_in_day * 24 * 2  # 2 points per hour
    else:  # 1h
        points_per_window = window_size_in_day * 24  # 1 point per hour

    print(f"\nTotal data points: {len(data)}")
    print(f"First date: {data.index[0]}")
    print(f"Last date: {data.index[-1]}")
    print(f"Window size: {window_size}")
    print(f"Step size: {step_size}")
    print(f"Number of possible windows: {num_windows}")
    print(f"Points per window: {points_per_window}")


def example_7_resample_and_window() -> None:
    """Resample DISPATCHPRICE data to 1-hour intervals and create 14-day windows.

    This example demonstrates:
    - Fetching DISPATCHPRICE data for QLD1
    - Resampling to 1-hour intervals
    - Creating 14-day windows for model training
    - Saving the processed data
    """
    print("\nExample 7: Resampling and windowing for model training")

    # Fetch full year of price data for QLD1
    data = ndt.fetch_data(
        data_type="DISPATCHPRICE",
        start_date="2023/01/01",
        end_date="2023/12/31",
        regions=["QLD1"],
        cache_path="./cache",
    )

    if data is None or data.empty:
        print("Error: No data fetched")
        return

    # Ensure data is sorted chronologically
    data = data.sort_index()

    print(f"\nOriginal data shape: {data.shape}")
    print(f"Original data frequency: {pd.infer_freq(data.index)}")
    print(f"Original data columns: {data.columns.tolist()}")
    print(f"Original data sample:\n{data.head()}")

    # Resample data to 1-hour intervals
    print("\nResampling data to 1-hour intervals...")
    resampled_data = ndt.resample_data(
        data,
        interval="1h",
        numeric_agg="mean",  # Use mean for numeric columns
        non_numeric_agg="first",  # Use first value for non-numeric columns
    )

    print(f"Resampled data shape: {resampled_data.shape}")
    print(f"Resampled data frequency: {pd.infer_freq(resampled_data.index)}")
    print(f"Resampled data columns: {resampled_data.columns.tolist()}")
    print(f"Resampled data sample:\n{resampled_data.head()}")

    # Get unique timestamps
    unique_times = resampled_data.index.unique()
    print(f"\nNumber of unique timestamps: {len(unique_times)}")
    print(f"First timestamp: {unique_times[0]}")
    print(f"Last timestamp: {unique_times[-1]}")

    # Check for missing values
    missing_values = resampled_data["RRP"].isnull().sum()
    print(f"\nNumber of missing RRP values: {missing_values}")
    if missing_values > 0:
        print("Sample of rows with missing values:")
        print(resampled_data[resampled_data["RRP"].isnull()].head())

    # Create 14-day windows
    print("\nCreating 14-day windows...")
    windows = ndt.create_time_windows(
        resampled_data,
        window_size_days=14,
        step_size_hours=1,
        check_column="RRP",  # Check for missing values in RRP column
    )

    print(f"\nTotal number of complete windows: {len(windows)}")

    # Save the first window as an example
    if windows:
        example_window = windows[0]
        print("\nExample window information:")
        print(f"Start time: {example_window.index[0]}")
        print(f"End time: {example_window.index[-1]}")
        print(f"Number of data points: {len(example_window)}")

        # Save the example window
        example_window.to_csv("./examples/example_window.csv")
        print("\nExample window saved to example_window.csv")

        # Create a plot of the example window
        plt.figure(figsize=(15, 8))
        plt.plot(example_window.index, example_window["RRP"])

        plt.title("14-Day Window of Hourly Prices - QLD1")
        plt.xlabel("Time")
        plt.ylabel("RRP ($/MWh)")
        plt.grid(True)
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.savefig("./examples/example_window.png")
        print("Plot saved to example_window.png")
    else:
        # If no complete windows, save a sample of the data for inspection
        print(
            "\nNo complete windows found. Saving sample data for inspection...",
        )
        resampled_data.head(1000).to_csv("./examples/resampled_data_sample.csv")
        print("Sample data saved to resampled_data_sample.csv")

    # Save all windows to a directory
    os.makedirs("./examples/windows", exist_ok=True)
    for i, window in enumerate(windows):
        window.to_csv(f"./examples/windows/window_{i:04d}.csv")

    print(f"\nAll {len(windows)} windows saved to ./examples/windows/")


if __name__ == "__main__":
    # Create cache directory
    os.makedirs("./cache", exist_ok=True)

    # Check connection to AEMO
    if ndt.check_connection():
        print("Successfully connected to AEMO")
    else:
        print("Warning: Cannot connect to AEMO")
        exit(1)

    # Run examples
    # example_1_basic_download()
    # example_2_price_and_demand()
    # example_3_batch_download()
    # example_4_parallel_yearly_download()
    # example_5_process_downloaded_data()
    example_6_time_windows(
        window_size_in_day=14,
        step_size_in_min=5,
        resample_interval="5min",
    )
    example_6_time_windows(
        window_size_in_day=14,
        step_size_in_min=30,
        resample_interval="5min",
    )
    example_6_time_windows(
        window_size_in_day=14,
        step_size_in_min=60,
        resample_interval="5min",
    )
    example_6_time_windows(
        window_size_in_day=14,
        step_size_in_min=30,
        resample_interval="30min",
    )
    example_6_time_windows(
        window_size_in_day=14,
        step_size_in_min=60,
        resample_interval="30min",
    )
    example_6_time_windows(
        window_size_in_day=14,
        step_size_in_min=60,
        resample_interval="1h",
    )
    example_7_resample_and_window()

    print("\nAll examples completed!")
