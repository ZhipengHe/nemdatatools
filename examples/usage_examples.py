"""Example usage of NEMDataTools.

This script demonstrates how to use the NEMDataTools package to download
and process data from AEMO.
"""

import os

import matplotlib.pyplot as plt

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
        years=[2022, 2023],
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
    example_1_basic_download()
    example_2_price_and_demand()
    example_3_batch_download()
    example_4_parallel_yearly_download()
    example_5_process_downloaded_data()

    print("\nAll examples completed!")
