"""Examples for processing price and demand data from AEMO.

This script demonstrates how to process and analyze price and demand data
from AEMO's MMSDM database.
"""

import os

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas as pd

import nemdatatools as ndt


def resample_price_data_by_region(
    start_date: str = "2021/01/01",
    end_date: str = "2024/12/31",
    regions: list[str] | None = None,  # ["NSW1", "QLD1", "SA1", "TAS1", "VIC1"],
    output_dir: str = "./examples/NEM_data/price_data",
) -> None:
    """Resample MMSDM data by region to 1-hour intervals and save to csv files.

    This example demonstrates:
    - Fetching MMSDM data for specified regions for a specified date range
    - Resampling to 1-hour intervals
    - Saving the processed data to csv files by region
    - Generating yearly statistics

    Args:
        start_date: Start date in YYYY/MM/DD format (default: "2021/01/01")
        end_date: End date in YYYY/MM/DD format (default: "2024/12/31")
        regions: List of regions to process (default: all NEM regions)
        output_dir: Directory to save all output files
            (default: "./examples/NEM_data/price_data")

    """
    if regions is None:
        regions = ["NSW1", "QLD1", "SA1", "TAS1", "VIC1"]

    print("\nExample 8: Resampling MMSDM data by region")
    print(f"Processing data from {start_date} to {end_date}")
    print(f"Regions: {', '.join(regions)}")

    # Create output directory
    try:
        os.makedirs(output_dir, exist_ok=True)
    except Exception as e:
        print(f"Error creating output directory: {e}")
        return

    # Fetch data for each region
    for region in regions:
        print(f"\nProcessing {region}...")

        try:
            # Fetch data for the region
            data = ndt.fetch_data(
                data_type="DISPATCHPRICE",
                start_date=start_date,
                end_date=end_date,
                regions=[region],
                cache_path="./cache",
            )

            if data is None or data.empty:
                print(f"Error: No data fetched for {region}")
                continue

            # Select only required columns (SETTLEMENTDATE is already the index)
            required_columns = ["REGIONID", "RRP"]
            data = data[required_columns]

            # Ensure data is sorted chronologically
            data = data.sort_index()

            print(f"Original data shape: {data.shape}")
            print(f"Original data frequency: {pd.infer_freq(data.index)}")
            print(f"Data range: {data.index[0]} to {data.index[-1]}")

            # Resample data to 1-hour intervals
            print(f"Resampling {region} data to 1-hour intervals...")
            resampled_data = ndt.resample_data(
                data,
                interval="1h",
                numeric_agg="mean",  # Use mean for numeric columns
                non_numeric_agg="first",  # Use first value for non-numeric columns
            )

            print(f"Resampled data shape: {resampled_data.shape}")
            print(f"Resampled data frequency: {pd.infer_freq(resampled_data.index)}")

            # Check for missing values
            missing_values = resampled_data["RRP"].isnull().sum()
            print(f"Number of missing RRP values: {missing_values}")

            # Calculate yearly statistics
            yearly_stats = (
                resampled_data.groupby(resampled_data.index.year)["RRP"]
                .agg(["count", "mean", "std", "min", "max"])
                .round(2)
            )
            print("\nYearly Statistics:")
            print(yearly_stats)

            # Save yearly statistics
            yearly_stats_file = os.path.join(
                output_dir,
                f"{region}_price_yearly_stats.csv",
            )
            yearly_stats.to_csv(yearly_stats_file)
            print(f"Saved yearly statistics to {yearly_stats_file}")

            # Save resampled data
            output_file = os.path.join(output_dir, f"{region}_price_hourly.csv")
            resampled_data.to_csv(output_file)
            print(f"Saved resampled data to {output_file}")

            # Create a plot of the resampled data
            plt.figure(figsize=(15, 8))
            plt.plot(resampled_data.index, resampled_data["RRP"], linewidth=0.5)

            plt.title(f"Hourly Prices - {region} (2021-2024)")
            plt.xlabel("Time")
            plt.ylabel("RRP ($/MWh)")
            plt.grid(True)
            plt.xticks(rotation=45)

            # Add yearly x-axis labels
            plt.gca().xaxis.set_major_locator(mdates.YearLocator())
            plt.gca().xaxis.set_major_formatter(mdates.DateFormatter("%Y"))

            plt.tight_layout()

            # Save the plot
            plot_file = os.path.join(output_dir, f"{region}_price_hourly.png")
            plt.savefig(plot_file, dpi=300)
            plt.close()
            print(f"Saved plot to {plot_file}")

        except Exception as e:
            print(f"Error processing {region}: {e}")
            continue

    print("\nProcessing complete!")
    print(f"All resampled data and plots saved to {output_dir}")

    # Create a summary of the processed data
    summary = []
    for region in regions:
        try:
            file_path = os.path.join(output_dir, f"{region}_price_hourly.csv")
            if os.path.exists(file_path):
                data = pd.read_csv(file_path, index_col=0, parse_dates=True)
                summary.append(
                    {
                        "Region": region,
                        "Data Points": len(data),
                        "Start Date": data.index[0],
                        "End Date": data.index[-1],
                        "Missing Values": data["RRP"].isnull().sum(),
                        "Mean Price": data["RRP"].mean(),
                        "Max Price": data["RRP"].max(),
                        "Min Price": data["RRP"].min(),
                        "Std Dev": data["RRP"].std(),
                    },
                )
        except Exception as e:
            print(f"Error creating summary for {region}: {e}")

    if summary:
        try:
            # Create and save summary DataFrame
            summary_df = pd.DataFrame(summary)
            summary_file = os.path.join(output_dir, "summary.csv")
            summary_df.to_csv(summary_file)
            print(f"\nSummary saved to {summary_file}")
            print("\nSummary of processed data:")
            print(summary_df.to_string(index=False))
        except Exception as e:
            print(f"Error saving summary: {e}")
    else:
        print("\nNo summary created - no data was successfully processed")


def resample_demand_data_by_region(
    start_date: str = "2021/01/01",
    end_date: str = "2024/12/31",
    regions: list[str] | None = None,
    output_dir: str = "./examples/NEM_data/demand_data",
) -> None:
    """Resample demand data by region to 1-hour intervals and save to csv files.

    This example demonstrates:
    - Fetching DISPATCHREGIONSUM data for specified regions for a specified date range
    - Resampling to 1-hour intervals
    - Saving the processed data to csv files by region
    - Generating yearly statistics

    Args:
        start_date: Start date in YYYY/MM/DD format (default: "2021/01/01")
        end_date: End date in YYYY/MM/DD format (default: "2024/12/31")
        regions: List of regions to process (default: all NEM regions)
        output_dir: Directory to save all output files
            (default: "./examples/NEM_data/demand_data")

    """
    if regions is None:
        regions = ["NSW1", "QLD1", "SA1", "TAS1", "VIC1"]

    print("\nExample 9: Resampling demand data by region")
    print(f"Processing data from {start_date} to {end_date}")
    print(f"Regions: {', '.join(regions)}")

    # Create output directory
    try:
        os.makedirs(output_dir, exist_ok=True)
    except Exception as e:
        print(f"Error creating output directory: {e}")
        return

    # Fetch data for each region
    for region in regions:
        print(f"\nProcessing {region}...")

        try:
            # Fetch data for the region
            data = ndt.fetch_data(
                data_type="DISPATCHREGIONSUM",
                start_date=start_date,
                end_date=end_date,
                regions=[region],
                cache_path="./cache",
            )

            if data is None or data.empty:
                print(f"Error: No data fetched for {region}")
                continue

            # Select only required columns (SETTLEMENTDATE is already the index)
            required_columns = ["REGIONID", "TOTALDEMAND"]
            data = data[required_columns]

            # Ensure data is sorted chronologically
            data = data.sort_index()

            print(f"Original data shape: {data.shape}")
            print(f"Original data frequency: {pd.infer_freq(data.index)}")
            print(f"Data range: {data.index[0]} to {data.index[-1]}")

            # Check for missing values in original data
            missing_original = data["TOTALDEMAND"].isnull().sum()
            if missing_original > 0:
                print(f"\nMissing values in original data: {missing_original}")
                print("Sample of missing values:")
                print(data[data["TOTALDEMAND"].isnull()].head())

            # Resample data to 1-hour intervals
            print(f"Resampling {region} data to 1-hour intervals...")
            resampled_data = ndt.resample_data(
                data,
                interval="1h",
                numeric_agg="mean",  # Use mean for numeric columns
                non_numeric_agg="first",  # Use first value for non-numeric columns
            )

            print(f"Resampled data shape: {resampled_data.shape}")
            print(f"Resampled data frequency: {pd.infer_freq(resampled_data.index)}")

            # Check for missing values
            missing_values = resampled_data["TOTALDEMAND"].isnull().sum()
            print(f"Number of missing TOTALDEMAND values: {missing_values}")

            # If there are missing values, show their details
            if missing_values > 0:
                print("\nMissing values details:")
                missing_data = resampled_data[resampled_data["TOTALDEMAND"].isnull()]
                print("\nTimestamps with missing values:")
                print(missing_data.index)
                print("\nMissing values by year:")
                print(missing_data.index.year.value_counts().sort_index())
                print("\nMissing values by month:")
                print(missing_data.index.month.value_counts().sort_index())

                # Save missing values to a separate file
                missing_file = os.path.join(
                    output_dir,
                    f"{region}_demand_missing_values.csv",
                )
                missing_data.to_csv(missing_file)
                print(f"\nSaved missing values details to {missing_file}")

            # Calculate yearly statistics
            yearly_stats = (
                resampled_data.groupby(resampled_data.index.year)["TOTALDEMAND"]
                .agg(["count", "mean", "std", "min", "max"])
                .round(2)
            )
            print("\nYearly Statistics:")
            print(yearly_stats)

            # Save yearly statistics
            yearly_stats_file = os.path.join(
                output_dir,
                f"{region}_demand_yearly_stats.csv",
            )
            yearly_stats.to_csv(yearly_stats_file)
            print(f"Saved yearly statistics to {yearly_stats_file}")

            # Save resampled data
            output_file = os.path.join(output_dir, f"{region}_demand_hourly.csv")
            resampled_data.to_csv(output_file)
            print(f"Saved resampled data to {output_file}")

            # Create a plot of the resampled data
            plt.figure(figsize=(15, 8))
            plt.plot(resampled_data.index, resampled_data["TOTALDEMAND"], linewidth=0.5)

            # Add markers for missing values if any
            if missing_values > 0:
                plt.scatter(
                    missing_data.index,
                    [resampled_data["TOTALDEMAND"].max()] * len(missing_data),
                    color="red",
                    marker="x",
                    label="Missing Values",
                )
                plt.legend()

            plt.title(f"Hourly Demand - {region} (2021-2024)")
            plt.xlabel("Time")
            plt.ylabel("Total Demand (MW)")
            plt.grid(True)
            plt.xticks(rotation=45)

            # Add yearly x-axis labels
            plt.gca().xaxis.set_major_locator(mdates.YearLocator())
            plt.gca().xaxis.set_major_formatter(mdates.DateFormatter("%Y"))

            plt.tight_layout()

            # Save the plot
            plot_file = os.path.join(output_dir, f"{region}_demand_hourly.png")
            plt.savefig(plot_file, dpi=300)
            plt.close()
            print(f"Saved plot to {plot_file}")

        except Exception as e:
            print(f"Error processing {region}: {e}")
            continue

    print("\nProcessing complete!")
    print(f"All resampled data and plots saved to {output_dir}")

    # Create a summary of the processed data
    summary = []
    for region in regions:
        try:
            file_path = os.path.join(output_dir, f"{region}_demand_hourly.csv")
            if os.path.exists(file_path):
                data = pd.read_csv(file_path, index_col=0, parse_dates=True)
                summary.append(
                    {
                        "Region": region,
                        "Data Points": len(data),
                        "Start Date": data.index[0],
                        "End Date": data.index[-1],
                        "Missing Values": data["TOTALDEMAND"].isnull().sum(),
                        "Mean Demand": data["TOTALDEMAND"].mean(),
                        "Max Demand": data["TOTALDEMAND"].max(),
                        "Min Demand": data["TOTALDEMAND"].min(),
                        "Std Dev": data["TOTALDEMAND"].std(),
                    },
                )
        except Exception as e:
            print(f"Error creating summary for {region}: {e}")

    if summary:
        try:
            # Create and save summary DataFrame
            summary_df = pd.DataFrame(summary)
            summary_file = os.path.join(output_dir, "summary.csv")
            summary_df.to_csv(summary_file)
            print(f"\nSummary saved to {summary_file}")
            print("\nSummary of processed data:")
            print(summary_df.to_string(index=False))
        except Exception as e:
            print(f"Error saving summary: {e}")
    else:
        print("\nNo summary created - no data was successfully processed")


def combine_price_and_demand(
    start_date: str = "2021/01/01",
    end_date: str = "2024/12/31",
    regions: list[str] | None = None,
    output_dir: str = "./examples/NEM_data/price_demand",
) -> None:
    """Combine price and demand data by region to 1-hour intervals and save csv.

    This example demonstrates:
    - Fetching both DISPATCHPRICE and DISPATCHREGIONSUM data for specified regions
    - Resampling to 1-hour intervals
    - Combining price and demand data
    - Saving the processed data to csv files by region
    - Generating yearly statistics

    Args:
        start_date: Start date in YYYY/MM/DD format (default: "2021/01/01")
        end_date: End date in YYYY/MM/DD format (default: "2024/12/31")
        regions: List of regions to process (default: all NEM regions)
        output_dir: Directory to save all output files
            (default: "./examples/NEM_data/price_demand")

    """
    if regions is None:
        regions = ["NSW1", "QLD1", "SA1", "TAS1", "VIC1"]

    print("\nExample 10: Combining price and demand data by region")
    print(f"Processing data from {start_date} to {end_date}")
    print(f"Regions: {', '.join(regions)}")

    # Create output directory
    try:
        os.makedirs(output_dir, exist_ok=True)
    except Exception as e:
        print(f"Error creating output directory: {e}")
        return

    # Process each region
    for region in regions:
        print(f"\nProcessing {region}...")

        try:
            # Download both price and demand data
            result = ndt.download_multiple_tables(
                table_names=["DISPATCHPRICE", "DISPATCHREGIONSUM"],
                start_date=start_date,
                end_date=end_date,
                regions=[region],
                cache_path="./cache",
            )

            if not result or len(result) != 2:
                print(f"Error: Failed to download data for {region}")
                continue

            price_data = result["DISPATCHPRICE"]
            demand_data = result["DISPATCHREGIONSUM"]

            if price_data is None or demand_data is None:
                print(f"Error: No data fetched for {region}")
                continue

            # Select required columns
            price_data = price_data[["REGIONID", "RRP"]]
            demand_data = demand_data[["REGIONID", "TOTALDEMAND"]]

            # Ensure data is sorted chronologically
            price_data = price_data.sort_index()
            demand_data = demand_data.sort_index()

            print(f"Original price data shape: {price_data.shape}")
            print(f"Original demand data shape: {demand_data.shape}")

            # Resample both datasets to 1-hour intervals
            print(f"Resampling {region} data to 1-hour intervals...")
            resampled_price = ndt.resample_data(
                price_data,
                interval="1h",
                numeric_agg="mean",
                non_numeric_agg="first",
            )

            resampled_demand = ndt.resample_data(
                demand_data,
                interval="1h",
                numeric_agg="mean",
                non_numeric_agg="first",
            )

            # Combine price and demand data
            combined_data = pd.concat(
                [resampled_price["RRP"], resampled_demand["TOTALDEMAND"]],
                axis=1,
            )

            print(f"Combined data shape: {combined_data.shape}")

            # Check for missing values
            missing_price = combined_data["RRP"].isnull().sum()
            missing_demand = combined_data["TOTALDEMAND"].isnull().sum()
            print(f"Number of missing RRP values: {missing_price}")
            print(f"Number of missing TOTALDEMAND values: {missing_demand}")

            # If there are missing values, show their details
            if missing_price > 0 or missing_demand > 0:
                print("\nMissing values details:")
                missing_data = combined_data[combined_data.isnull().any(axis=1)]
                print("\nTimestamps with missing values:")
                print(missing_data.index)
                print("\nMissing values by year:")
                print(missing_data.index.year.value_counts().sort_index())

                # Save missing values to a separate file
                missing_file = os.path.join(output_dir, f"{region}_missing_values.csv")
                missing_data.to_csv(missing_file)
                print(f"\nSaved missing values details to {missing_file}")

            # Calculate yearly statistics
            yearly_stats = (
                combined_data.groupby(combined_data.index.year)
                .agg(
                    {
                        "RRP": ["count", "mean", "std", "min", "max"],
                        "TOTALDEMAND": ["count", "mean", "std", "min", "max"],
                    },
                )
                .round(2)
            )
            print("\nYearly Statistics:")
            print(yearly_stats)

            # Save yearly statistics
            yearly_stats_file = os.path.join(output_dir, f"{region}_yearly_stats.csv")
            yearly_stats.to_csv(yearly_stats_file)
            print(f"Saved yearly statistics to {yearly_stats_file}")

            # Save combined data
            output_file = os.path.join(output_dir, f"{region}_price_demand.csv")
            combined_data.to_csv(output_file)
            print(f"Saved price and demand data to {output_file}")

            # Create plots
            fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(15, 12))

            # Price plot
            ax1.plot(combined_data.index, combined_data["RRP"], linewidth=0.5)
            if missing_price > 0:
                missing_price_data = combined_data[combined_data["RRP"].isnull()]
                ax1.scatter(
                    missing_price_data.index,
                    [combined_data["RRP"].max()] * len(missing_price_data),
                    color="red",
                    marker="x",
                    label="Missing Values",
                )
                ax1.legend()
            ax1.set_title(f"Hourly Prices - {region} (2021-2024)")
            ax1.set_ylabel("RRP ($/MWh)")
            ax1.grid(True)
            ax1.tick_params(axis="x", rotation=45)

            # Demand plot
            ax2.plot(combined_data.index, combined_data["TOTALDEMAND"], linewidth=0.5)
            if missing_demand > 0:
                missing_demand_data = combined_data[
                    combined_data["TOTALDEMAND"].isnull()
                ]
                ax2.scatter(
                    missing_demand_data.index,
                    [combined_data["TOTALDEMAND"].max()] * len(missing_demand_data),
                    color="red",
                    marker="x",
                    label="Missing Values",
                )
                ax2.legend()
            ax2.set_title(f"Hourly Demand - {region} (2021-2024)")
            ax2.set_xlabel("Time")
            ax2.set_ylabel("Total Demand (MW)")
            ax2.grid(True)
            ax2.tick_params(axis="x", rotation=45)

            # Add yearly x-axis labels
            for ax in [ax1, ax2]:
                ax.xaxis.set_major_locator(mdates.YearLocator())
                ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))

            plt.tight_layout()

            # Save the plot
            plot_file = os.path.join(output_dir, f"{region}_price_demand.png")
            plt.savefig(plot_file, dpi=300)
            plt.close()
            print(f"Saved plot to {plot_file}")

        except Exception as e:
            print(f"Error processing {region}: {e}")
            continue

    print("\nProcessing complete!")
    print(f"All price and demand data and plots saved to {output_dir}")

    # Create a summary of the processed data
    summary = []
    for region in regions:
        try:
            file_path = os.path.join(output_dir, f"{region}_price_demand.csv")
            if os.path.exists(file_path):
                data = pd.read_csv(file_path, index_col=0, parse_dates=True)
                summary.append(
                    {
                        "Region": region,
                        "Data Points": len(data),
                        "Start Date": data.index[0],
                        "End Date": data.index[-1],
                        "Missing Price Values": data["RRP"].isnull().sum(),
                        "Missing Demand Values": data["TOTALDEMAND"].isnull().sum(),
                        "Mean Price": data["RRP"].mean(),
                        "Max Price": data["RRP"].max(),
                        "Min Price": data["RRP"].min(),
                        "Mean Demand": data["TOTALDEMAND"].mean(),
                        "Max Demand": data["TOTALDEMAND"].max(),
                        "Min Demand": data["TOTALDEMAND"].min(),
                    },
                )
        except Exception as e:
            print(f"Error creating summary for {region}: {e}")

    if summary:
        try:
            # Create and save summary DataFrame
            summary_df = pd.DataFrame(summary)
            summary_file = os.path.join(output_dir, "summary.csv")
            summary_df.to_csv(summary_file)
            print(f"\nSummary saved to {summary_file}")
            print("\nSummary of processed data:")
            print(summary_df.to_string(index=False))
        except Exception as e:
            print(f"Error saving summary: {e}")
    else:
        print("\nNo summary created - no data was successfully processed")


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
    resample_price_data_by_region(
        start_date="2021/01/01",
        end_date="2024/12/31",
        regions=["NSW1", "QLD1", "SA1", "TAS1", "VIC1"],
        output_dir="./examples/NEM_data/price_data",
    )
    resample_demand_data_by_region(
        start_date="2021/01/01",
        end_date="2024/12/31",
        regions=["NSW1", "QLD1", "SA1", "TAS1", "VIC1"],
        output_dir="./examples/NEM_data/demand_data",
    )
    combine_price_and_demand(
        start_date="2021/01/01",
        end_date="2024/12/31",
        regions=["NSW1", "QLD1", "SA1", "TAS1", "VIC1"],
        output_dir="./examples/NEM_data/price_demand",
    )

    print("\nAll examples completed!")
