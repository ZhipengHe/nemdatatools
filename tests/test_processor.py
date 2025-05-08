"""Tests for the processor module."""

import numpy as np
import pandas as pd
import pytest

from nemdatatools import processor


@pytest.fixture
def sample_dispatch_price_data():
    """Create sample dispatch price data for testing."""
    return pd.DataFrame(
        {
            "SETTLEMENTDATE": ["2023/01/01 00:05:00", "2023/01/01 00:10:00"],
            "REGIONID": ["NSW1", "VIC1"],
            "RRP": [25.0, 30.0],
            "INTERVENTION": [0, 0],
        },
    )


@pytest.fixture
def sample_dispatch_region_sum_data():
    """Create sample dispatch region sum data for testing."""
    return pd.DataFrame(
        {
            "SETTLEMENTDATE": ["2023/01/01 00:05:00", "2023/01/01 00:10:00"],
            "REGIONID": ["NSW1", "VIC1"],
            "TOTALDEMAND": [8000.0, 7500.0],
            "INTERVENTION": [0, 0],
        },
    )


@pytest.fixture
def sample_dispatch_unit_scada_data():
    """Create sample dispatch unit SCADA data for testing."""
    return pd.DataFrame(
        {
            "SETTLEMENTDATE": ["2023/01/01 00:05:00", "2023/01/01 00:10:00"],
            "DUID": ["UNIT1", "UNIT2"],
            "SCADAVALUE": [120.5, 85.2],
            "INTERVENTION": [0, 0],
        },
    )


@pytest.fixture
def sample_predispatch_price_data():
    """Create sample predispatch price data for testing."""
    return pd.DataFrame(
        {
            "PREDISPATCH_RUN_DATETIME": ["2023/01/01 00:00:00", "2023/01/01 00:00:00"],
            "DATETIME": ["2023/01/01 01:00:00", "2023/01/01 01:00:00"],
            "REGIONID": ["NSW1", "VIC1"],
            "RRP": [28.5, 32.1],
            "INTERVENTION": [0, 0],
        },
    )


@pytest.fixture
def sample_p5min_data():
    """Create sample P5MIN data for testing."""
    return pd.DataFrame(
        {
            "RUN_DATETIME": ["2023/01/01 00:00:00", "2023/01/01 00:00:00"],
            "INTERVAL_DATETIME": ["2023/01/01 00:05:00", "2023/01/01 00:05:00"],
            "REGIONID": ["NSW1", "VIC1"],
            "RRP": [26.4, 31.2],
            "TOTALDEMAND": [8100.0, 7600.0],
            "INTERVENTION": [0, 0],
        },
    )


@pytest.fixture
def sample_bid_day_offer_data():
    """Create sample bid day offer data for testing."""
    return pd.DataFrame(
        {
            "SETTLEMENTDATE": ["2023/01/01 00:00:00", "2023/01/01 00:00:00"],
            "DUID": ["UNIT1", "UNIT2"],
            "BIDTYPE": ["ENERGY", "ENERGY"],
            "PRICEBAND1": [50.0, 45.0],
            "PRICEBAND2": [75.0, 70.0],
            "INTERVENTION": [0, 0],
        },
    )


@pytest.fixture
def sample_price_and_demand_data():
    """Create sample price and demand data for testing."""
    return pd.DataFrame(
        {
            "SETTLEMENTDATE": ["2023/01/01 00:05:00", "2023/01/01 00:10:00"],
            "REGIONID": ["NSW1", "VIC1"],
            "RRP": [27.8, 33.5],
            "TOTALDEMAND": [8050.0, 7550.0],
            "PERIODTYPE": ["TRADE", "TRADE"],
        },
    )


def test_standardize_empty_data():
    """Test standardization with empty DataFrame."""
    empty_df = pd.DataFrame()
    result = processor.standardize(empty_df, "DISPATCHPRICE")

    assert result.empty
    assert isinstance(result, pd.DataFrame)


def test_standardize_dispatch_price(sample_dispatch_price_data):
    """Test standardization with DISPATCHPRICE data."""
    result = processor.standardize(sample_dispatch_price_data, "DISPATCHPRICE")

    assert not result.empty
    assert len(result) == len(sample_dispatch_price_data)

    # Check that SETTLEMENTDATE is the index
    assert result.index.name == "SETTLEMENTDATE"

    # Check that expected columns are present
    assert "REGIONID" in result.columns
    assert "RRP" in result.columns

    # Verify index values match original SETTLEMENTDATE values
    assert all(
        pd.to_datetime(sample_dispatch_price_data["SETTLEMENTDATE"]) == result.index,
    )

    # Verify data remains the same
    assert all(sample_dispatch_price_data["RRP"].values == result["RRP"].values)
    assert all(
        sample_dispatch_price_data["REGIONID"].values == result["REGIONID"].values,
    )


def test_standardize_dispatch_region_sum(sample_dispatch_region_sum_data):
    """Test standardization with DISPATCHREGIONSUM data."""
    result = processor.standardize(sample_dispatch_region_sum_data, "DISPATCHREGIONSUM")

    assert not result.empty
    assert len(result) == len(sample_dispatch_region_sum_data)

    # Check that SETTLEMENTDATE is the index
    assert result.index.name == "SETTLEMENTDATE"

    # Check that expected columns are present
    assert "REGIONID" in result.columns
    assert "TOTALDEMAND" in result.columns

    # Verify negative demand values are handled
    test_data = sample_dispatch_region_sum_data.copy()
    test_data.loc[0, "TOTALDEMAND"] = -100  # Add a negative value
    result_with_neg = processor.standardize(test_data, "DISPATCHREGIONSUM")
    assert np.isnan(
        result_with_neg["TOTALDEMAND"].iloc[0],
    )  # Should be converted to NaN


def test_standardize_dispatch_unit_scada(sample_dispatch_unit_scada_data):
    """Test standardization with DISPATCH_UNIT_SCADA data."""
    result = processor.standardize(
        sample_dispatch_unit_scada_data,
        "DISPATCH_UNIT_SCADA",
    )

    assert not result.empty
    assert len(result) == len(sample_dispatch_unit_scada_data)

    # Check that we have a multi-index with SETTLEMENTDATE and DUID
    assert result.index.names[0] == "SETTLEMENTDATE"
    assert result.index.names[1] == "DUID"

    # Check that SCADAVALUE is present and numeric
    assert "SCADAVALUE" in result.columns
    assert result["SCADAVALUE"].dtype in [np.float64, np.int64]


def test_standardize_predispatch_price(sample_predispatch_price_data):
    """Test standardization with PREDISPATCHPRICE data."""
    result = processor.standardize(sample_predispatch_price_data, "PREDISPATCHPRICE")

    assert not result.empty
    assert len(result) == len(sample_predispatch_price_data)

    # Check that we have a multi-index
    assert "PREDISPATCH_RUN_DATETIME" in result.index.names
    assert "DATETIME" in result.index.names

    # Check that forecast horizon was calculated
    assert "FORECAST_HORIZON_HOURS" in result.columns

    # Check that RRP is numeric
    assert result["RRP"].dtype in [np.float64, np.int64]


def test_standardize_p5min_region_solution(sample_p5min_data):
    """Test standardization with P5MIN_REGIONSOLUTION data."""
    result = processor.standardize(sample_p5min_data, "P5MIN_REGIONSOLUTION")

    assert not result.empty
    assert len(result) == len(sample_p5min_data)

    # Check that we have a multi-index
    assert "RUN_DATETIME" in result.index.names
    assert "INTERVAL_DATETIME" in result.index.names
    assert "REGIONID" in result.index.names

    # Check that forecast horizon was calculated (in minutes for P5MIN)
    assert "FORECAST_HORIZON_MINUTES" in result.columns


def test_standardize_bid_day_offer(sample_bid_day_offer_data):
    """Test standardization with BIDDAYOFFER_D data."""
    result = processor.standardize(sample_bid_day_offer_data, "BIDDAYOFFER_D")

    assert not result.empty
    assert len(result) == len(sample_bid_day_offer_data)

    # Check that we have a multi-index
    assert "SETTLEMENTDATE" in result.index.names
    assert "DUID" in result.index.names
    assert "BIDTYPE" in result.index.names

    # Check that price bands are numeric
    assert result["PRICEBAND1"].dtype in [np.float64, np.int64]
    assert result["PRICEBAND2"].dtype in [np.float64, np.int64]


def test_standardize_price_and_demand(sample_price_and_demand_data):
    """Test standardization with PRICE_AND_DEMAND data."""
    result = processor.standardize(sample_price_and_demand_data, "PRICE_AND_DEMAND")

    assert not result.empty
    assert len(result) == len(sample_price_and_demand_data)

    # Check that SETTLEMENTDATE is the index
    assert result.index.name == "SETTLEMENTDATE"

    # Check that PERIODTYPE is uppercase
    assert all(result["PERIODTYPE"].str.isupper())


def test_standardize_unknown_data_type(sample_dispatch_price_data):
    """Test standardization with unknown data type."""
    result = processor.standardize(sample_dispatch_price_data, "UNKNOWN_TYPE")

    assert not result.empty
    assert len(result) == len(sample_dispatch_price_data)
    # Should apply general standardization


def test_filter_by_regions_with_regionid():
    """Test filtering by regions using REGIONID column."""
    test_data = pd.DataFrame(
        {"REGIONID": ["NSW1", "VIC1", "QLD1", "SA1"], "VALUE": [1, 2, 3, 4]},
    )

    result = processor.filter_by_regions(test_data, ["NSW1", "VIC1"])

    assert len(result) == 2
    assert all(result["REGIONID"].isin(["NSW1", "VIC1"]))


def test_filter_by_regions_with_region():
    """Test filtering by regions using REGION column."""
    test_data = pd.DataFrame(
        {"REGION": ["NSW1", "VIC1", "QLD1", "SA1"], "VALUE": [1, 2, 3, 4]},
    )

    result = processor.filter_by_regions(test_data, ["NSW1", "VIC1"])

    assert len(result) == 2
    assert all(result["REGION"].isin(["NSW1", "VIC1"]))


def test_filter_by_regions_case_insensitive():
    """Test that region filtering is case insensitive."""
    test_data = pd.DataFrame(
        {"REGIONID": ["NSW1", "VIC1", "QLD1", "SA1"], "VALUE": [1, 2, 3, 4]},
    )

    result = processor.filter_by_regions(test_data, ["nsw1", "vic1"])

    assert len(result) == 2
    assert all(result["REGIONID"].isin(["NSW1", "VIC1"]))


def test_filter_by_regions_no_region_column():
    """Test filtering when no region column exists."""
    test_data = pd.DataFrame({"VALUE": [1, 2, 3, 4]})

    result = processor.filter_by_regions(test_data, ["NSW1", "VIC1"])

    # Should return the original DataFrame unchanged
    assert len(result) == len(test_data)


def test_calculate_price_statistics():
    """Test calculating price statistics."""
    test_data = pd.DataFrame(
        {
            "SETTLEMENTDATE": pd.date_range(
                start="2023-01-01",
                periods=48,
                freq="30min",
            ),
            "REGIONID": ["NSW1"] * 48,
            "RRP": np.random.normal(50, 10, 48),
        },
    ).set_index("SETTLEMENTDATE")

    result = processor.calculate_price_statistics(test_data)

    assert not result.empty
    assert "REGIONID" in result.columns
    assert "RRP_MIN" in result.columns
    assert "RRP_MAX" in result.columns
    assert "RRP_MEAN" in result.columns


def test_calculate_demand_statistics():
    """Test calculating demand statistics."""
    test_data = pd.DataFrame(
        {
            "SETTLEMENTDATE": pd.date_range(
                start="2023-01-01",
                periods=48,
                freq="30min",
            ),
            "REGIONID": ["NSW1"] * 48,
            "TOTALDEMAND": np.random.normal(8000, 500, 48),
        },
    ).set_index("SETTLEMENTDATE")

    result = processor.calculate_demand_statistics(test_data)

    assert not result.empty
    assert "REGIONID" in result.columns
    assert "TOTALDEMAND_MIN" in result.columns
    assert "TOTALDEMAND_MAX" in result.columns
    assert "TOTALDEMAND_MEAN" in result.columns


def test_merge_datasets():
    """Test merging multiple datasets."""
    df1 = pd.DataFrame(
        {
            "SETTLEMENTDATE": ["2023/01/01", "2023/01/02"],
            "REGIONID": ["NSW1", "VIC1"],
            "RRP": [25.0, 30.0],
        },
    )

    df2 = pd.DataFrame(
        {
            "SETTLEMENTDATE": ["2023/01/01", "2023/01/02"],
            "REGIONID": ["NSW1", "VIC1"],
            "TOTALDEMAND": [8000.0, 7500.0],
        },
    )

    result = processor.merge_datasets([df1, df2], on=["SETTLEMENTDATE", "REGIONID"])

    assert not result.empty
    assert len(result) == 2
    assert "RRP" in result.columns
    assert "TOTALDEMAND" in result.columns


def test_merge_datasets_single_dataset():
    """Test merging with only one dataset."""
    df = pd.DataFrame(
        {
            "SETTLEMENTDATE": ["2023/01/01", "2023/01/02"],
            "REGIONID": ["NSW1", "VIC1"],
            "RRP": [25.0, 30.0],
        },
    )

    result = processor.merge_datasets([df])

    assert not result.empty
    assert len(result) == 2
    assert all(result.columns == df.columns)


def test_merge_datasets_empty_list():
    """Test merging with an empty list."""
    result = processor.merge_datasets([])

    assert result.empty
