"""Tests for batch commands module."""

from unittest import mock

import pandas as pd
import pytest

from nemdatatools import batch_commands


@pytest.fixture
def mock_dispatch_price_data():
    """Fixture for mock dispatch price data."""
    return pd.DataFrame(
        {"SETTLEMENTDATE": ["2023-01-01"], "REGIONID": ["NSW1"], "RRP": [50.0]},
    )


@pytest.fixture
def mock_dispatch_region_sum_data():
    """Fixture for mock dispatch region sum data."""
    return pd.DataFrame(
        {
            "SETTLEMENTDATE": ["2023-01-01"],
            "REGIONID": ["NSW1"],
            "TOTALDEMAND": [8000.0],
        },
    )


@pytest.mark.parametrize(
    "tables,expected",
    [
        (
            ["DISPATCHPRICE", "DISPATCHREGIONSUM"],
            ["DISPATCHPRICE", "DISPATCHREGIONSUM"],
        ),
        (["PRICE_AND_DEMAND"], ["PRICE_AND_DEMAND"]),
    ],
)
def test_download_multiple_tables(
    tables,
    expected,
    mock_dispatch_price_data,
    mock_dispatch_region_sum_data,
):
    """Test batch download of multiple tables from usage examples."""
    with (
        mock.patch("nemdatatools.batch_commands.logger"),
        mock.patch("nemdatatools.downloader.download_mmsdm_data") as mock_mmsdm,
        mock.patch("nemdatatools.downloader.download_price_and_demand") as mock_price,
    ):
        # Setup mock responses based on table type
        mock_mmsdm.side_effect = [
            (
                mock_dispatch_price_data
                if table == "DISPATCHPRICE"
                else mock_dispatch_region_sum_data
            )
            for table in tables
        ]
        mock_price.return_value = None

        result = batch_commands.download_multiple_tables(
            table_names=tables,
            start_date="2023/01/01",
            end_date="2023/03/01",
            cache_path="./cache",
        )

        # Verify results structure
        assert isinstance(result, dict)
        assert set(result.keys()) == set(expected)
        for table in expected:
            assert isinstance(result[table], pd.DataFrame)
            assert len(result[table]) > 0


@pytest.mark.parametrize(
    "years,tables",
    [
        ([2022, 2023], ["PRICE_AND_DEMAND", "DISPATCHPRICE"]),
        ([2021], ["DISPATCHPRICE"]),
    ],
)
def test_download_yearly_data(years, tables, mock_dispatch_price_data):
    """Test parallel yearly download from usage examples."""
    with (
        mock.patch("nemdatatools.batch_commands.logger"),
        mock.patch(
            "nemdatatools.batch_commands.download_multiple_tables",
        ) as mock_download,
    ):
        # Setup mock responses for different years
        mock_download.side_effect = [
            {table: mock_dispatch_price_data for table in tables} for _ in years
        ]

        result = batch_commands.download_yearly_data(
            years=years,
            tables=tables,
            cache_path="./cache",
            max_workers=1,
        )

        # Verify results structure
        assert isinstance(result, dict)
        assert set(result.keys()) == set(years)
        for year in years:
            assert isinstance(result[year], dict)
            assert set(result[year].keys()) == set(tables)
            for table in tables:
                assert isinstance(result[year][table], pd.DataFrame)
                assert len(result[year][table]) > 0


def test_download_parallel_years_alias(mock_dispatch_price_data):
    """Test download_parallel_years is an alias for download_yearly_data."""
    with (
        mock.patch("nemdatatools.batch_commands.logger"),
        mock.patch("nemdatatools.batch_commands.download_yearly_data") as mock_yearly,
    ):
        mock_yearly.return_value = {2022: {"DISPATCHPRICE": mock_dispatch_price_data}}

        result = batch_commands.download_parallel_years(
            years=[2022],
            tables=["DISPATCHPRICE"],
            cache_path="./cache",
        )

        assert mock_yearly.called
        assert isinstance(result, dict)
        assert 2022 in result


def test_empty_inputs():
    """Test handling of empty inputs."""
    with mock.patch("nemdatatools.batch_commands.logger"):
        # Empty years list
        result = batch_commands.download_yearly_data([], ["TABLE"])
        assert result == {}

        # Empty tables list
        result = batch_commands.download_yearly_data([2023], [])
        assert result == {2023: {}}
