"""Tests for batch commands module."""

import tempfile
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


def test_download_multiple_tables_successful(
    mock_dispatch_price_data,
    mock_dispatch_region_sum_data,
):
    """Test successful download of multiple tables."""
    with (
        mock.patch("nemdatatools.batch_commands.logger"),
        mock.patch("nemdatatools.batch_commands.fetch_data") as mock_fetch,
        tempfile.TemporaryDirectory() as tmpdir,
    ):
        # Setup mock responses for different tables
        mock_fetch.side_effect = [
            mock_dispatch_price_data,
            mock_dispatch_region_sum_data,
        ]

        # Call function
        result = batch_commands.download_multiple_tables(
            table_names=["DISPATCHPRICE", "DISPATCHREGIONSUM"],
            start_date="2023/01/01",
            end_date="2023/03/01",
            regions=["NSW1", "VIC1"],
            cache_path=tmpdir,
            delay=0,
            overwrite=True,
        )

        # Verify results
        assert isinstance(result, dict)
        assert set(result.keys()) == {"DISPATCHPRICE", "DISPATCHREGIONSUM"}
        assert result["DISPATCHPRICE"].equals(mock_dispatch_price_data)
        assert result["DISPATCHREGIONSUM"].equals(mock_dispatch_region_sum_data)

        # Check fetch_data calls
        assert mock_fetch.call_count == 2
        mock_fetch.assert_any_call(
            data_type="DISPATCHPRICE",
            start_date="2023/01/01",
            end_date="2023/03/01",
            regions=["NSW1", "VIC1"],
            cache_path=tmpdir,
            delay=0,
            overwrite=True,
        )
        mock_fetch.assert_any_call(
            data_type="DISPATCHREGIONSUM",
            start_date="2023/01/01",
            end_date="2023/03/01",
            regions=["NSW1", "VIC1"],
            cache_path=tmpdir,
            delay=0,
            overwrite=True,
        )


def test_download_multiple_tables_with_failure(mock_dispatch_price_data):
    """Test download with one successful and one failed table."""
    with (
        mock.patch("nemdatatools.batch_commands.logger") as mock_logger,
        mock.patch("nemdatatools.batch_commands.fetch_data") as mock_fetch,
        tempfile.TemporaryDirectory() as tmpdir,
    ):
        # First call succeeds, second raises exception
        mock_fetch.side_effect = [mock_dispatch_price_data, Exception("Test error")]

        # Call function
        result = batch_commands.download_multiple_tables(
            table_names=["DISPATCHPRICE", "DISPATCHREGIONSUM"],
            start_date="2023/01/01",
            end_date="2023/03/01",
            cache_path=tmpdir,
        )

        # Verify results
        assert isinstance(result, dict)
        assert set(result.keys()) == {"DISPATCHPRICE", "DISPATCHREGIONSUM"}
        assert result["DISPATCHPRICE"].equals(mock_dispatch_price_data)
        assert result["DISPATCHREGIONSUM"] is None

        # Check error logging
        mock_logger.error.assert_called_once()
        assert (
            "Failed to download DISPATCHREGIONSUM" in mock_logger.error.call_args[0][0]
        )


def test_download_multiple_tables_empty():
    """Test download with empty table list."""
    with (
        mock.patch("nemdatatools.batch_commands.logger"),
        mock.patch("nemdatatools.batch_commands.fetch_data") as mock_fetch,
        tempfile.TemporaryDirectory() as tmpdir,
    ):
        # Call function with empty list
        result = batch_commands.download_multiple_tables(
            table_names=[],
            start_date="2023/01/01",
            end_date="2023/03/01",
            cache_path=tmpdir,
        )

        # Verify results
        assert isinstance(result, dict)
        assert result == {}

        # Verify fetch_data not called
        mock_fetch.assert_not_called()


def test_download_multiple_tables_progress_bar():
    """Test progress bar functionality."""
    with (
        mock.patch("nemdatatools.batch_commands.fetch_data") as mock_fetch,
        mock.patch("nemdatatools.batch_commands.tqdm") as mock_tqdm,
        tempfile.TemporaryDirectory() as tmpdir,
    ):
        # Setup mocks
        mock_progress = mock.MagicMock()
        mock_tqdm.return_value.__enter__.return_value = mock_progress
        mock_fetch.return_value = pd.DataFrame()

        # Call function
        batch_commands.download_multiple_tables(
            table_names=["DISPATCHPRICE", "DISPATCHREGIONSUM"],
            start_date="2023/01/01",
            end_date="2023/03/01",
            cache_path=tmpdir,
        )

        # Verify progress bar updates
        assert mock_progress.update.call_count == 2
        mock_tqdm.assert_called_once_with(total=2, desc="Downloading tables")


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
def test_download_multiple_tables_different_tables(
    tables,
    expected,
    mock_dispatch_price_data,
    mock_dispatch_region_sum_data,
):
    """Test download with different types of tables."""
    with (
        mock.patch("nemdatatools.batch_commands.logger"),
        mock.patch("nemdatatools.batch_commands.fetch_data") as mock_fetch,
        tempfile.TemporaryDirectory() as tmpdir,
    ):
        # Setup mock responses based on table type
        if len(tables) > 1:
            # For multiple tables, return different data for each
            mock_fetch.side_effect = [
                (
                    mock_dispatch_price_data
                    if table == "DISPATCHPRICE"
                    else mock_dispatch_region_sum_data
                )
                for table in tables
            ]
        else:
            # For single table, just return price data
            mock_fetch.return_value = mock_dispatch_price_data

        # Call function
        result = batch_commands.download_multiple_tables(
            table_names=tables,
            start_date="2023/01/01",
            end_date="2023/03/01",
            cache_path=tmpdir,
        )

        # Verify results structure
        assert isinstance(result, dict)
        assert set(result.keys()) == set(expected)
        for table in expected:
            assert isinstance(result[table], pd.DataFrame)
            assert len(result[table]) > 0

        # Verify fetch_data was called for each table
        assert mock_fetch.call_count == len(tables)


# TODO: Test for download yearly data needs to be rewritten
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
        tempfile.TemporaryDirectory() as tmpdir,
    ):
        # Setup mock responses for different years
        mock_download.side_effect = [
            {table: mock_dispatch_price_data for table in tables} for _ in years
        ]

        result = batch_commands.download_yearly_data(
            years=years,
            tables=tables,
            cache_path=tmpdir,
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
        tempfile.TemporaryDirectory() as tmpdir,
    ):
        mock_yearly.return_value = {2022: {"DISPATCHPRICE": mock_dispatch_price_data}}

        result = batch_commands.download_parallel_years(
            years=[2022],
            tables=["DISPATCHPRICE"],
            cache_path=tmpdir,
        )

        assert mock_yearly.called
        assert isinstance(result, dict)
        assert 2022 in result


def test_empty_inputs():
    """Test handling of empty inputs."""
    with (
        mock.patch("nemdatatools.batch_commands.logger"),
        tempfile.TemporaryDirectory() as tmpdir,
    ):
        # Empty years list
        result = batch_commands.download_yearly_data([], ["TABLE"], cache_path=tmpdir)
        assert result == {}

        # Empty tables list
        result = batch_commands.download_yearly_data([2023], [], cache_path=tmpdir)
        assert result == {2023: {}}
