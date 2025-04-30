"""Tests for the downloader module."""

import os
import tempfile
import zipfile
from unittest import mock
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
import requests

from nemdatatools import downloader


def test_get_available_data_types():
    """Test getting available data types."""
    data_types = downloader.get_available_data_types()
    assert isinstance(data_types, list)
    assert len(data_types) > 0
    assert "DISPATCHPRICE" in data_types


def test_fetch_data_basic():
    """Test the basic functionality of fetch_data."""
    # Create a simple mock to avoid actual API calls during testing
    with mock.patch("nemdatatools.downloader.logger"):
        result = downloader.fetch_data(
            data_type="DISPATCHPRICE",
            start_date="2023/01/01",
            end_date="2023/01/02",
            regions=["NSW1"],
        )

        assert isinstance(result, pd.DataFrame)


def test_get_random_headers():
    """Test get_random_headers returns valid headers."""
    headers = downloader.get_random_headers()
    assert isinstance(headers, dict)
    assert "User-Agent" in headers
    assert "Accept" in headers
    assert "Accept-Language" in headers
    assert "Connection" in headers


def test_build_price_and_demand_url():
    """Test URL construction for price and demand data."""
    url = downloader.build_price_and_demand_url(2023, 1, "NSW1")
    assert isinstance(url, str)
    assert "202301" in url
    assert "NSW1" in url


@patch("requests.get")
@patch("requests.head")
def test_download_file_success(mock_head, mock_get):
    """Test successful file download."""
    # Setup mock responses
    mock_head_response = MagicMock()
    mock_head_response.status_code = 200
    mock_head_response.headers = {"content-length": "100"}
    mock_head.return_value = mock_head_response

    # Create a complete mock response that works with context manager
    mock_get_response = MagicMock()
    mock_get_response.status_code = 200
    mock_get_response.content = b"test content"  # Direct assignment

    # Configure the response to return itself in __enter__
    mock_get_response.__enter__.return_value = mock_get_response

    mock_get.return_value = mock_get_response

    # Create temp file
    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_path = tmp.name

    try:
        # Test download
        success = downloader.download_file("http://test.example.com", tmp_path)
        assert success is True
        assert os.path.exists(tmp_path)
        with open(tmp_path, "rb") as f:
            assert f.read() == b"test content"
    finally:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)


@patch("requests.get")
def test_download_file_failure(mock_get):
    """Test failed file download."""
    # Setup mock response to raise exception
    mock_get.side_effect = requests.exceptions.RequestException("Failed")

    # Create temp file
    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_path = tmp.name

    try:
        # Test download
        success = downloader.download_file("http://test.example.com", tmp_path)
        assert success is False
    finally:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)


def test_extract_zip():
    """Test zip file extraction."""
    # Create a test zip file
    with tempfile.TemporaryDirectory() as tmpdir:
        zip_path = os.path.join(tmpdir, "test.zip")
        extract_dir = os.path.join(tmpdir, "extracted")
        test_content = b"test file content"

        # Create zip file
        with zipfile.ZipFile(zip_path, "w") as zipf:
            zipf.writestr("test.txt", test_content)

        # Test extraction
        result = downloader.extract_zip(zip_path, extract_dir)
        assert result is None  # No specific file requested
        assert os.path.exists(os.path.join(extract_dir, "test.txt"))

        # Test specific file extraction
        specific_result = downloader.extract_zip(
            zip_path,
            extract_dir,
            specific_file="test.txt",
        )
        assert specific_result == os.path.join(extract_dir, "test.txt")


def test_check_connection_success():
    """Test successful connection check."""
    with patch("requests.get") as mock_get:
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_get.return_value = mock_response

        assert downloader.check_connection() is True


def test_check_connection_failure():
    """Test failed connection check."""
    with patch("requests.get") as mock_get:
        mock_get.side_effect = requests.exceptions.RequestException("Failed")
        assert downloader.check_connection() is False


@pytest.mark.parametrize("data_type", ["DISPATCHPRICE", "PRICE_AND_DEMAND"])
def test_download_all_regions(data_type):
    """Test downloading data for all regions."""
    with (
        patch("nemdatatools.downloader.download_price_and_demand") as mock_price,
        patch("nemdatatools.downloader.download_mmsdm_data") as mock_mmsdm,
    ):
        # Setup mocks based on data type
        if data_type == "PRICE_AND_DEMAND":
            mock_price.return_value = ["file1.csv", "file2.csv"]
            mock_mmsdm.return_value = []
        else:
            mock_price.return_value = []
            mock_mmsdm.return_value = ["file1.csv", "file2.csv"]

        result = downloader.download_all_regions(
            data_type=data_type,
            start_date="2023/01/01",
            end_date="2023/01/02",
        )

        assert isinstance(result, dict)
        assert len(result) > 0


@patch("nemdatatools.downloader.download_file")
def test_download_static_data_success(mock_download):
    """Test successful static data download."""
    mock_download.return_value = True

    with tempfile.TemporaryDirectory() as tmpdir:
        result = downloader.download_static_data(
            data_type="NEM_REG_AND_EXEMPTION",
            output_dir=tmpdir,
            overwrite=True,
        )

        assert result is not None
        assert result.endswith("NEM_REG_AND_EXEMPTION.xlsx")
        # Don't check file existence since we're mocking download_file


@patch("nemdatatools.downloader.download_file")
def test_download_static_data_failure(mock_download):
    """Test failed static data download."""
    mock_download.return_value = False

    with tempfile.TemporaryDirectory() as tmpdir:
        result = downloader.download_static_data(
            data_type="NEM_REG_AND_EXEMPTION",
            output_dir=tmpdir,
        )

        assert result is None


@patch("nemdatatools.downloader.download_file")
def test_download_static_data_invalid_type(mock_download):
    """Test static data download with invalid type."""
    mock_download.return_value = True

    with tempfile.TemporaryDirectory() as tmpdir:
        result = downloader.download_static_data(
            data_type="INVALID_TYPE",
            output_dir=tmpdir,
        )

        assert result is None


@patch("nemdatatools.downloader.download_file")
def test_download_price_and_demand_success(mock_download):
    """Test successful price and demand download."""
    mock_download.return_value = True

    with tempfile.TemporaryDirectory() as tmpdir:
        # Mock expected return paths
        expected_files = [os.path.join(tmpdir, "PRICE_AND_DEMAND_202301_NSW1.csv")]

        result = downloader.download_price_and_demand(
            start_date="2023/01/01",
            end_date="2023/01/02",
            regions=["NSW1"],
            output_dir=tmpdir,
            overwrite=True,
        )

        assert isinstance(result, list)
        assert len(result) == 1
        assert result[0].endswith(".csv")
        assert result == expected_files


@patch("nemdatatools.downloader.download_file")
def test_download_price_and_demand_failure(mock_download):
    """Test failed price and demand download."""
    mock_download.return_value = False

    with tempfile.TemporaryDirectory() as tmpdir:
        result = downloader.download_price_and_demand(
            start_date="2023/01/01",
            end_date="2023/01/02",
            regions=["NSW1"],
            output_dir=tmpdir,
        )

        assert isinstance(result, list)
        assert len(result) == 0


@patch("nemdatatools.downloader.download_file")
def test_download_price_and_demand_all_regions(mock_download):
    """Test price and demand download with all regions."""
    mock_download.return_value = True

    with tempfile.TemporaryDirectory() as tmpdir:
        result = downloader.download_price_and_demand(
            start_date="2023/01/01",
            end_date="2023/01/02",
            regions=None,  # Should default to all regions
            output_dir=tmpdir,
        )

        assert isinstance(result, list)
        assert len(result) > 0
        # Should have files for all regions
        regions_in_files = {f.split("_")[-1].split(".")[0] for f in result}
        assert regions_in_files == set(downloader.NEM_REGIONS)


def test_fetch_data_mmsdm():
    """Test fetch_data specifically for MMSDM type."""
    with (
        mock.patch("nemdatatools.downloader.logger"),
        mock.patch("nemdatatools.downloader.download_mmsdm_data") as mock_download,
        mock.patch("nemdatatools.mmsdm_helper.combine_mmsdm_files") as mock_combine,
        mock.patch("nemdatatools.mmsdm_helper.filter_mmsdm_data") as mock_filter,
        # Mock os.path.exists to control cache behavior
        mock.patch("os.path.exists", return_value=False),
        tempfile.TemporaryDirectory() as tmpdir,
    ):
        # Setup mock for download_mmsdm_data to return CSV file paths
        mock_files = [
            os.path.join(tmpdir, "DISPATCHPRICE_202301.csv"),
            os.path.join(tmpdir, "DISPATCHPRICE_202302.csv"),
        ]
        mock_download.return_value = mock_files

        # Setup mock for combine_mmsdm_files to return a raw DataFrame
        combined_df = pd.DataFrame(
            {
                "SETTLEMENTDATE": pd.date_range("2023-01-01", periods=400, freq="5min"),
                "REGIONID": ["NSW1"] * 200 + ["VIC1"] * 200,
                "RRP": [50.0] * 400,
            },
        )
        mock_combine.return_value = combined_df

        # Setup mock for filter_mmsdm_data to return the filtered DataFrame
        filtered_df = pd.DataFrame(
            {
                "SETTLEMENTDATE": pd.date_range("2023-01-01", periods=288, freq="5min"),
                "REGIONID": ["NSW1"] * 144 + ["VIC1"] * 144,
                "RRP": [50.0] * 288,
            },
        )
        mock_filter.return_value = filtered_df

        # Test with parameters from example_1_basic_download
        result = downloader.fetch_data(
            data_type="DISPATCHPRICE",
            start_date="2023/01/01",
            end_date="2023/01/07",
            regions=["NSW1", "VIC1"],
            cache_path=tmpdir,
            download_dir=tmpdir,
        )

        # Verify mock was called
        mock_download.assert_called_once()
        mock_combine.assert_called_once_with(mock_files)
        mock_filter.assert_called_once()

        # Verify result structure
        assert isinstance(result, pd.DataFrame), "Result should be a DataFrame"
        assert len(result) == 288, "Result should have 288 rows"
        assert set(result["REGIONID"].unique()) == {
            "NSW1",
            "VIC1",
        }, "Result should contain both regions"
        assert "RRP" in result.columns, "Result should contain RRP column"


def test_fetch_data_price_and_demand():
    """Test fetch_data specifically for PRICE_AND_DEMAND type."""
    with (
        mock.patch("nemdatatools.downloader.logger"),
        mock.patch(
            "nemdatatools.downloader.download_price_and_demand",
        ) as mock_download,
        # Mock os.path.exists to control cache behavior
        mock.patch("os.path.exists", return_value=False),
        tempfile.TemporaryDirectory() as tmpdir,
    ):
        # Setup mock for download_price_and_demand to return CSV file paths
        mock_files = [
            os.path.join(tmpdir, "PRICE_AND_DEMAND_202301_NSW1.csv"),
        ]
        mock_download.return_value = mock_files

        # Create a real CSV file for testing
        test_df = pd.DataFrame(
            {
                "SETTLEMENTDATE": pd.to_datetime(
                    ["2023-01-01 00:00:00", "2023-01-01 00:05:00"],
                ),
                "REGION": ["NSW1", "NSW1"],
                "RRP": [50.25, 51.75],
                "TOTALDEMAND": [7500.0, 7550.0],
                "PERIODTYPE": ["TRADE", "TRADE"],
            },
        )

        # Save the test DataFrame to the mocked file location
        os.makedirs(os.path.dirname(mock_files[0]), exist_ok=True)
        test_df.to_csv(mock_files[0], index=False)

        # Test fetch
        result = downloader.fetch_data(
            data_type="PRICE_AND_DEMAND",
            start_date="2023/01/01",
            end_date="2023/01/02",
            regions=["NSW1"],
            cache_path=tmpdir,
            download_dir=tmpdir,
        )

        # Verify mock was called
        mock_download.assert_called_once()

        # Verify results
        assert isinstance(result, pd.DataFrame), "Result should be a DataFrame"
        assert len(result) == 2, "Result should have 2 rows"
        assert "REGION" in result.columns, "Result should contain REGION column"
        assert "RRP" in result.columns, "Result should contain RRP column"
        assert "TOTALDEMAND" in result.columns, "Result should contain TOTALDEMAND"
        assert set(result["REGION"].unique()) == {
            "NSW1",
        }, "Result should only contain NSW1 region"

        # Check values are preserved
        assert result["RRP"].tolist() == [
            50.25,
            51.75,
        ], "RRP values should match test data"
        assert result["TOTALDEMAND"].tolist() == [
            7500.0,
            7550.0,
        ], "TOTALDEMAND values should match test data"
