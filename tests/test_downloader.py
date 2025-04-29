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
