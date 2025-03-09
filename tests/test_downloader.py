"""Tests for the downloader module."""

from unittest import mock

import pandas as pd

from nemdatatools import downloader

# import pytest


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
