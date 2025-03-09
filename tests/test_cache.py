"""Tests for the cache module."""

import os
import tempfile

import pandas as pd
import pytest

from nemdatatools import cache


@pytest.fixture
def sample_data():
    """Create sample data for testing."""
    return pd.DataFrame(
        {
            "SETTLEMENTDATE": ["2023/01/01 00:05:00", "2023/01/01 00:10:00"],
            "REGIONID": ["NSW1", "NSW1"],
            "RRP": [25.0, 30.0],
        }
    )


def test_cache_manager_init():
    """Test initialization of CacheManager."""
    with tempfile.TemporaryDirectory() as tmpdir:
        manager = cache.CacheManager(tmpdir)
        assert os.path.exists(tmpdir)
        assert manager.cache_dir == tmpdir


def test_cache_data_and_retrieve(sample_data):
    """Test caching data and retrieving it."""
    with tempfile.TemporaryDirectory() as tmpdir:
        manager = cache.CacheManager(tmpdir)

        # Initial check should return None as cache is empty
        cached = manager.get_cached_data(
            data_type="DISPATCHPRICE",
            start_date="2023/01/01",
            end_date="2023/01/02",
            regions=["NSW1"],
        )
        assert cached is None

        # Cache data
        manager.cache_data(
            data_type="DISPATCHPRICE",
            start_date="2023/01/01",
            end_date="2023/01/02",
            regions=["NSW1"],
            data=sample_data,
        )

        # This is just a placeholder test since our skeleton doesn't actually cache
        # In a real implementation, you would verify the data was cached
