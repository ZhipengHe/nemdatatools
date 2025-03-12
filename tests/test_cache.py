"""Tests for the cache module."""

import datetime
import json
import os
import tempfile
from pathlib import Path

import pandas as pd
import pytest

from nemdatatools import cache


@pytest.fixture
def sample_data():
    """Create sample data for testing."""
    return pd.DataFrame(
        {
            "SETTLEMENTDATE": pd.to_datetime(
                [
                    "2023/01/01 00:05:00",
                    "2023/01/01 00:10:00",
                    "2023/01/01 12:00:00",
                    "2023/01/02 00:00:00",
                ],
            ),
            "REGIONID": ["NSW1", "QLD1", "NSW1", "VIC1"],
            "RRP": [25.0, 30.0, 40.0, 35.0],
        },
    )


@pytest.fixture
def cache_manager():
    """Create a temporary cache manager for testing."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield cache.CacheManager(tmpdir)


def test_cache_manager_init():
    """Test initialization of CacheManager."""
    with tempfile.TemporaryDirectory() as tmpdir:
        manager = cache.CacheManager(tmpdir)
        assert os.path.exists(tmpdir)
        assert manager.cache_dir == tmpdir
        assert os.path.exists(os.path.join(tmpdir, "metadata"))
        assert os.path.exists(os.path.join(tmpdir, "metadata", "index.json"))

        # Check index file structure
        with open(os.path.join(tmpdir, "metadata", "index.json")) as f:
            index = json.load(f)
        assert "last_updated" in index
        assert "entries" in index
        assert isinstance(index["entries"], dict)


def test_generate_cache_key(cache_manager):
    """Test cache key generation."""
    data_type = "DISPATCHPRICE"
    start_date = datetime.datetime(2023, 1, 1)
    end_date = datetime.datetime(2023, 1, 2)
    regions = ["NSW1", "QLD1"]

    key1 = cache_manager._generate_cache_key(data_type, start_date, end_date, regions)
    key2 = cache_manager._generate_cache_key(data_type, start_date, end_date, regions)

    # Same parameters should generate same key
    assert key1 == key2

    # Different parameters should generate different keys
    key3 = cache_manager._generate_cache_key(data_type, start_date, end_date, ["NSW1"])
    assert key1 != key3


def test_cache_data_and_retrieve(cache_manager, sample_data):
    """Test caching data and retrieving it."""
    data_type = "DISPATCHPRICE"
    start_date = datetime.datetime(2023, 1, 1)
    end_date = datetime.datetime(2023, 1, 2)
    regions = ["NSW1", "QLD1"]

    # Initial check should return None as cache is empty
    cached = cache_manager.get_cached_data(
        data_type=data_type,
        start_date=start_date,
        end_date=end_date,
        regions=regions,
    )
    assert cached is None

    # Cache data
    cache_manager.cache_data(
        data_type=data_type,
        start_date=start_date,
        end_date=end_date,
        regions=regions,
        data=sample_data,
    )

    # Retrieve cached data
    cached = cache_manager.get_cached_data(
        data_type=data_type,
        start_date=start_date,
        end_date=end_date,
        regions=regions,
    )

    # Verify data was cached and retrieved correctly
    assert cached is not None
    assert len(cached) == len(sample_data)
    assert set(cached.columns) == set(sample_data.columns)

    # Check metadata was created
    cache_key = cache_manager._generate_cache_key(
        data_type,
        start_date,
        end_date,
        regions,
    )
    metadata_path = cache_manager._get_metadata_path(cache_key)
    assert os.path.exists(metadata_path)

    # Check index was updated
    with open(cache_manager.metadata_index_path) as f:
        index = json.load(f)
    assert cache_key in index["entries"]


def test_empty_data_not_cached(cache_manager):
    """Test that empty DataFrames are not cached."""
    empty_df = pd.DataFrame()

    cache_manager.cache_data(
        data_type="DISPATCHPRICE",
        start_date=datetime.datetime(2023, 1, 1),
        end_date=datetime.datetime(2023, 1, 2),
        regions=["NSW1"],
        data=empty_df,
    )

    # Check that no cache file was created
    cache_files = list(Path(cache_manager.cache_dir).glob("*.parquet"))
    assert len(cache_files) == 0


def test_partial_cache_match(cache_manager, sample_data):
    """Test retrieving data with partially overlapping cached entries."""
    # Split sample data into two parts
    data1 = sample_data[sample_data["REGIONID"] == "NSW1"]
    data2 = sample_data[sample_data["REGIONID"] == "QLD1"]

    # Cache the two parts separately
    cache_manager.cache_data(
        data_type="DISPATCHPRICE",
        start_date=datetime.datetime(2023, 1, 1),
        end_date=datetime.datetime(2023, 1, 2),
        regions=["NSW1"],
        data=data1,
    )

    cache_manager.cache_data(
        data_type="DISPATCHPRICE",
        start_date=datetime.datetime(2023, 1, 1),
        end_date=datetime.datetime(2023, 1, 2),
        regions=["QLD1"],
        data=data2,
    )

    # Try to retrieve combined data
    result = cache_manager.get_cached_data(
        data_type="DISPATCHPRICE",
        start_date=datetime.datetime(2023, 1, 1),
        end_date=datetime.datetime(2023, 1, 2),
        regions=["NSW1", "QLD1"],
    )

    # Should not get full data since caches are for individual regions
    assert result is None


def test_date_filtering(cache_manager, sample_data):
    """Test retrieving data for a specific date range."""
    # Cache full data
    cache_manager.cache_data(
        data_type="DISPATCHPRICE",
        start_date=datetime.datetime(2023, 1, 1),
        end_date=datetime.datetime(2023, 1, 2),
        regions=["NSW1", "QLD1", "VIC1"],
        data=sample_data,
    )

    # Retrieve data for a narrower date range
    result = cache_manager.get_cached_data(
        data_type="DISPATCHPRICE",
        start_date=datetime.datetime(2023, 1, 1, 6, 0),  # Jan 1st, 6:00 AM
        end_date=datetime.datetime(2023, 1, 1, 18, 0),  # Jan 1st, 6:00 PM
        regions=["NSW1", "QLD1", "VIC1"],
    )

    # Should get only the data for Jan 1st, 12:00
    assert result is not None
    assert len(result) == 1
    assert result.iloc[0]["SETTLEMENTDATE"] == pd.Timestamp("2023-01-01 12:00:00")


def test_clear_cache(cache_manager, sample_data):
    """Test clearing cache entries."""
    # Cache some data
    cache_manager.cache_data(
        data_type="DISPATCHPRICE",
        start_date=datetime.datetime(2023, 1, 1),
        end_date=datetime.datetime(2023, 1, 2),
        regions=["NSW1", "QLD1"],
        data=sample_data,
    )

    # Verify data is cached
    assert (
        cache_manager.get_cached_data(
            data_type="DISPATCHPRICE",
            start_date=datetime.datetime(2023, 1, 1),
            end_date=datetime.datetime(2023, 1, 2),
            regions=["NSW1", "QLD1"],
        )
        is not None
    )

    # Clear cache
    cleared = cache_manager.clear_cache()
    assert cleared == 1

    # Verify data is no longer cached
    assert (
        cache_manager.get_cached_data(
            data_type="DISPATCHPRICE",
            start_date=datetime.datetime(2023, 1, 1),
            end_date=datetime.datetime(2023, 1, 2),
            regions=["NSW1", "QLD1"],
        )
        is None
    )

    # Index should be empty
    with open(cache_manager.metadata_index_path) as f:
        index = json.load(f)
    assert len(index["entries"]) == 0


def test_clear_cache_missing_index(cache_manager):
    """Test clearing cache when metadata index file doesn't exist."""
    # Delete the index file
    if os.path.exists(cache_manager.metadata_index_path):
        os.remove(cache_manager.metadata_index_path)

    # Try to clear cache
    cleared = cache_manager.clear_cache()

    # Should return 0 since there's no index file
    assert cleared == 0


def test_clear_cache_with_date_filter(cache_manager, sample_data):
    """Test clearing cache entries older than a specified date."""
    # Create two cache entries with different access times

    # Entry 1 - Recent
    cache_manager.cache_data(
        data_type="DISPATCHPRICE",
        start_date=datetime.datetime(2023, 1, 1),
        end_date=datetime.datetime(2023, 1, 2),
        regions=["NSW1"],
        data=sample_data,
    )

    recent_key = cache_manager._generate_cache_key(
        "DISPATCHPRICE",
        datetime.datetime(2023, 1, 1),
        datetime.datetime(2023, 1, 2),
        ["NSW1"],
    )

    # Entry 2 - Old
    cache_manager.cache_data(
        data_type="DISPATCHPRICE",
        start_date=datetime.datetime(2023, 2, 1),
        end_date=datetime.datetime(2023, 2, 2),
        regions=["QLD1"],
        data=sample_data,
    )

    old_key = cache_manager._generate_cache_key(
        "DISPATCHPRICE",
        datetime.datetime(2023, 2, 1),
        datetime.datetime(2023, 2, 2),
        ["QLD1"],
    )

    # Modify the old entry's last_accessed time to be in the past
    old_metadata_path = cache_manager._get_metadata_path(old_key)
    with open(old_metadata_path) as f:
        old_metadata = json.load(f)

    old_time = (datetime.datetime.now() - datetime.timedelta(days=30)).isoformat()
    old_metadata["last_accessed"] = old_time

    with open(old_metadata_path, "w") as f:
        json.dump(old_metadata, f)

    # Update index as well
    with open(cache_manager.metadata_index_path) as f:
        index = json.load(f)

    index["entries"][old_key]["last_accessed"] = old_time

    with open(cache_manager.metadata_index_path, "w") as f:
        json.dump(index, f)

    # Clear entries older than 15 days
    cutoff_date = datetime.datetime.now() - datetime.timedelta(days=15)
    cleared = cache_manager.clear_cache(older_than=cutoff_date)

    # Should only clear the old entry
    assert cleared == 1

    # Verify recent entry still exists
    with open(cache_manager.metadata_index_path) as f:
        index = json.load(f)

    assert recent_key in index["entries"]
    assert old_key not in index["entries"]

    # Check that the recent entry's data can still be retrieved
    recent_data = cache_manager.get_cached_data(
        data_type="DISPATCHPRICE",
        start_date=datetime.datetime(2023, 1, 1),
        end_date=datetime.datetime(2023, 1, 2),
        regions=["NSW1"],
    )
    assert recent_data is not None


def test_get_cache_info(cache_manager, sample_data):
    """Test getting cache information."""
    # Initially cache should be empty
    info = cache_manager.get_cache_info()
    assert info["entry_count"] == 0
    assert info["total_size_mb"] == 0

    # Cache some data
    cache_manager.cache_data(
        data_type="DISPATCHPRICE",
        start_date=datetime.datetime(2023, 1, 1),
        end_date=datetime.datetime(2023, 1, 2),
        regions=["NSW1", "QLD1"],
        data=sample_data,
    )

    # Check updated info
    info = cache_manager.get_cache_info()
    assert info["entry_count"] == 1
    assert info["total_size_mb"] > 0
    assert "last_updated" in info


def test_get_cache_info_missing_index(cache_manager):
    """Test getting cache info when metadata index file doesn't exist."""
    # Delete the metadata index file
    if os.path.exists(cache_manager.metadata_index_path):
        os.remove(cache_manager.metadata_index_path)

    # Get cache info
    info = cache_manager.get_cache_info()

    # Should return default values
    assert info["entry_count"] == 0
    assert info["total_size_mb"] == 0
    assert info["last_updated"] is None


def test_update_access_time(cache_manager, sample_data):
    """Test updating access time when retrieving data."""
    # Cache data
    cache_manager.cache_data(
        data_type="DISPATCHPRICE",
        start_date=datetime.datetime(2023, 1, 1),
        end_date=datetime.datetime(2023, 1, 2),
        regions=["NSW1", "QLD1"],
        data=sample_data,
    )

    # Get the cache key
    cache_key = cache_manager._generate_cache_key(
        "DISPATCHPRICE",
        datetime.datetime(2023, 1, 1),
        datetime.datetime(2023, 1, 2),
        ["NSW1", "QLD1"],
    )

    # Get initial metadata
    with open(cache_manager._get_metadata_path(cache_key)) as f:
        initial_metadata = json.load(f)

    initial_time = initial_metadata["last_accessed"]

    # Wait a bit to ensure time difference
    import time

    time.sleep(0.1)

    # Retrieve data (should update access time)
    cache_manager.get_cached_data(
        data_type="DISPATCHPRICE",
        start_date=datetime.datetime(2023, 1, 1),
        end_date=datetime.datetime(2023, 1, 2),
        regions=["NSW1", "QLD1"],
    )

    # Check if access time was updated
    with open(cache_manager._get_metadata_path(cache_key)) as f:
        updated_metadata = json.load(f)

    updated_time = updated_metadata["last_accessed"]

    # Access time should be updated
    assert initial_time != updated_time


def test_different_data_types(cache_manager, sample_data):
    """Test caching different data types."""
    # Cache data with one data type
    cache_manager.cache_data(
        data_type="DISPATCHPRICE",
        start_date=datetime.datetime(2023, 1, 1),
        end_date=datetime.datetime(2023, 1, 2),
        regions=["NSW1", "QLD1"],
        data=sample_data,
    )

    # Try to retrieve with a different data type
    result = cache_manager.get_cached_data(
        data_type="DISPATCHREGIONSUM",  # Different data type
        start_date=datetime.datetime(2023, 1, 1),
        end_date=datetime.datetime(2023, 1, 2),
        regions=["NSW1", "QLD1"],
    )

    # Should not find a match
    assert result is None


def test_missing_metadata_file_scenarios(cache_manager, sample_data):
    """Test behavior when metadata index file doesn't exist."""
    # Delete the metadata index file
    if os.path.exists(cache_manager.metadata_index_path):
        os.remove(cache_manager.metadata_index_path)

    # Test 1: find_matching_cache_entries should return empty list
    # when index doesn't exist
    matching_entries = cache_manager._find_matching_cache_entries(
        data_type="DISPATCHPRICE",
        start_date=datetime.datetime(2023, 1, 1),
        end_date=datetime.datetime(2023, 1, 2),
        regions=["NSW1", "QLD1"],
    )
    # Should return empty list when index file doesn't exist
    assert matching_entries == []
    assert not os.path.exists(cache_manager.metadata_index_path)

    # Test 2: _update_metadata_index should create a new index file
    # Create a test metadata dict
    metadata = {
        "cache_key": "test_key",
        "data_type": "DISPATCHPRICE",
        "start_date": datetime.datetime(2023, 1, 1).isoformat(),
        "end_date": datetime.datetime(2023, 1, 2).isoformat(),
        "regions": ["NSW1", "QLD1"],
        "created": datetime.datetime.now().isoformat(),
        "last_accessed": datetime.datetime.now().isoformat(),
    }

    # Update the index with this metadata (should create the file)
    cache_manager._update_metadata_index("test_key", metadata)

    # Verify that the index file was created
    assert os.path.exists(cache_manager.metadata_index_path)

    # Check the content of the created index file
    with open(cache_manager.metadata_index_path) as f:
        index = json.load(f)

    assert "last_updated" in index
    assert "entries" in index
    assert "test_key" in index["entries"]

    # Test 3: After creating the index, find_matching_cache_entries should now work
    # But since we don't have the actual metadata file for the entry, it won't match
    matching_entries = cache_manager._find_matching_cache_entries(
        data_type="DISPATCHPRICE",
        start_date=datetime.datetime(2023, 1, 1),
        end_date=datetime.datetime(2023, 1, 2),
        regions=["NSW1", "QLD1"],
    )
    # Should still be empty because the metadata file doesn't exist
    assert matching_entries == []


def test_matching_keys_without_parquet_files(cache_manager, sample_data):
    """Test when matching keys are found but parquet files don't exist."""
    # Cache some data
    cache_manager.cache_data(
        data_type="DISPATCHPRICE",
        start_date=datetime.datetime(2023, 1, 1),
        end_date=datetime.datetime(2023, 1, 2),
        regions=["NSW1", "QLD1"],
        data=sample_data,
    )

    # Get the cache key
    cache_key = cache_manager._generate_cache_key(
        "DISPATCHPRICE",
        datetime.datetime(2023, 1, 1),
        datetime.datetime(2023, 1, 2),
        ["NSW1", "QLD1"],
    )

    # Delete the parquet file but keep the metadata
    cache_path = cache_manager._get_cache_path(cache_key)
    os.remove(cache_path)

    # Attempt to retrieve the data
    result = cache_manager.get_cached_data(
        data_type="DISPATCHPRICE",
        start_date=datetime.datetime(2023, 1, 1),
        end_date=datetime.datetime(2023, 1, 2),
        regions=["NSW1", "QLD1"],
    )

    # Should return None because the parquet file doesn't exist
    assert result is None
