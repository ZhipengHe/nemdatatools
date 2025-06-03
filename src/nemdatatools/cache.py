"""NEMDataTools - Module for caching AEMO data.

This module provides functions for caching downloaded AEMO data
to avoid redundant requests.
"""

import datetime
import hashlib
import json
import logging
import os

import pandas as pd

logger = logging.getLogger(__name__)

# Constants


class CacheManager:
    """Manages local caching of AEMO data.

    This class provides methods to cache and retrieve data
    downloaded from the Australian Energy Market Operator (AEMO).
    """

    def __init__(self, cache_dir: str):
        """Initialize the cache manager.

        Args:
            cache_dir: Directory path for cache storage

        """
        self.cache_dir = cache_dir
        os.makedirs(cache_dir, exist_ok=True)
        logger.info(f"Initialized cache manager with directory: {cache_dir}")

        # Create metadata directory if it doesn't exist
        self.metadata_dir = os.path.join(cache_dir, "metadata")
        os.makedirs(self.metadata_dir, exist_ok=True)

        # Initialize metadata index if it doesn't exist
        self.metadata_index_path = os.path.join(self.metadata_dir, "index.json")
        if not os.path.exists(self.metadata_index_path):
            self._initialize_metadata_index()

    def _initialize_metadata_index(self) -> None:
        """Initialize the metadata index file."""
        index = {"last_updated": datetime.datetime.now().isoformat(), "entries": {}}
        with open(self.metadata_index_path, "w") as f:
            json.dump(index, f, indent=2)

    def _generate_cache_key(
        self,
        data_type: str,
        start_date: datetime.datetime,
        end_date: datetime.datetime,
        regions: list[str],
    ) -> str:
        """Generate a unique cache key based on parameters.

        Args:
            data_type: Type of data
            start_date: Start date
            end_date: End date
            regions: List of regions

        Returns:
            Unique cache key string

        """
        # Create a string with all parameters
        regions_str = "_".join(sorted(regions))
        param_str = (
            f"{data_type}_{start_date.isoformat()}_{end_date.isoformat()}_{regions_str}"
        )

        # Generate a hash of the parameter string
        hash_obj = hashlib.sha256(param_str.encode())
        return hash_obj.hexdigest()

    def _get_cache_path(self, cache_key: str) -> str:
        """Get the file path for a cache entry.

        Args:
            cache_key: Unique cache key

        Returns:
            Path to cache file

        """
        return os.path.join(self.cache_dir, f"{cache_key}.parquet")

    def _get_metadata_path(self, cache_key: str) -> str:
        """Get the file path for a cache entry's metadata.

        Args:
            cache_key: Unique cache key

        Returns:
            Path to metadata file

        """
        return os.path.join(self.metadata_dir, f"{cache_key}.json")

    def _save_metadata(
        self,
        cache_key: str,
        data_type: str,
        start_date: datetime.datetime,
        end_date: datetime.datetime,
        regions: list[str],
    ) -> None:
        """Save metadata for a cache entry.

        Args:
            cache_key: Unique cache key
            data_type: Type of data
            start_date: Start date
            end_date: End date
            regions: List of regions

        """
        metadata = {
            "cache_key": cache_key,
            "data_type": data_type,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "regions": regions,
            "created": datetime.datetime.now().isoformat(),
            "last_accessed": datetime.datetime.now().isoformat(),
        }

        # Save metadata file
        with open(self._get_metadata_path(cache_key), "w") as f:
            json.dump(metadata, f, indent=2)

        # Update metadata index
        self._update_metadata_index(cache_key, metadata)

    def _update_metadata_index(self, cache_key: str, metadata: dict) -> None:
        """Update the metadata index with a new or updated entry.

        Args:
            cache_key: Unique cache key
            metadata: Metadata dictionary

        """
        if os.path.exists(self.metadata_index_path):
            with open(self.metadata_index_path) as f:
                index = json.load(f)
        else:
            index = {"last_updated": "", "entries": {}}

        # Update index
        index["entries"][cache_key] = {
            "data_type": metadata["data_type"],
            "start_date": metadata["start_date"],
            "end_date": metadata["end_date"],
            "created": metadata["created"],
            "last_accessed": metadata["last_accessed"],
        }
        index["last_updated"] = datetime.datetime.now().isoformat()

        # Save updated index
        with open(self.metadata_index_path, "w") as f:
            json.dump(index, f, indent=2)

    def _update_access_time(self, cache_key: str) -> None:
        """Update the last accessed time for a cache entry.

        Args:
            cache_key: Unique cache key

        """
        metadata_path = self._get_metadata_path(cache_key)
        if os.path.exists(metadata_path):
            with open(metadata_path) as f:
                metadata = json.load(f)

            metadata["last_accessed"] = datetime.datetime.now().isoformat()

            with open(metadata_path, "w") as f:
                json.dump(metadata, f, indent=2)

            # Update index as well
            self._update_metadata_index(cache_key, metadata)

    def _find_matching_cache_entries(
        self,
        data_type: str,
        start_date: datetime.datetime,
        end_date: datetime.datetime,
        regions: list[str],
    ) -> list[str]:
        """Find cache entries that overlap with the requested date range.

        Args:
            data_type: Type of data
            start_date: Start date
            end_date: End date
            regions: List of regions

        Returns:
            List of cache keys for matching entries

        """
        if not os.path.exists(self.metadata_index_path):
            return []

        with open(self.metadata_index_path) as f:
            index = json.load(f)

        matching_keys = []

        for cache_key, entry in index.get("entries", {}).items():
            # Only consider entries of the same data type
            if entry["data_type"] != data_type:
                continue

            # Parse dates
            entry_start = datetime.datetime.fromisoformat(entry["start_date"])
            entry_end = datetime.datetime.fromisoformat(entry["end_date"])

            # Check for overlap
            if entry_start <= end_date and entry_end >= start_date:
                # Check regions - only consider if all requested regions are included
                metadata_path = self._get_metadata_path(cache_key)
                if os.path.exists(metadata_path):
                    with open(metadata_path) as f:
                        metadata = json.load(f)

                    entry_regions = set(metadata.get("regions", []))
                    requested_regions = set(regions)

                    if requested_regions.issubset(entry_regions):
                        matching_keys.append(cache_key)

        return matching_keys

    # TODO: Currently not used, but keep it here for future reference
    def _get_date_column_for_data_type(self, data_type: str) -> str:
        """Get the appropriate date column name for a data type.

        Args:
            data_type: Type of data

        Returns:
            Column name for dates

        """
        # This mapping should be based on actual AEMO data structure
        # TODO: Move this to a constant or configuration file later
        column_map = {
            "DISPATCHPRICE": "SETTLEMENTDATE",
            "DISPATCHREGIONSUM": "SETTLEMENTDATE",
            "PREDISPATCH": "DATETIME",
            "P5MIN": "INTERVAL_DATETIME",
        }

        return column_map.get(data_type, "SETTLEMENTDATE")

    def _get_region_column_for_data_type(self, data_type: str) -> str:
        """Get the appropriate region column name for a data type.

        Args:
            data_type: Type of data

        Returns:
            Column name for regions

        """
        # Most AEMO data uses REGIONID
        return "REGIONID"

    def get_cached_data(
        self,
        data_type: str,
        start_date: datetime.datetime,
        end_date: datetime.datetime,
        regions: list[str],
    ) -> pd.DataFrame | None:
        """Get cached data for the specified parameters.

        Args:
            data_type: Type of data
            start_date: Start date
            end_date: End date
            regions: List of regions

        Returns:
            DataFrame with cached data or None if not available

        """
        # First check for exact match
        cache_key = self._generate_cache_key(data_type, start_date, end_date, regions)
        cache_path = self._get_cache_path(cache_key)

        if os.path.exists(cache_path):
            logger.info(
                f"Exact cache match found for {data_type} from "
                f"{start_date} to {end_date}",
            )
            self._update_access_time(cache_key)
            return pd.read_parquet(cache_path)

        # Check for overlapping entries
        matching_keys = self._find_matching_cache_entries(
            data_type,
            start_date,
            end_date,
            regions,
        )

        if not matching_keys:
            logger.info(
                f"No cache match found for {data_type} from {start_date} to {end_date}",
            )
            return None

        # If matches found, combine them
        logger.info(f"Found {len(matching_keys)} overlapping cache entries")

        # Read and combine data from matching entries
        dfs = []
        for key in matching_keys:
            path = self._get_cache_path(key)
            if os.path.exists(path):
                self._update_access_time(key)
                df = pd.read_parquet(path)
                dfs.append(df)

        if not dfs:
            return None

        # Combine dataframes and filter for requested date range and regions
        # Notice that you cannot remove indexes after standardizing
        combined_df = pd.concat(dfs, ignore_index=False)

        # Filter for date range - handle both single and multi-index cases
        # TODO: This is a hack to handle the fact that the index is a multi-index
        #       for PREDISPATCH data (need to handle this properly in the future)
        if isinstance(combined_df.index, pd.MultiIndex):
            # For multi-index, get the first level which should be the date
            date_level = combined_df.index.get_level_values(0)
            combined_df = combined_df[
                (date_level >= start_date) & (date_level <= end_date)
            ]
        else:
            # For single index
            combined_df = combined_df[
                (combined_df.index >= start_date) & (combined_df.index <= end_date)
            ]

        # Filter for regions
        region_column = self._get_region_column_for_data_type(data_type)
        if region_column in combined_df.columns:
            combined_df = combined_df[combined_df[region_column].isin(regions)]

        # Remove duplicates
        combined_df = combined_df.drop_duplicates()

        return combined_df

    def cache_data(
        self,
        data_type: str,
        start_date: datetime.datetime,
        end_date: datetime.datetime,
        regions: list[str],
        data: pd.DataFrame,
    ) -> None:
        """Cache the provided data.

        Args:
            data_type: Type of data
            start_date: Start date
            end_date: End date
            regions: List of regions
            data: DataFrame to cache

        """
        if data.empty:
            logger.warning(f"Not caching empty data for {data_type}")
            return

        cache_key = self._generate_cache_key(data_type, start_date, end_date, regions)
        cache_path = self._get_cache_path(cache_key)

        # Save data to parquet file
        data.to_parquet(cache_path, index=True)

        # Save metadata
        self._save_metadata(cache_key, data_type, start_date, end_date, regions)

        logger.info(
            f"Cached {len(data)} rows of {data_type} data "
            f"from {start_date} to {end_date}",
        )

    def clear_cache(self, older_than: datetime.datetime | None = None) -> int:
        """Clear cache entries.

        Args:
            older_than: If provided, only clear entries older than this date

        Returns:
            Number of entries cleared

        """
        if not os.path.exists(self.metadata_index_path):
            return 0

        with open(self.metadata_index_path) as f:
            index = json.load(f)

        entries_to_remove = []

        for cache_key, entry in index.get("entries", {}).items():
            if older_than is not None:
                # Convert last accessed time to datetime
                last_accessed = datetime.datetime.fromisoformat(entry["last_accessed"])

                # Skip if entry is newer than cutoff
                if last_accessed >= older_than:
                    continue

            entries_to_remove.append(cache_key)

        # Remove entries
        for cache_key in entries_to_remove:
            cache_path = self._get_cache_path(cache_key)
            metadata_path = self._get_metadata_path(cache_key)

            # Remove files if they exist
            if os.path.exists(cache_path):
                os.remove(cache_path)

            if os.path.exists(metadata_path):
                os.remove(metadata_path)

            # Remove from index
            if cache_key in index["entries"]:
                del index["entries"][cache_key]

        # Update index
        index["last_updated"] = datetime.datetime.now().isoformat()
        with open(self.metadata_index_path, "w") as f:
            json.dump(index, f, indent=2)

        logger.info(f"Cleared {len(entries_to_remove)} cache entries")
        return len(entries_to_remove)

    def get_cache_info(self) -> dict:
        """Get information about the current cache state.

        Returns:
            Dictionary with cache information

        """
        if not os.path.exists(self.metadata_index_path):
            return {"entry_count": 0, "total_size_mb": 0, "last_updated": None}

        with open(self.metadata_index_path) as f:
            index = json.load(f)

        # Get total size of cache files
        total_size = 0
        for cache_key in index.get("entries", {}).keys():
            cache_path = self._get_cache_path(cache_key)
            if os.path.exists(cache_path):
                total_size += os.path.getsize(cache_path)

        return {
            "entry_count": len(index.get("entries", {})),
            "total_size_mb": total_size / (1024 * 1024),
            "last_updated": index.get("last_updated"),
        }
