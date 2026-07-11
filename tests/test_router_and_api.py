"""Offline tests for gap checking, catalog lookup, and the API surface."""

import datetime
import io
import zipfile
from pathlib import Path

import pytest

from nemdatatools import availability, tables
from nemdatatools.cache import Cache
from nemdatatools.catalog import get_table
from nemdatatools.errors import AvailabilityGapError
from nemdatatools.router import _check_gaps, fetch

FIXTURES = Path(__file__).parent / "fixtures"


class TestGapChecking:
    """Ranges crossing known holes fail loudly with the substitute named."""

    def test_bid_gap_raises_with_substitute(self) -> None:
        """A 2022 BIDPEROFFER_D request names the raw-offer substitute."""
        spec = get_table("BIDPEROFFER_D")
        with pytest.raises(AvailabilityGapError, match="BIDPEROFFER"):
            _check_gaps(
                spec,
                datetime.datetime(2022, 1, 1),
                datetime.datetime(2022, 2, 1),
            )

    def test_range_before_gap_passes(self) -> None:
        """A 2020 request predates the five-minute-settlement gap."""
        spec = get_table("BIDPEROFFER_D")
        _check_gaps(
            spec,
            datetime.datetime(2020, 1, 1),
            datetime.datetime(2021, 2, 28),
        )

    def test_decommissioned_table_open_gap(self) -> None:
        """TRADINGREGIONSUM after 2021-11 is gone for good."""
        spec = get_table("TRADINGREGIONSUM")
        with pytest.raises(AvailabilityGapError, match="DISPATCHREGIONSUM"):
            _check_gaps(
                spec,
                datetime.datetime(2023, 1, 1),
                datetime.datetime(2023, 1, 2),
            )


class TestApiSurface:
    """Catalog lookups and discovery helpers."""

    def test_tables_lists_all_families(self) -> None:
        """Curated names cover prices, generation, forecasts, and bids."""
        names = tables()
        for expected in (
            "DISPATCHPRICE",
            "TRADINGPRICE",
            "DISPATCH_UNIT_SCADA",
            "P5MIN_REGIONSOLUTION",
            "BIDPEROFFER_D",
        ):
            assert expected in names

    def test_availability_reports_gaps(self) -> None:
        """The availability view exposes gap bounds and reasons."""
        info = availability("TRADINGREGIONSUM")
        assert info["gaps"] and info["gaps"][0]["to"] == "ongoing"

    def test_unknown_table_lists_alternatives(self) -> None:
        """Unknown names raise KeyError carrying the curated list."""
        with pytest.raises(KeyError, match="DISPATCHPRICE"):
            get_table("NOT_A_TABLE")

    def test_lookup_is_case_insensitive(self) -> None:
        """Lower-case names resolve to the same spec."""
        assert get_table("dispatchprice").name == "DISPATCHPRICE"

    def test_end_before_start_rejected(self) -> None:
        """Reversed ranges are a caller error, not an empty result."""
        with pytest.raises(ValueError, match="before start"):
            fetch("DISPATCHPRICE", "2026/01/02", "2026/01/01")

    def test_price_and_demand_rejects_reversed_range(self) -> None:
        """The aggregated-CSV path validates ranges like fetch() does."""
        from nemdatatools import fetch_price_and_demand

        with pytest.raises(ValueError, match="before start"):
            fetch_price_and_demand("2026/01/02", "2026/01/01")

    def test_missing_key_column_raises_not_collapses(self) -> None:
        """De-duplication never silently falls back to time-only identity."""
        import pandas as pd

        from nemdatatools.errors import CoverageError
        from nemdatatools.router import _finalise

        spec = get_table("DISPATCHPRICE")
        data = pd.DataFrame(
            {
                "SETTLEMENTDATE": ["2026/05/01 00:05:00"] * 2,
                "REGIONID": ["QLD1", "QLD1"],
                "RRP": [50.0, 60.0],
                # RUNNO and INTERVENTION absent
            },
        )
        with pytest.raises(CoverageError, match="RUNNO"):
            _finalise(
                spec,
                data,
                datetime.datetime(2026, 5, 1),
                datetime.datetime(2026, 5, 2),
                None,
            )


class TestCacheParquet:
    """Parsed tables round-trip through the parquet layer."""

    def test_second_read_skips_download(self, tmp_path: Path) -> None:
        """After one load, the raw file can vanish and reads still work."""
        payload = io.BytesIO()
        with zipfile.ZipFile(payload, "w") as zf:
            zf.write(FIXTURES / "tradingis_sample.csv", "sample.CSV")
        url = "https://nemweb.com.au/Reports/Current/TradingIS_Reports/X.zip"

        cache = Cache(tmp_path)
        raw_target = cache._raw_path(url)
        raw_target.parent.mkdir(parents=True)
        raw_target.write_bytes(payload.getvalue())

        first = cache.load_table(url, ("TRADING", "PRICE"))
        assert not first.empty
        raw_target.unlink()
        second = cache.load_table(url, ("TRADING", "PRICE"))
        assert second.equals(first)

    def test_absent_segment_memoized_empty(self, tmp_path: Path) -> None:
        """Asking for a table the payload lacks yields a cached empty frame."""
        payload = io.BytesIO()
        with zipfile.ZipFile(payload, "w") as zf:
            zf.write(FIXTURES / "tradingis_sample.csv", "sample.CSV")
        url = "https://nemweb.com.au/Reports/Current/TradingIS_Reports/Y.zip"

        cache = Cache(tmp_path)
        raw_target = cache._raw_path(url)
        raw_target.parent.mkdir(parents=True)
        raw_target.write_bytes(payload.getvalue())

        missing = cache.load_table(url, ("DISPATCH", "PRICE"))
        assert missing.empty
        raw_target.unlink()
        assert cache.load_table(url, ("DISPATCH", "PRICE")).empty
