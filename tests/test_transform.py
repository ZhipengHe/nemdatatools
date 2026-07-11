"""Tests for interval-ending resampling semantics."""

import numpy as np
import pandas as pd
import pytest

from nemdatatools.transform import resample


def _five_minute_frame() -> pd.DataFrame:
    """One hour of 5-minute interval-ending data, values 1..12."""
    stamps = pd.date_range("2026-05-01 00:05", periods=12, freq="5min")
    return pd.DataFrame(
        {"RRP": np.arange(1.0, 13.0)},
        index=pd.DatetimeIndex(stamps, name="SETTLEMENTDATE"),
    )


class TestIntervalEnding:
    """AEMO stamps mark interval ends; buckets must close on the right."""

    def test_half_hour_buckets_align_to_trading_periods(self) -> None:
        """The 00:30 bucket is the mean of rows stamped 00:05 to 00:30.

        This mirrors how AEMO derives 30-minute trading prices from the
        six 5-minute dispatch intervals of the trading period.
        """
        result = resample(_five_minute_frame(), "30min")
        assert list(result.index) == [
            pd.Timestamp("2026-05-01 00:30"),
            pd.Timestamp("2026-05-01 01:00"),
        ]
        assert result["RRP"].tolist() == [3.5, 9.5]  # mean(1..6), mean(7..12)

    def test_default_pandas_would_misalign(self) -> None:
        """Guard: the naive pandas default gives a different, wrong answer."""
        naive = _five_minute_frame().resample("30min").mean()
        correct = resample(_five_minute_frame(), "30min")
        assert not naive["RRP"].equals(correct["RRP"])


class TestEntityGrouping:
    """Frames holding several entities must not be averaged together."""

    def test_multi_region_without_by_raises(self) -> None:
        """Ungrouped multi-region frames are rejected loudly."""
        frame = _five_minute_frame()
        frame["REGIONID"] = ["NSW1", "QLD1"] * 6
        with pytest.raises(ValueError, match="REGIONID"):
            resample(frame, "30min")

    def test_by_region_keeps_series_separate(self) -> None:
        """Grouped resampling aggregates each region independently."""
        frame = _five_minute_frame()
        frame["REGIONID"] = ["NSW1", "QLD1"] * 6
        result = resample(frame, "1h", by="REGIONID")
        nsw = result[result["REGIONID"] == "NSW1"]["RRP"].iloc[0]
        qld = result[result["REGIONID"] == "QLD1"]["RRP"].iloc[0]
        assert nsw == np.mean([1, 3, 5, 7, 9, 11])
        assert qld == np.mean([2, 4, 6, 8, 10, 12])


class TestTradingDay:
    """Daily aggregates can align to the 04:00-04:00 NEM trading day."""

    def test_trading_day_buckets_offset_to_0400(self) -> None:
        """Bucket edges land on 04:00 when trading_day is set."""
        stamps = pd.date_range("2026-05-01 01:00", periods=48, freq="1h")
        frame = pd.DataFrame(
            {"RRP": np.ones(48)},
            index=pd.DatetimeIndex(stamps, name="SETTLEMENTDATE"),
        )
        result = resample(frame, "1D", trading_day=True)
        assert all(ts.hour == 4 for ts in result.index)


class TestValidation:
    """Bad inputs fail with clear messages."""

    def test_non_datetime_index_rejected(self) -> None:
        """A RangeIndex frame cannot be resampled."""
        with pytest.raises(ValueError, match="interval-ending"):
            resample(pd.DataFrame({"RRP": [1.0]}), "30min")
