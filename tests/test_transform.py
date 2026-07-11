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

    def test_missing_entity_ids_not_silently_dropped(self) -> None:
        """Rows with a missing entity id form their own group."""
        frame = _five_minute_frame()
        frame["REGIONID"] = ["NSW1"] * 6 + [None] * 6
        result = resample(frame, "1h", by="REGIONID")
        assert len(result) == 2
        assert result["RRP"].sum() == frame["RRP"].sum() / 6

    def test_single_region_with_missing_ids_needs_grouping(self) -> None:
        """A NaN id counts as a distinct entity for the ungrouped guard."""
        frame = _five_minute_frame()
        frame["REGIONID"] = ["NSW1"] * 6 + [None] * 6
        with pytest.raises(ValueError, match="REGIONID"):
            resample(frame, "1h")

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
        """Right-closed daily buckets align exactly to 04:00 boundaries.

        Hourly stamps May 1 01:00 .. May 3 00:00 with values 0..47:
        the bucket labelled May 1 04:00 covers stamps 01:00-04:00
        (values 0-3), the May 2 04:00 bucket covers the next 24 hours
        (values 4-27), the rest fall into the May 3 04:00 bucket.
        """
        stamps = pd.date_range("2026-05-01 01:00", periods=48, freq="1h")
        frame = pd.DataFrame(
            {"RRP": np.arange(48.0)},
            index=pd.DatetimeIndex(stamps, name="SETTLEMENTDATE"),
        )
        result = resample(frame, "1D", trading_day=True)
        assert list(result.index) == [
            pd.Timestamp("2026-05-01 04:00"),
            pd.Timestamp("2026-05-02 04:00"),
            pd.Timestamp("2026-05-03 04:00"),
        ]
        assert result["RRP"].tolist() == [1.5, 15.5, 37.5]

    def test_trading_day_rejects_sub_daily_rules(self) -> None:
        """The 04:00 offset makes no sense below daily aggregation."""
        with pytest.raises(ValueError, match="daily-or-coarser"):
            resample(_five_minute_frame(), "30min", trading_day=True)


class TestValidation:
    """Bad inputs fail with clear messages."""

    def test_non_datetime_index_rejected(self) -> None:
        """A RangeIndex frame cannot be resampled."""
        with pytest.raises(ValueError, match="interval-ending"):
            resample(pd.DataFrame({"RRP": [1.0]}), "30min")
