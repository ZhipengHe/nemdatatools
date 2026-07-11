"""Tests for Reports filename stamp parsing and window selection."""

import datetime

from nemdatatools.reports import _stamp_range


class TestStampRange:
    """Filenames carry one stamp (CURRENT) or two (weekly ARCHIVE bundles)."""

    def test_single_minute_stamp(self) -> None:
        """CURRENT payloads stamp publish minute plus an event id."""
        stamps = _stamp_range("PUBLIC_TRADINGIS_202607112235_0000000526970804.zip")
        assert stamps is not None
        assert stamps[0] == stamps[1] == datetime.datetime(2026, 7, 11, 22, 35)

    def test_daily_bundle_stamp(self) -> None:
        """DispatchIS ARCHIVE bundles carry one date stamp."""
        stamps = _stamp_range("PUBLIC_DISPATCHIS_20260709.zip")
        assert stamps is not None
        assert stamps[0] == stamps[1] == datetime.datetime(2026, 7, 9)

    def test_weekly_bundle_covers_range(self) -> None:
        """TradingIS ARCHIVE bundles are weekly with two date stamps.

        A request starting mid-week must still select the covering bundle,
        so the second stamp has to be read as the coverage end.
        """
        stamps = _stamp_range("PUBLIC_TRADINGIS_20260621_20260627.zip")
        assert stamps is not None
        assert stamps[0] == datetime.datetime(2026, 6, 21)
        assert stamps[1] == datetime.datetime(2026, 6, 27)

    def test_event_ids_are_not_stamps(self) -> None:
        """16-digit event ids must not be mistaken for timestamps."""
        stamps = _stamp_range("PUBLIC_NEXT_DAY_DISPATCH_20260710_0000000526847186.zip")
        assert stamps is not None
        assert stamps[0] == stamps[1] == datetime.datetime(2026, 7, 10)

    def test_unstamped_name_returns_none(self) -> None:
        """Names without any parsable stamp are skipped."""
        assert _stamp_range("DUPLICATE") is None
