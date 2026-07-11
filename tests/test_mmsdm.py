"""Tests for MMSDM era-aware file matching, against survey-verified names."""

import datetime

from nemdatatools.catalog import get_table
from nemdatatools.listing import ListingEntry
from nemdatatools.mmsdm import match_table_zips, months_between


def _entries(*names: str) -> list[ListingEntry]:
    """Wrap filenames as listing entries."""
    return [ListingEntry(name=n, url=f"https://x/{n}", is_dir=False) for n in names]


class TestDvdEra:
    """Pre-2024-08 snapshots use PUBLIC_DVD_<TABLE>_<stamp>.zip names."""

    def test_matches_plain_table(self) -> None:
        """DISPATCHPRICE matches its DVD filename exactly."""
        entries = _entries(
            "PUBLIC_DVD_DISPATCHPRICE_202001010000.zip",
            "PUBLIC_DVD_DISPATCHREGIONSUM_202001010000.zip",
        )
        spec = get_table("DISPATCHPRICE")
        assert [e.name for e in match_table_zips(entries, spec, 2020, 1)] == [
            "PUBLIC_DVD_DISPATCHPRICE_202001010000.zip",
        ]

    def test_p5min_requires_all_suffix(self) -> None:
        """DVD-era P5MIN files carry an _ALL suffix the spec must match."""
        entries = _entries(
            "PUBLIC_DVD_P5MIN_REGIONSOLUTION_ALL_202001010000.zip",
        )
        spec = get_table("P5MIN_REGIONSOLUTION")
        assert len(match_table_zips(entries, spec, 2020, 1)) == 1

    def test_numbered_splits_all_matched(self) -> None:
        """DVD-era PREDISPATCHLOAD is split as LOAD1 + LOAD2."""
        entries = _entries(
            "PUBLIC_DVD_PREDISPATCHLOAD1_202001010000.zip",
            "PUBLIC_DVD_PREDISPATCHLOAD2_202001010000.zip",
        )
        spec = get_table("PREDISPATCHLOAD")
        assert len(match_table_zips(entries, spec, 2020, 1)) == 2

    def test_prefix_names_do_not_cross_match(self) -> None:
        """BIDDAYOFFER must not swallow BIDDAYOFFER_D files."""
        entries = _entries(
            "PUBLIC_DVD_BIDDAYOFFER_202001010000.zip",
            "PUBLIC_DVD_BIDDAYOFFER_D_202001010000.zip",
        )
        raw = get_table("BIDDAYOFFER")
        daily = get_table("BIDDAYOFFER_D")
        assert [e.name for e in match_table_zips(entries, raw, 2020, 1)] == [
            "PUBLIC_DVD_BIDDAYOFFER_202001010000.zip",
        ]
        assert [e.name for e in match_table_zips(entries, daily, 2020, 1)] == [
            "PUBLIC_DVD_BIDDAYOFFER_D_202001010000.zip",
        ]


class TestArchiveEra:
    """2024-08+ snapshots use PUBLIC_ARCHIVE#<TABLE>#FILEnn#<stamp>.zip."""

    def test_matches_every_filenn_part(self) -> None:
        """Multi-part tables return all FILEnn parts, not just FILE01."""
        entries = _entries(
            "PUBLIC_ARCHIVE#BIDPEROFFER_D#FILE01#202605010000.zip",
            "PUBLIC_ARCHIVE#BIDPEROFFER_D#FILE02#202605010000.zip",
            "PUBLIC_ARCHIVE#BIDPEROFFER_D#FILE56#202605010000.zip",
            "PUBLIC_ARCHIVE#BIDDAYOFFER_D#FILE01#202605010000.zip",
        )
        spec = get_table("BIDPEROFFER_D")
        assert len(match_table_zips(entries, spec, 2026, 5)) == 3

    def test_p5min_all_segment_matched(self) -> None:
        """ARCHIVE-era P5MIN files carry an #ALL# segment."""
        entries = _entries(
            "PUBLIC_ARCHIVE#P5MIN_REGIONSOLUTION#ALL#FILE01#202605010000.zip",
        )
        spec = get_table("P5MIN_REGIONSOLUTION")
        assert len(match_table_zips(entries, spec, 2026, 5)) == 1

    def test_decommissioned_table_matches_nothing(self) -> None:
        """TRADINGREGIONSUM has no ARCHIVE-era name at all."""
        entries = _entries(
            "PUBLIC_ARCHIVE#TRADINGPRICE#FILE01#202605010000.zip",
        )
        spec = get_table("TRADINGREGIONSUM")
        assert match_table_zips(entries, spec, 2026, 5) == []


class TestMonthsBetween:
    """Month enumeration covering a date range."""

    def test_spans_year_boundary(self) -> None:
        """Nov to Feb yields four months across the year end."""
        months = months_between(
            datetime.datetime(2023, 11, 15),
            datetime.datetime(2024, 2, 1),
        )
        assert months == [(2023, 11), (2023, 12), (2024, 1), (2024, 2)]
