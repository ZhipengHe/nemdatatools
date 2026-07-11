"""Tests for the C/I/D parser against a real TradingIS payload."""

import io
from pathlib import Path

import pandas as pd

from nemdatatools.cid import TableKey, parse_cid

FIXTURES = Path(__file__).parent / "fixtures"


class TestRealTradingIsFile:
    """The captured file carries two tables: INTERCONNECTORRES and PRICE."""

    def test_finds_both_tables(self) -> None:
        """Both C/I/D table segments are discovered."""
        tables = parse_cid(FIXTURES / "tradingis_sample.csv")
        keys = {(t.key.component, t.key.table) for t in tables}
        assert keys == {("TRADING", "INTERCONNECTORRES"), ("TRADING", "PRICE")}

    def test_price_table_has_five_regions(self) -> None:
        """PRICE rows cover all five NEM regions."""
        tables = {t.key.table: t for t in parse_cid(FIXTURES / "tradingis_sample.csv")}
        price = tables["PRICE"].frame
        assert set(price["REGIONID"]) == {"NSW1", "QLD1", "SA1", "TAS1", "VIC1"}

    def test_numeric_columns_converted(self) -> None:
        """Numeric columns become floats; timestamps stay strings."""
        tables = {t.key.table: t for t in parse_cid(FIXTURES / "tradingis_sample.csv")}
        price = tables["PRICE"].frame
        assert price["RRP"].dtype.kind == "f"
        # Timestamps stay string-like for the caller to parse (object on
        # pandas 2, str dtype on pandas 3 — never numeric).
        assert pd.api.types.is_string_dtype(price["SETTLEMENTDATE"])
        assert not pd.api.types.is_numeric_dtype(price["SETTLEMENTDATE"])

    def test_version_captured(self) -> None:
        """Schema version from the I row is preserved."""
        tables = parse_cid(FIXTURES / "tradingis_sample.csv")
        assert all(t.key.version >= 1 for t in tables)


class TestSyntheticEdgeCases:
    """Constructed payloads for structure the fixture doesn't exercise."""

    def test_interleaved_segments_regroup(self) -> None:
        """D rows are routed by their own key, not file order."""
        text = (
            "C,NEMP.WORLD,X,AEMO,PUBLIC,2026/07/10,21:55:06,1,X,1\n"
            "I,A,ONE,1,COL\n"
            "D,A,ONE,1,1\n"
            "I,B,TWO,1,COL\n"
            "D,B,TWO,1,2\n"
            "D,A,ONE,1,3\n"
        )
        tables = {t.key: t for t in parse_cid(io.StringIO(text))}
        assert list(tables[TableKey("A", "ONE", 1)].frame["COL"]) == [1, 3]
        assert list(tables[TableKey("B", "TWO", 1)].frame["COL"]) == [2]

    def test_same_table_two_versions_kept_separate(self) -> None:
        """Version bump starts a separate segment."""
        text = "I,A,ONE,1,COL\nD,A,ONE,1,1\nI,A,ONE,2,COL,EXTRA\nD,A,ONE,2,2,x\n"
        tables = {t.key: t for t in parse_cid(io.StringIO(text))}
        assert len(tables) == 2
        assert "EXTRA" in tables[TableKey("A", "ONE", 2)].frame.columns

    def test_malformed_rows_skipped_not_fatal(self) -> None:
        """Truncated rows and bad version fields skip, parsing continues."""
        text = (
            "I,A,ONE,notanumber,COL\n"  # bad version: header dropped
            "D,A,ONE,1,99\n"  # orphaned D row: no active header
            "I,A,ONE,1,COL\n"
            "D,A\n"  # truncated D row
            "D,A,ONE,1,1\n"
            "D,A,ONE,1,2,extra\n"  # too many fields for the header
        )
        tables = parse_cid(io.StringIO(text))
        assert len(tables) == 1
        assert list(tables[0].frame["COL"]) == [1]

    def test_empty_table_yields_empty_frame(self) -> None:
        """An I row with no D rows gives an empty frame."""
        tables = parse_cid(io.StringIO("I,A,ONE,1,COL\n"))
        assert len(tables) == 1
        assert tables[0].frame.empty
