"""Tests for nemweb directory-listing parsing against real captured HTML."""

from pathlib import Path

from nemdatatools.listing import parse_listing_html

FIXTURES = Path(__file__).parent / "fixtures"


def _read(name: str) -> str:
    """Read a fixture file as text."""
    return (FIXTURES / name).read_text(encoding="utf-8", errors="replace")


class TestReportsCurrentListing:
    """Reports pages: no trailing slash on dirs, <dir> marker in text."""

    def test_parses_zip_entries(self) -> None:
        """Report zips parse with absolute HTTPS URLs."""
        entries = parse_listing_html(
            _read("listing_reports_current_unquoted.html"),
            base_url="https://nemweb.com.au/Reports/Current/TradingIS_Reports/",
        )
        zips = [e for e in entries if e.name.endswith(".zip")]
        assert zips, "expected at least one zip entry in fixture"
        assert all(e.name.startswith("PUBLIC_TRADINGIS_") for e in zips)
        assert all(not e.is_dir for e in zips)
        assert all(e.url.startswith("https://") for e in zips)

    def test_marks_dir_without_trailing_slash(self) -> None:
        """The <dir> text marker flags directories."""
        entries = parse_listing_html(
            _read("listing_reports_current_unquoted.html"),
            base_url="https://nemweb.com.au/Reports/Current/TradingIS_Reports/",
        )
        dirs = {e.name for e in entries if e.is_dir}
        assert "DUPLICATE" in dirs

    def test_excludes_parent_link(self) -> None:
        """Parent-directory links are dropped."""
        entries = parse_listing_html(
            _read("listing_reports_current_unquoted.html"),
            base_url="https://nemweb.com.au/Reports/Current/TradingIS_Reports/",
        )
        assert all("/TradingIS_Reports" in e.url for e in entries)


class TestDataArchiveListing:
    """Data_Archive pages: percent-encoded '#' in names, trailing-slash dirs."""

    def test_decodes_percent_encoded_names(self) -> None:
        """%23 decodes to # in entry names."""
        entries = parse_listing_html(
            _read("listing_data_archive_quoted.html"),
            base_url=(
                "https://nemweb.com.au/Data_Archive/Wholesale_Electricity/MMSDM/"
                "2026/MMSDM_2026_05/MMSDM_Historical_Data_SQLLoader/DATA/"
            ),
        )
        names = [e.name for e in entries if e.name.endswith(".zip")]
        assert names, "expected zip entries in fixture"
        assert any(n.startswith("PUBLIC_ARCHIVE#") for n in names)
        assert all("%23" not in n for n in names)

    def test_urls_keep_percent_encoding(self) -> None:
        """Entry URLs keep %23 so requests stay valid."""
        entries = parse_listing_html(
            _read("listing_data_archive_quoted.html"),
            base_url=(
                "https://nemweb.com.au/Data_Archive/Wholesale_Electricity/MMSDM/"
                "2026/MMSDM_2026_05/MMSDM_Historical_Data_SQLLoader/DATA/"
            ),
        )
        zip_urls = [e.url for e in entries if e.name.endswith(".zip")]
        assert all("%23" in u for u in zip_urls)
