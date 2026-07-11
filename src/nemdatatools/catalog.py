"""Catalog of curated AEMO tables and where they live across publication tiers.

AEMO publishes the same market data at three ages — Reports CURRENT
(rolling days), Reports ARCHIVE (~13 months of daily bundles), and the
MMSDM monthly archive (2009 onward) — and the names under which a table is
published change over time. The catalog records, per table, everything the
router needs to locate it in each tier and era, plus known availability
gaps so requests that cross a gap fail loudly instead of returning
silently partial data.

Era facts encoded here were verified against live nemweb listings
(2020-2026 survey, 2026-07-11): the MMSDM filename format changed at
2024-08 from ``PUBLIC_DVD_<TABLE>_...`` to
``PUBLIC_ARCHIVE#<TABLE>#FILEnn#...``; DVD-era P5MIN tables carry an
``_ALL`` suffix; DVD-era large tables split via numbered names while
ARCHIVE-era tables split via FILEnn parts; the five-minute-settlement
transition removed the ``_D`` bid tables from MMSDM between 2021-03 and
2024-07 and decommissioned the 30-minute TRADING summary tables.
"""

from __future__ import annotations

from dataclasses import dataclass, field

# First MMSDM month published in the PUBLIC_ARCHIVE filename format.
ARCHIVE_ERA_START: tuple[int, int] = (2024, 8)


@dataclass(frozen=True)
class MmsdmLocation:
    """Where a table lives inside an MMSDM monthly snapshot.

    Attributes:
        subdir: Snapshot subdirectory (``DATA``, ``P5MIN_ALL_DATA`` or
            ``PREDISP_ALL_DATA``).
        dvd_names: Filename table tokens to match in the DVD era. Several
            tokens express DVD-era splits (e.g. ``PREDISPATCHLOAD1`` and
            ``PREDISPATCHLOAD2``) or the P5MIN ``_ALL`` suffix. Empty when
            the table does not exist in the DVD era.
        archive_name: Filename table token in the ARCHIVE era (every FILEnn
            part is fetched). None when the table left MMSDM before 2024-08.

    """

    subdir: str
    dvd_names: tuple[str, ...]
    archive_name: str | None


@dataclass(frozen=True)
class ReportLocation:
    """Where a table lives under Reports CURRENT / ARCHIVE.

    Attributes:
        package: Path below ``Reports/Current/`` (may be nested, e.g.
            ``ROOFTOP_PV/ACTUAL``).
        file_prefix: Filename prefix identifying payloads in the package.
        archive_package: Path below ``Reports/Archive/`` holding daily
            zip-of-zips bundles, or None when the package has no archive.

    """

    package: str
    file_prefix: str
    archive_package: str | None


@dataclass(frozen=True)
class Gap:
    """A known hole in a table's MMSDM availability.

    Attributes:
        start: First missing month, inclusive, as ``(year, month)``.
        end: Last missing month inclusive, or None for "gone for good".
        reason: Domain explanation surfaced to the user, ideally naming
            the substitute table to use across the gap.

    """

    start: tuple[int, int]
    end: tuple[int, int] | None
    reason: str


@dataclass(frozen=True)
class TableSpec:
    """Everything the router needs to fetch one table across tiers.

    Attributes:
        name: Public table name exposed by this package's API.
        cid_key: ``(component, table)`` identifying the table inside
            C/I/D payloads, e.g. ``("DISPATCH", "PRICE")``.
        time_column: Column used for date-range filtering. AEMO
            timestamps are interval-ending NEM time.
        key_columns: Identity columns (with the time column) used to
            de-duplicate rows where tiers overlap.
        region_column: Column carrying a NEM region id, or None.
        mmsdm: MMSDM location, or None if the table never appears there.
        report: Reports CURRENT/ARCHIVE location, or None.
        first_month: Earliest MMSDM month known to carry the table
            (bounded by the 2020 survey; may exist earlier).
        gaps: Known MMSDM availability gaps.

    """

    name: str
    cid_key: tuple[str, str]
    time_column: str
    key_columns: tuple[str, ...]
    region_column: str | None = None
    mmsdm: MmsdmLocation | None = None
    report: ReportLocation | None = None
    first_month: tuple[int, int] = (2009, 7)
    gaps: tuple[Gap, ...] = field(default=())


_FIVE_MS_BID_GAP = Gap(
    start=(2021, 3),
    end=(2024, 7),
    reason=(
        "The daily-snapshot bid tables left MMSDM at the five-minute "
        "settlement transition and only returned with the 2024-08 archive "
        "format; use BIDDAYOFFER/BIDPEROFFER (raw offers) across the gap."
    ),
)

_DISPATCH_IS = ReportLocation(
    package="DispatchIS_Reports",
    file_prefix="PUBLIC_DISPATCHIS_",
    archive_package="DispatchIS_Reports",
)
_TRADING_IS = ReportLocation(
    package="TradingIS_Reports",
    file_prefix="PUBLIC_TRADINGIS_",
    archive_package="TradingIS_Reports",
)
_PREDISPATCH_IS = ReportLocation(
    package="PredispatchIS_Reports",
    file_prefix="PUBLIC_PREDISPATCHIS_",
    archive_package="PredispatchIS_Reports",
)
_P5_REPORTS = ReportLocation(
    package="P5_Reports",
    file_prefix="PUBLIC_P5MIN_",
    archive_package="P5_Reports",
)

TABLES: dict[str, TableSpec] = {
    spec.name: spec
    for spec in (
        # --- Prices and demand -------------------------------------------
        TableSpec(
            name="DISPATCHPRICE",
            cid_key=("DISPATCH", "PRICE"),
            time_column="SETTLEMENTDATE",
            key_columns=("REGIONID", "RUNNO", "INTERVENTION"),
            region_column="REGIONID",
            mmsdm=MmsdmLocation("DATA", ("DISPATCHPRICE",), "DISPATCHPRICE"),
            report=_DISPATCH_IS,
        ),
        TableSpec(
            name="DISPATCHREGIONSUM",
            cid_key=("DISPATCH", "REGIONSUM"),
            time_column="SETTLEMENTDATE",
            key_columns=("REGIONID", "RUNNO", "INTERVENTION"),
            region_column="REGIONID",
            mmsdm=MmsdmLocation("DATA", ("DISPATCHREGIONSUM",), "DISPATCHREGIONSUM"),
            report=_DISPATCH_IS,
        ),
        TableSpec(
            name="TRADINGPRICE",
            cid_key=("TRADING", "PRICE"),
            time_column="SETTLEMENTDATE",
            key_columns=("REGIONID", "RUNNO"),
            region_column="REGIONID",
            mmsdm=MmsdmLocation("DATA", ("TRADINGPRICE",), "TRADINGPRICE"),
            report=_TRADING_IS,
        ),
        TableSpec(
            name="TRADINGINTERCONNECT",
            cid_key=("TRADING", "INTERCONNECTORRES"),
            time_column="SETTLEMENTDATE",
            key_columns=("INTERCONNECTORID", "RUNNO"),
            mmsdm=MmsdmLocation(
                "DATA",
                ("TRADINGINTERCONNECT",),
                "TRADINGINTERCONNECT",
            ),
            report=_TRADING_IS,
        ),
        TableSpec(
            name="DISPATCHINTERCONNECTORRES",
            cid_key=("DISPATCH", "INTERCONNECTORRES"),
            time_column="SETTLEMENTDATE",
            key_columns=("INTERCONNECTORID", "RUNNO", "INTERVENTION"),
            mmsdm=MmsdmLocation(
                "DATA",
                ("DISPATCHINTERCONNECTORRES",),
                "DISPATCHINTERCONNECTORRES",
            ),
            report=_DISPATCH_IS,
        ),
        # --- Generation and SCADA ----------------------------------------
        TableSpec(
            name="DISPATCH_UNIT_SCADA",
            cid_key=("DISPATCH", "UNIT_SCADA"),
            time_column="SETTLEMENTDATE",
            key_columns=("DUID",),
            mmsdm=MmsdmLocation(
                "DATA",
                ("DISPATCH_UNIT_SCADA",),
                "DISPATCH_UNIT_SCADA",
            ),
            report=ReportLocation(
                package="Dispatch_SCADA",
                file_prefix="PUBLIC_DISPATCHSCADA_",
                archive_package="Dispatch_SCADA",
            ),
        ),
        TableSpec(
            name="DISPATCHLOAD",
            cid_key=("DISPATCH", "UNIT_SOLUTION"),
            time_column="SETTLEMENTDATE",
            key_columns=("DUID", "RUNNO", "INTERVENTION"),
            mmsdm=MmsdmLocation("DATA", ("DISPATCHLOAD",), "DISPATCHLOAD"),
            report=ReportLocation(
                package="Next_Day_Dispatch",
                file_prefix="PUBLIC_NEXT_DAY_DISPATCH_",
                archive_package="Next_Day_Dispatch",
            ),
        ),
        TableSpec(
            name="ROOFTOP_PV_ACTUAL",
            cid_key=("ROOFTOP", "ACTUAL"),
            time_column="INTERVAL_DATETIME",
            key_columns=("REGIONID", "TYPE"),
            region_column="REGIONID",
            mmsdm=MmsdmLocation(
                "DATA",
                ("ROOFTOP_PV_ACTUAL",),
                "ROOFTOP_PV_ACTUAL",
            ),
            report=ReportLocation(
                package="ROOFTOP_PV/ACTUAL",
                file_prefix="PUBLIC_ROOFTOP_PV_ACTUAL_",
                archive_package="ROOFTOP_PV/ACTUAL",
            ),
        ),
        # --- Forecasts ----------------------------------------------------
        TableSpec(
            name="P5MIN_REGIONSOLUTION",
            cid_key=("P5MIN", "REGIONSOLUTION"),
            time_column="INTERVAL_DATETIME",
            key_columns=("REGIONID", "RUN_DATETIME", "INTERVENTION"),
            region_column="REGIONID",
            mmsdm=MmsdmLocation(
                "P5MIN_ALL_DATA",
                ("P5MIN_REGIONSOLUTION_ALL",),
                "P5MIN_REGIONSOLUTION",
            ),
            report=_P5_REPORTS,
        ),
        TableSpec(
            name="P5MIN_INTERCONNECTORSOLN",
            cid_key=("P5MIN", "INTERCONNECTORSOLN"),
            time_column="INTERVAL_DATETIME",
            key_columns=("INTERCONNECTORID", "RUN_DATETIME", "INTERVENTION"),
            mmsdm=MmsdmLocation(
                "P5MIN_ALL_DATA",
                ("P5MIN_INTERCONNECTORSOLN_ALL",),
                "P5MIN_INTERCONNECTORSOLN",
            ),
            report=_P5_REPORTS,
        ),
        TableSpec(
            name="PREDISPATCHPRICE",
            cid_key=("PREDISPATCH", "REGION_PRICES"),
            time_column="DATETIME",
            key_columns=("REGIONID", "PREDISPATCHSEQNO", "RUNNO"),
            region_column="REGIONID",
            mmsdm=MmsdmLocation(
                "PREDISP_ALL_DATA",
                ("PREDISPATCHPRICE",),
                "PREDISPATCHPRICE",
            ),
            report=_PREDISPATCH_IS,
        ),
        TableSpec(
            name="PREDISPATCHREGIONSUM",
            cid_key=("PREDISPATCH", "REGION_SOLUTION"),
            time_column="DATETIME",
            key_columns=("REGIONID", "PREDISPATCHSEQNO", "RUNNO"),
            region_column="REGIONID",
            mmsdm=MmsdmLocation(
                "PREDISP_ALL_DATA",
                ("PREDISPATCHREGIONSUM",),
                "PREDISPATCHREGIONSUM",
            ),
            report=_PREDISPATCH_IS,
        ),
        TableSpec(
            name="PREDISPATCHLOAD",
            cid_key=("PREDISPATCH", "UNIT_SOLUTION"),
            time_column="DATETIME",
            key_columns=("DUID", "PREDISPATCHSEQNO", "RUNNO"),
            mmsdm=MmsdmLocation(
                "PREDISP_ALL_DATA",
                ("PREDISPATCHLOAD1", "PREDISPATCHLOAD2"),
                "PREDISPATCHLOAD",
            ),
        ),
        # --- Bids and offers ----------------------------------------------
        TableSpec(
            name="BIDDAYOFFER_D",
            cid_key=("BID", "BIDDAYOFFER_D"),
            time_column="SETTLEMENTDATE",
            key_columns=("DUID", "BIDTYPE"),
            mmsdm=MmsdmLocation("DATA", ("BIDDAYOFFER_D",), "BIDDAYOFFER_D"),
            report=ReportLocation(
                package="Bidmove_Complete",
                file_prefix="PUBLIC_BIDMOVE_COMPLETE_",
                archive_package="Bidmove_Complete",
            ),
            gaps=(_FIVE_MS_BID_GAP,),
        ),
        TableSpec(
            name="BIDPEROFFER_D",
            cid_key=("BID", "BIDPEROFFER_D"),
            time_column="SETTLEMENTDATE",
            key_columns=("DUID", "BIDTYPE", "INTERVAL_DATETIME"),
            mmsdm=MmsdmLocation("DATA", ("BIDPEROFFER_D",), "BIDPEROFFER_D"),
            report=ReportLocation(
                package="Bidmove_Complete",
                file_prefix="PUBLIC_BIDMOVE_COMPLETE_",
                archive_package="Bidmove_Complete",
            ),
            gaps=(_FIVE_MS_BID_GAP,),
        ),
        TableSpec(
            name="BIDDAYOFFER",
            cid_key=("BID", "BIDDAYOFFER"),
            time_column="SETTLEMENTDATE",
            key_columns=("DUID", "BIDTYPE", "OFFERDATE"),
            mmsdm=MmsdmLocation("DATA", ("BIDDAYOFFER",), "BIDDAYOFFER"),
        ),
        TableSpec(
            name="BIDPEROFFER",
            cid_key=("BID", "BIDPEROFFER"),
            time_column="SETTLEMENTDATE",
            key_columns=("DUID", "BIDTYPE", "OFFERDATE", "PERIODID"),
            mmsdm=MmsdmLocation(
                "DATA",
                ("BIDPEROFFER", "BIDPEROFFER1", "BIDPEROFFER2"),
                None,
            ),
            gaps=(
                Gap(
                    start=(2024, 8),
                    end=None,
                    reason=(
                        "Raw BIDPEROFFER was replaced by BIDOFFERPERIOD in "
                        "the 2024-08 archive format; use BIDPEROFFER_D or "
                        "BIDOFFERPERIOD from 2024-08 onward."
                    ),
                ),
            ),
        ),
        # --- Decommissioned, kept for pre-5MS history ----------------------
        TableSpec(
            name="TRADINGREGIONSUM",
            cid_key=("TRADING", "REGIONSUM"),
            time_column="SETTLEMENTDATE",
            key_columns=("REGIONID", "RUNNO"),
            region_column="REGIONID",
            mmsdm=MmsdmLocation("DATA", ("TRADINGREGIONSUM",), None),
            gaps=(
                Gap(
                    start=(2021, 11),
                    end=None,
                    reason=(
                        "The 30-minute trading summaries were decommissioned "
                        "at the five-minute settlement transition; use "
                        "DISPATCHREGIONSUM (5-minute) from 2021-10 onward."
                    ),
                ),
            ),
        ),
    )
}

NEM_REGIONS: tuple[str, ...] = ("NSW1", "QLD1", "SA1", "TAS1", "VIC1")


def get_table(name: str) -> TableSpec:
    """Look up a curated table by name.

    Args:
        name: Public table name, case-insensitive.

    Returns:
        The matching :class:`TableSpec`.

    Raises:
        KeyError: With the list of curated names when unknown.

    """
    try:
        return TABLES[name.upper()]
    except KeyError:
        known = ", ".join(sorted(TABLES))
        raise KeyError(
            f"Unknown table {name!r}. Curated tables: {known}. Other MMSDM "
            "tables can be fetched with fetch_mmsdm_table().",
        ) from None
