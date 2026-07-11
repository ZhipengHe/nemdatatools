# NEMDataTools

An MIT-licensed Python package for accessing and preprocessing Australian
Energy Market Operator (AEMO) data for the National Electricity Market (NEM).

## How it works

AEMO publishes the same market data at three ages, and NEMDataTools models
that system directly instead of hard-coding one source per table:

| Tier | Location | Granularity | Retention |
|------|----------|-------------|-----------|
| Reports CURRENT | `nemweb.com.au/Reports/Current/` | one file per event | rolling days (varies per package) |
| Reports ARCHIVE | `nemweb.com.au/Reports/Archive/` | daily bundles | ~13 months |
| MMSDM Data Archive | `nemweb.com.au/Data_Archive/` | monthly snapshots | 2009 → ~6 weeks ago |

One `fetch()` call stitches whichever tiers a date range needs. Remote files
are discovered by reading directory listings and pattern-matching — never by
constructing filenames — so AEMO's filename-format changes (such as the
August 2024 `PUBLIC_DVD_*` → `PUBLIC_ARCHIVE#*` switch) and multi-part
archives are handled transparently, including all `FILEnn` parts of large
tables. Known holes in a table's history (for example the bid tables removed
at the 2021 five-minute-settlement transition) raise a clear error naming
the substitute table instead of returning silently partial data.

## Installation

```bash
pip install nemdatatools
```

Requires Python 3.10+. Dependencies: pandas, pyarrow, requests,
beautifulsoup4.

## Quick start

```python
import nemdatatools as ndt

# One call, any range — tiers are stitched automatically.
prices = ndt.fetch(
    "DISPATCHPRICE",
    "2020/01/01",
    "2026/07/01",
    regions=["QLD1"],
)

# Interval-ending-aware resampling: the 00:30 bucket aggregates the six
# 5-minute rows stamped 00:05..00:30, matching AEMO's own convention.
half_hourly = ndt.resample(prices[["RRP"]], "30min")

# Frames holding several regions/units must be grouped explicitly:
all_regions = ndt.fetch("DISPATCHPRICE", "2026/06/01", "2026/06/07")
daily = ndt.resample(all_regions, "1D", by="REGIONID", trading_day=True)

# Aggregated price+demand CSVs (aemo.com.au visualisation service):
pd_data = ndt.fetch_price_and_demand("2024/01/01", "2024/12/31", ["NSW1"])

# Discovery:
ndt.tables()                        # curated table names
ndt.availability("BIDPEROFFER_D")   # tier locations + known gaps

# Escape hatch: any of the ~236 MMSDM tables, era-aware, all parts:
gencon = ndt.fetch_mmsdm_table("GENCONDATA", "2026/05/01", "2026/05/31")
```

All datetimes are naive **NEM time** (fixed UTC+10, no daylight saving);
timezone-aware datetimes are rejected rather than silently converted. AEMO
timestamps mark the **end** of the interval they describe.

## Curated tables

`fetch()` accepts curated tables spanning four families; each is wired to
its locations in every tier and era:

- **Prices & demand** — `DISPATCHPRICE`, `TRADINGPRICE`,
  `DISPATCHREGIONSUM`, `TRADINGINTERCONNECT`, `DISPATCHINTERCONNECTORRES`
- **Generation & SCADA** — `DISPATCH_UNIT_SCADA`, `DISPATCHLOAD`,
  `ROOFTOP_PV_ACTUAL`
- **Forecasts** — `P5MIN_REGIONSOLUTION`, `P5MIN_INTERCONNECTORSOLN`,
  `PREDISPATCHPRICE`, `PREDISPATCHREGIONSUM`, `PREDISPATCHLOAD`
- **Bids & offers** — `BIDDAYOFFER_D`, `BIDPEROFFER_D`, `BIDDAYOFFER`,
  `BIDPEROFFER` (plus pre-2021 `TRADINGREGIONSUM` for history)

Every other MMSDM table is reachable through `fetch_mmsdm_table()`.

## Caching

Downloads land under `~/.nemdatatools/` (override with
`ndt.Cache("path")` passed as `cache=`):

- `raw/` mirrors nemweb paths — full provenance, a parser fix never forces
  a re-download;
- `parquet/` stores parsed per-table frames, so repeat reads skip zip
  extraction and CSV parsing entirely.

## Data attribution

Data is © AEMO and provided under AEMO's terms; this package downloads
publicly available files and does not redistribute data. See NOTICE and
LICENSE for details.

## License

MIT — see [LICENSE](LICENSE).
