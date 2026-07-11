# Data sources and tier stitching

AEMO publishes the same market data at three ages. NEMDataTools models
that system directly instead of hard-coding one source per table.

## The three publication tiers

| Tier | Location | Granularity | Retention |
|------|----------|-------------|-----------|
| Reports CURRENT | `nemweb.com.au/Reports/Current/` | one file per event | rolling days (varies per package) |
| Reports ARCHIVE | `nemweb.com.au/Reports/Archive/` | daily bundles | ~13 months |
| MMSDM Data Archive | `nemweb.com.au/Data_Archive/` | monthly snapshots | 2009 → ~6 weeks ago |

A fresh dispatch interval appears first as a small file under Reports
CURRENT. Within days it is rolled into a daily zip bundle under Reports
ARCHIVE, and roughly six weeks after month end it lands in that month's
MMSDM snapshot, where it stays permanently.

## How a fetch is routed

`fetch()` splits the requested range by age and serves each part from the
oldest tier that already covers it:

1. **MMSDM monthly snapshots** for everything up to the newest published
   month — the archive is queried live to find where it currently ends.
2. **Reports ARCHIVE daily bundles** for days after the newest MMSDM
   month.
3. **Reports CURRENT files** only for days newer than the newest ARCHIVE
   bundle.

Where tiers overlap, rows are de-duplicated on the table's identity
columns (time column plus key columns) rather than trusting tier
boundaries to be exact.

## Discovery, not construction

Remote files are found by reading directory listings and pattern-matching
— never by constructing filenames. This makes AEMO's format changes
invisible to callers:

- The August 2024 switch from `PUBLIC_DVD_*` to `PUBLIC_ARCHIVE#*`
  filenames inside MMSDM snapshots is handled by matching either era.
- Large tables split into `FILEnn` multi-part archives are detected and
  fetched in full.

## Failing loudly

The package's design promise is that missing data fails with a domain
explanation — never a bare 404 and never a silently partial or empty
DataFrame:

- {class}`~nemdatatools.AvailabilityGapError` — the range crosses a known
  hole in the table's history (for example the bid tables removed at the
  2021 five-minute-settlement transition). The message names the gap
  bounds and the substitute table; re-request around the gap for the
  covered part.
- {class}`~nemdatatools.CoverageError` — part of the range cannot be
  served by any tier, for example a range reaching past the newest MMSDM
  month for a table that has no Reports package.

Both derive from {class}`~nemdatatools.NemDataError`.

## The one exception: price-and-demand CSVs

{func}`~nemdatatools.fetch_price_and_demand` is the only function that
does not read nemweb. It fetches AEMO's aggregated monthly per-region
price-and-demand CSVs from the visualisation service on `aemo.com.au` —
plain CSV, no C/I/D framing. They provide a convenient long history of
5-minute regional price and demand without MMSDM's bulk.
