# Tables: curated catalog and escape hatch

## Curated tables

{func}`~nemdatatools.fetch` accepts curated tables spanning four
families; each is wired to its locations in every tier and filename era:

- **Prices & demand** — `DISPATCHPRICE`, `TRADINGPRICE`,
  `DISPATCHREGIONSUM`, `TRADINGINTERCONNECT`,
  `DISPATCHINTERCONNECTORRES`
- **Generation & SCADA** — `DISPATCH_UNIT_SCADA`, `DISPATCHLOAD`,
  `ROOFTOP_PV_ACTUAL`
- **Forecasts** — `P5MIN_REGIONSOLUTION`, `P5MIN_INTERCONNECTORSOLN`,
  `PREDISPATCHPRICE`, `PREDISPATCHREGIONSUM`, `PREDISPATCHLOAD`
- **Bids & offers** — `BIDDAYOFFER_D`, `BIDPEROFFER_D`, `BIDDAYOFFER`,
  `BIDPEROFFER` (plus pre-2021 `TRADINGREGIONSUM` for history)

List them programmatically with {func}`~nemdatatools.tables`:

```python
import nemdatatools as ndt

ndt.tables()
```

## Inspecting availability

{func}`~nemdatatools.availability` describes where a curated table lives
and any known holes in its history:

```python
info = ndt.availability("BIDPEROFFER_D")
for gap in info["gaps"]:
    print(f"gap {gap['from']} -> {gap['to']}: {gap['reason']}")
```

The result names the MMSDM subdirectory and filenames in both eras, the
Reports CURRENT and ARCHIVE packages, and each gap's bounds and reason.
Requesting a range that crosses a gap raises
{class}`~nemdatatools.AvailabilityGapError` naming the substitute table,
instead of returning silently partial data.

## Region filtering

Tables with a region column accept a `regions=` filter using the five
NEM region identifiers (available as
{data}`~nemdatatools.NEM_REGIONS`): `NSW1`, `QLD1`, `SA1`, `TAS1`,
`VIC1`.

```python
prices = ndt.fetch("DISPATCHPRICE", "2026/06/01", "2026/06/07", regions=["QLD1"])
```

## Escape hatch: any MMSDM table

Every other MMSDM table — roughly 236 of them — is reachable through
{func}`~nemdatatools.fetch_mmsdm_table`:

```python
gencon = ndt.fetch_mmsdm_table("GENCONDATA", "2026/05/01", "2026/05/31")
```

Files are still discovered by listing-and-matching in both filename eras
and all `FILEnn` parts are fetched, but no Reports stitching, gap
checking, or de-duplication is applied — the newest data available is
the newest MMSDM monthly snapshot (~6 weeks behind). The time column is
auto-detected for range filtering (`SETTLEMENTDATE`,
`INTERVAL_DATETIME`, or `DATETIME`); when none is recognised, all rows
of the fetched months are returned.

Snapshot subdirectories other than the default `DATA` are selected with
`subdir=` — `P5MIN_ALL_DATA` and `PREDISP_ALL_DATA` hold the complete
5-minute pre-dispatch and pre-dispatch runs.

## Aggregated price and demand

{func}`~nemdatatools.fetch_price_and_demand` fetches AEMO's aggregated
5-minute price and demand CSVs by region — a lightweight alternative to
`DISPATCHPRICE`/`DISPATCHREGIONSUM` when only regional price and demand
are needed:

```python
pd_data = ndt.fetch_price_and_demand("2024/01/01", "2024/12/31", ["NSW1"])
```

It returns `REGION`, `TOTALDEMAND`, `RRP`, and `PERIODTYPE` indexed by
`SETTLEMENTDATE`.
