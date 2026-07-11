# Time conventions and resampling

## NEM time

All AEMO market timestamps are in NEM time: Australian Eastern Standard
Time, a fixed UTC+10 offset with **no daylight saving** (equivalent to
the Australia/Brisbane timezone — never Sydney/Melbourne time).

The public API accepts and returns **naive** datetimes that are
understood to be NEM time. Timezone-aware datetimes are rejected with a
`TypeError` rather than silently converted; convert first if you work in
another zone:

```python
import datetime

aware.astimezone(datetime.timezone(datetime.timedelta(hours=10))).replace(tzinfo=None)
```

Date arguments to the fetch functions may be naive `datetime` objects,
`date` objects (midnight assumed), or strings in `YYYY/MM/DD` or
`YYYY/MM/DD HH:MM:SS` form. Ranges are inclusive at both ends.

## Interval-ending timestamps

AEMO timestamps mark the **end** of the interval they describe: the row
stamped 00:05 covers 00:00–00:05. This matters when aggregating —
pandas' `resample` defaults (`closed='left', label='left'`) shift every
result by one interval and mix trading periods.

{func}`~nemdatatools.resample` fixes both to `'right'`, so the 00:30
half-hour bucket aggregates the six 5-minute rows stamped 00:05 through
00:30 — matching how AEMO itself derives 30-minute trading prices from
5-minute dispatch prices:

```python
import nemdatatools as ndt

prices = ndt.fetch("DISPATCHPRICE", "2026/06/01", "2026/06/07", regions=["QLD1"])
half_hourly = ndt.resample(prices[["RRP"]], "30min")
```

## Grouping multi-entity frames

A frame holding several regions, units, or interconnectors must be
grouped explicitly with `by=`, otherwise distinct entities would be
averaged together. Ungrouped frames that clearly hold several entities
are rejected with a `ValueError`:

```python
all_regions = ndt.fetch("DISPATCHPRICE", "2026/06/01", "2026/06/07")
daily = ndt.resample(all_regions, "1D", by="REGIONID")
```

## Aggregation functions

`agg` is a single function name applied to all numeric columns
(default `"mean"`), or a column-to-function mapping — non-mapped columns
are dropped:

```python
daily = ndt.resample(scada, "1D", agg={"SCADAVALUE": "sum"}, by="DUID")
```

## The NEM trading day

The NEM trading day runs 04:00 to 04:00, not midnight to midnight. When
aggregating to days or coarser, pass `trading_day=True` to align buckets
to the trading day:

```python
daily = ndt.resample(all_regions, "1D", by="REGIONID", trading_day=True)
```

Each resulting bucket is labelled with its interval-ending timestamp
(the 04:00 that closes the trading day).
