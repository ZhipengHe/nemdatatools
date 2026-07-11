# Caching

Every download and every parsed table is cached, so repeat analysis over
the same range costs no network traffic and no re-parsing.

## Layout

Downloads land under `~/.nemdatatools/` by default, in two layers:

- `raw/` mirrors the nemweb path of every downloaded file — full
  provenance, and a parser fix never forces a re-download.
- `parquet/` stores each parsed `(payload, table)` pair, so repeat reads
  skip zip extraction and CSV parsing entirely.

Nothing is evicted automatically; delete directories under the cache
root to reclaim space. Deleting only `parquet/` keeps the raw files, so
the tables are rebuilt locally without touching the network.

## Custom cache location

Pass a {class}`~nemdatatools.Cache` rooted elsewhere to any fetch
function:

```python
import nemdatatools as ndt

cache = ndt.Cache("/data/nem-cache")
prices = ndt.fetch("DISPATCHPRICE", "2026/06/01", "2026/06/07", cache=cache)
```

A shared `requests.Session` can also be supplied for custom HTTP
behaviour (proxies, headers).

## Politeness

nemweb is a shared public service. Downloads are spaced at least half a
second apart, and an intermittent CDN 403 is retried once after a short
pause before being raised.
