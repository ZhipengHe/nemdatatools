"""Quickstart examples for NEMDataTools v0.2.

Each example is a standalone function; run the module to execute them all.
Downloads are cached under ~/.nemdatatools, so re-runs are fast.
"""

import nemdatatools as ndt


def example_fetch_prices() -> None:
    """Fetch 5-minute dispatch prices, stitching tiers automatically."""
    prices = ndt.fetch(
        "DISPATCHPRICE",
        "2026/05/01",
        "2026/05/07",
        regions=["QLD1"],
    )
    print(f"rows={len(prices)}  span={prices.index.min()} -> {prices.index.max()}")
    print(prices[["REGIONID", "RRP"]].head())


def example_resample_interval_ending() -> None:
    """Aggregate 5-minute prices to half-hourly, interval-ending aligned."""
    prices = ndt.fetch("DISPATCHPRICE", "2026/05/01", "2026/05/02")
    half_hourly = ndt.resample(prices, "30min", by="REGIONID")
    print(half_hourly.head())


def example_price_and_demand() -> None:
    """Fetch the aggregated price+demand CSVs for one region."""
    data = ndt.fetch_price_and_demand("2026/04/01", "2026/04/30", ["NSW1"])
    print(f"rows={len(data)}  mean RRP={data['RRP'].mean():.2f}")


def example_discovery() -> None:
    """List curated tables and inspect one table's availability."""
    print(ndt.tables())
    info = ndt.availability("BIDPEROFFER_D")
    for gap in info["gaps"]:
        print(f"gap {gap['from']} -> {gap['to']}: {gap['reason']}")


def example_generic_mmsdm() -> None:
    """Fetch a non-curated MMSDM table via the escape hatch."""
    frame = ndt.fetch_mmsdm_table("GENCONDATA", "2026/05/01", "2026/05/31")
    print(f"rows={len(frame)}")


if __name__ == "__main__":
    example_fetch_prices()
    example_resample_interval_ending()
    example_price_and_demand()
    example_discovery()
    example_generic_mmsdm()
