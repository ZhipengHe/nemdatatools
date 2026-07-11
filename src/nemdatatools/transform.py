"""Time-series transforms with AEMO interval-ending semantics.

AEMO timestamps mark the END of the interval they describe: the row
stamped 00:05 covers 00:00-00:05. Aggregating such series with pandas
defaults (``closed='left', label='left'``) shifts every result by one
interval and mixes trading periods; the helpers here fix ``closed`` and
``label`` to ``'right'`` so, for example, the 00:30 half-hour aggregates
the six 5-minute rows stamped 00:05 through 00:30 — matching how AEMO
itself derives 30-minute trading prices from 5-minute dispatch prices.
"""

from __future__ import annotations

import pandas as pd

# The NEM trading day runs 04:00 to 04:00, not midnight to midnight.
TRADING_DAY_OFFSET = "4h"

AggSpec = str | dict[str, str]


def resample(
    frame: pd.DataFrame,
    rule: str,
    agg: AggSpec = "mean",
    by: str | list[str] | None = None,
    trading_day: bool = False,
) -> pd.DataFrame:
    """Resample interval-ending AEMO data to a coarser interval.

    Args:
        frame: Data indexed by a DatetimeIndex of interval-ending
            timestamps (as returned by :func:`nemdatatools.fetch`).
        rule: Target interval, e.g. ``"30min"``, ``"1h"``, ``"1D"``.
        agg: Aggregation for numeric columns — a single function name, or
            a column-to-function mapping (non-mapped columns are dropped).
        by: Identity column(s) to group by first, e.g. ``"REGIONID"`` or
            ``"DUID"``. Required when the frame carries more than one
            entity, otherwise their values would be averaged together.
        trading_day: When aggregating to days or coarser, align buckets to
            the 04:00-04:00 NEM trading day instead of midnight.

    Returns:
        Aggregated rows labelled with interval-ending timestamps, grouped
        columns preserved as regular columns.

    Raises:
        ValueError: If the index is not a DatetimeIndex, or the frame has
            several entities and ``by`` was not given.

    """
    if not isinstance(frame.index, pd.DatetimeIndex):
        raise ValueError("frame must be indexed by interval-ending timestamps")

    group_cols = [by] if isinstance(by, str) else list(by or [])
    if not group_cols:
        _guard_single_entity(frame)

    if isinstance(agg, dict):
        targets = agg
    else:
        numeric = frame.select_dtypes("number").columns
        targets = {c: agg for c in numeric if c not in group_cols}

    grouper = pd.Grouper(
        level=frame.index.name or 0,
        freq=rule,
        closed="right",
        label="right",
        offset=TRADING_DAY_OFFSET if trading_day else None,
    )
    grouped = frame.groupby([*group_cols, grouper])
    result = grouped.agg(targets).reset_index(group_cols)
    return result


def _guard_single_entity(frame: pd.DataFrame) -> None:
    """Reject ungrouped frames that clearly hold several entities."""
    for column in ("REGIONID", "REGION", "DUID", "INTERCONNECTORID"):
        if column in frame.columns and frame[column].nunique() > 1:
            raise ValueError(
                f"frame holds {frame[column].nunique()} distinct "
                f"{column} values; pass by={column!r} so entities are "
                "not averaged together",
            )
