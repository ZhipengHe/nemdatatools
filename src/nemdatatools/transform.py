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
    if trading_day and not _is_daily_or_coarser(rule):
        raise ValueError(
            "trading_day aligns buckets to the 04:00 NEM trading day and "
            f"only applies to daily-or-coarser rules, not {rule!r}",
        )

    group_cols = [by] if isinstance(by, str) else list(by or [])
    if not group_cols:
        _guard_single_entity(frame)

    if isinstance(agg, dict):
        targets = agg
    else:
        numeric = frame.select_dtypes("number").columns
        targets = {c: agg for c in numeric if c not in group_cols}

    # Trading-day alignment is done by shifting the index rather than via
    # Grouper's offset parameter, whose semantics changed in pandas 3.0;
    # the shift is equivalent and behaves the same on pandas 2 and 3.
    shift = pd.Timedelta(TRADING_DAY_OFFSET) if trading_day else None
    work = frame if shift is None else frame.set_axis(frame.index - shift)
    grouper = pd.Grouper(
        level=frame.index.name or 0,
        freq=rule,
        closed="right",
        label="right",
    )
    # dropna=False keeps rows whose entity id is missing as their own
    # group instead of silently discarding them.
    grouped = work.groupby([*group_cols, grouper], dropna=False)
    result = grouped.agg(targets).reset_index(group_cols)
    if shift is not None:
        result.index = result.index + shift
    return _trim_empty_edge_buckets(result, list(targets))


def _trim_empty_edge_buckets(
    result: pd.DataFrame,
    value_columns: list[str],
) -> pd.DataFrame:
    """Drop leading/trailing buckets that contain no observations.

    pandas 3 emits an empty right-closed bin before the first observation
    where pandas 2 trims it; normalising here keeps output identical
    across versions. Interior empty buckets are kept — they are real
    holes in the data, not binning artifacts.
    """
    if result.empty or not value_columns:
        return result
    populated = result[value_columns].notna().any(axis=1)
    labels_with_data = populated.groupby(level=0).any()
    valid = labels_with_data[labels_with_data].index
    if valid.empty or len(valid) == len(labels_with_data):
        return result
    in_span = (result.index >= valid.min()) & (result.index <= valid.max())
    trimmed: pd.DataFrame = result.loc[in_span]
    return trimmed


def _is_daily_or_coarser(rule: str) -> bool:
    """Tell whether a resample rule spans at least one day.

    Calendar-based rules (weekly, monthly, ...) have no fixed length and
    are always daily-or-coarser.
    """
    offset = pd.tseries.frequencies.to_offset(rule)
    try:
        # Only fixed-length (Tick) frequencies have nanos; calendar rules
        # raise ValueError here.
        nanos = int(offset.nanos)
    except ValueError:
        return True
    return nanos >= 24 * 60 * 60 * 1_000_000_000


def _guard_single_entity(frame: pd.DataFrame) -> None:
    """Reject ungrouped frames that clearly hold several entities.

    Missing ids count as their own entity so NaN rows cannot be blended
    into a real series unnoticed.
    """
    for column in ("REGIONID", "REGION", "DUID", "INTERCONNECTORID"):
        if column not in frame.columns:
            continue
        distinct = frame[column].nunique(dropna=False)
        if distinct > 1:
            raise ValueError(
                f"frame holds {distinct} distinct {column} values; pass "
                f"by={column!r} so entities are not averaged together",
            )
