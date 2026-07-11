"""Time handling for AEMO market data.

All AEMO market timestamps are in NEM time: Australian Eastern Standard
Time, a fixed UTC+10 offset with no daylight saving (the Australia/Brisbane
timezone). The public API of this package accepts and returns naive
datetimes that are understood to be NEM time; timezone-aware datetimes are
rejected rather than silently converted.
"""

from __future__ import annotations

import datetime

# Fixed UTC+10, no daylight saving. Equivalent to Australia/Brisbane.
NEM_TZ = datetime.timezone(datetime.timedelta(hours=10), name="NEM")

# Timestamp format used inside AEMO CSV payloads, e.g. "2026/07/10 22:00:00".
AEMO_DATETIME_FORMAT = "%Y/%m/%d %H:%M:%S"


def parse_date(value: str | datetime.datetime) -> datetime.datetime:
    """Parse user-supplied date input into a naive NEM-time datetime.

    Args:
        value: A naive datetime, or a string in ``YYYY/MM/DD`` or
            ``YYYY/MM/DD HH:MM:SS`` form.

    Returns:
        A naive :class:`datetime.datetime` interpreted as NEM time.

    Raises:
        TypeError: If ``value`` is a timezone-aware datetime.
        ValueError: If a string does not match the accepted formats.

    """
    if isinstance(value, datetime.datetime):
        if value.tzinfo is not None:
            raise TypeError(
                "Datetimes must be naive and are interpreted as NEM time "
                "(fixed UTC+10); convert with .astimezone(NEM_TZ)"
                ".replace(tzinfo=None) first.",
            )
        return value

    for fmt in (AEMO_DATETIME_FORMAT, "%Y/%m/%d"):
        try:
            return datetime.datetime.strptime(value, fmt)
        except ValueError:
            continue
    raise ValueError(
        f"Invalid date {value!r}: expected YYYY/MM/DD or YYYY/MM/DD HH:MM:SS",
    )
