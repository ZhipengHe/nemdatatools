"""Parser for AEMO's C/I/D CSV payload format.

Every nemweb CSV payload uses the same row-tagged layout:

- ``C`` rows: comment/control metadata (publisher, report id, row counts)
- ``I`` rows: column headers introducing a table segment —
  ``I,<component>,<table>,<version>,<column>,...``
- ``D`` rows: data rows belonging to the most recent ``I`` row

A single file may interleave several tables (for example TradingIS files
carry both ``TRADING,PRICE`` and ``TRADING,INTERCONNECTORRES``), and the
same table may appear at different schema versions. Segments are grouped by
``(component, table, version)``.
"""

from __future__ import annotations

import csv
import io
import zipfile
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import IO

import pandas as pd


@dataclass(frozen=True)
class TableKey:
    """Identity of one table segment inside a C/I/D file."""

    component: str
    table: str
    version: int


@dataclass
class CidTable:
    """One parsed table from a C/I/D file."""

    key: TableKey
    frame: pd.DataFrame


def parse_cid(source: str | Path | IO[str]) -> list[CidTable]:
    """Parse a C/I/D CSV stream into per-table DataFrames.

    Args:
        source: Path to a CSV file, or an open text stream.

    Returns:
        One :class:`CidTable` per ``(component, table, version)`` segment
        group, in first-appearance order. Numeric-looking columns are
        converted; timestamp columns are left as strings for the caller
        to parse (AEMO quotes them as ``YYYY/MM/DD HH:MM:SS``).

    """
    if isinstance(source, (str, Path)):
        with open(source, newline="", encoding="utf-8", errors="replace") as fh:
            return _parse_rows(csv.reader(fh))
    return _parse_rows(csv.reader(source))


def parse_cid_zip(zip_path: str | Path) -> list[CidTable]:
    """Parse every CSV inside a nemweb zip payload.

    Args:
        zip_path: Path to a downloaded ``.zip`` payload.

    Returns:
        Concatenated results of :func:`parse_cid` over each inner CSV.

    """
    tables: list[CidTable] = []
    with zipfile.ZipFile(zip_path) as archive:
        for inner in archive.namelist():
            if inner.lower().endswith(".csv"):
                with archive.open(inner) as raw:
                    text = io.TextIOWrapper(
                        raw,
                        encoding="utf-8",
                        errors="replace",
                        newline="",
                    )
                    tables.extend(_parse_rows(csv.reader(text)))
    return tables


def _parse_rows(rows: Iterator[list[str]]) -> list[CidTable]:
    """Group tagged rows into per-table DataFrames."""
    columns: dict[TableKey, list[str]] = {}
    data: dict[TableKey, list[list[str]]] = {}
    current: TableKey | None = None

    for row in rows:
        if not row:
            continue
        tag = row[0]
        if tag == "I":
            current = TableKey(row[1], row[2], int(row[3]))
            columns.setdefault(current, row[4:])
            data.setdefault(current, [])
        elif tag == "D" and current is not None:
            # D rows echo component/table/version in fields 1-3; trust the
            # explicit fields rather than assuming they match `current` —
            # AEMO files have been observed to interleave segments.
            key = TableKey(row[1], row[2], int(row[3]))
            target = key if key in columns else current
            data[target].append(row[4:])

    tables: list[CidTable] = []
    for key, cols in columns.items():
        frame = pd.DataFrame(data[key], columns=cols)
        for col in frame.columns:
            converted = pd.to_numeric(frame[col], errors="coerce")
            if (
                not converted.isna().all()
                and converted.notna().eq(frame[col].ne("")).all()
            ):
                frame[col] = converted
        tables.append(CidTable(key=key, frame=frame))
    return tables
