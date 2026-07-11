"""Errors raised by nemdatatools.

The package's design promise is that missing data fails loudly with a
domain explanation — never a bare 404 and never a silently partial or
empty DataFrame.
"""

from __future__ import annotations


class NemDataError(Exception):
    """Base class for nemdatatools errors."""


class AvailabilityGapError(NemDataError):
    """The requested range crosses a known hole in a table's history.

    The message names the gap and the substitute table to use across it;
    callers who want partial data can re-request around the gap bounds.
    """


class CoverageError(NemDataError):
    """No publication tier can serve part of the requested range."""
