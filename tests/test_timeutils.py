"""Tests for the timeutils module."""

import datetime

import pytest

from nemdatatools import timeutils


def test_parse_date_with_time():
    """Test parsing date string with time component."""
    date_str = "2023/01/01 12:30:00"
    result = timeutils.parse_date(date_str)

    assert isinstance(result, datetime.datetime)
    assert result.year == 2023
    assert result.month == 1
    assert result.day == 1
    assert result.hour == 12
    assert result.minute == 30


def test_parse_date_without_time():
    """Test parsing date string without time component."""
    date_str = "2023/01/01"
    result = timeutils.parse_date(date_str)

    assert isinstance(result, datetime.datetime)
    assert result.year == 2023
    assert result.month == 1
    assert result.day == 1
    assert result.hour == 0
    assert result.minute == 0


def test_parse_date_invalid_format():
    """Test parsing date with invalid format raises ValueError."""
    date_str = "2023-01-01"  # Wrong format (hyphen instead of slash)

    with pytest.raises(ValueError):
        timeutils.parse_date(date_str)
