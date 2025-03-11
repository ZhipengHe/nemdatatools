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


def test_format_date_with_time():
    """Test formatting datetime with time component."""
    dt = datetime.datetime(2023, 1, 1, 12, 30, 0)
    result = timeutils.format_date(dt, include_time=True)

    assert result == "2023/01/01 12:30:00"


def test_format_date_without_time():
    """Test formatting datetime without time component."""
    dt = datetime.datetime(2023, 1, 1, 12, 30, 0)
    result = timeutils.format_date(dt, include_time=False)

    assert result == "2023/01/01"


def test_generate_time_periods():
    """Test generating time periods between dates."""
    start_date = datetime.datetime(2023, 1, 1)
    end_date = datetime.datetime(2023, 1, 3)
    data_type = "DISPATCHPRICE"

    result = timeutils.generate_time_periods(start_date, end_date, data_type)

    assert len(result) == 3  # 3 days
    assert result[0] == {"year": "2023", "month": "01", "date": "20230101"}
    assert result[1] == {"year": "2023", "month": "01", "date": "20230102"}
    assert result[2] == {"year": "2023", "month": "01", "date": "20230103"}


def test_generate_intervals():
    """Test generating time intervals between dates."""
    start_date = datetime.datetime(2023, 1, 1, 0, 0)
    end_date = datetime.datetime(2023, 1, 1, 0, 15)

    result = timeutils.generate_intervals(start_date, end_date)

    assert len(result) == 4  # 4 intervals (0:00, 0:05, 0:10, 0:15)
    assert result[0].minute == 0
    assert result[1].minute == 5
    assert result[2].minute == 10
    assert result[3].minute == 15

    # Check timezone
    assert result[0].tzinfo == timeutils.AEST_OFFSET


def test_generate_intervals_custom_interval():
    """Test generating time intervals with custom interval."""
    start_date = datetime.datetime(2023, 1, 1, 0, 0)
    end_date = datetime.datetime(2023, 1, 1, 1, 0)

    result = timeutils.generate_intervals(start_date, end_date, interval="30min")

    assert len(result) == 3  # 3 intervals (0:00, 0:30, 1:00)
    assert result[0].minute == 0
    assert result[1].minute == 30
    assert result[2].minute == 0
    assert result[2].hour == 1


def test_get_forecast_horizon():
    """Test calculating forecast horizon between times."""
    run_time = datetime.datetime(2023, 1, 1, 12, 0)
    target_time = datetime.datetime(2023, 1, 1, 14, 30)

    result = timeutils.get_forecast_horizon(run_time, target_time)

    assert result == datetime.timedelta(hours=2, minutes=30)


def test_get_forecast_horizon_invalid():
    """Test forecast horizon with invalid target time raises ValueError."""
    run_time = datetime.datetime(2023, 1, 1, 12, 0)
    target_time = datetime.datetime(2023, 1, 1, 10, 30)  # Before run_time

    with pytest.raises(ValueError):
        timeutils.get_forecast_horizon(run_time, target_time)


def test_get_data_type_interval():
    """Test getting interval for a data type."""
    assert timeutils.get_data_type_interval("DISPATCHPRICE") == "5min"
    assert timeutils.get_data_type_interval("PREDISPATCH") == "30min"
    assert timeutils.get_data_type_interval("P5MIN") == "5min"


def test_get_data_type_interval_invalid():
    """Test getting interval for invalid data type raises ValueError."""
    with pytest.raises(ValueError):
        timeutils.get_data_type_interval("INVALID_TYPE")


def test_is_dispatch_interval():
    """Test checking if datetime is on a dispatch interval boundary."""
    # Valid dispatch intervals
    assert timeutils.is_dispatch_interval(datetime.datetime(2023, 1, 1, 12, 0, 0))
    assert timeutils.is_dispatch_interval(datetime.datetime(2023, 1, 1, 12, 5, 0))
    assert timeutils.is_dispatch_interval(datetime.datetime(2023, 1, 1, 12, 30, 0))

    # Invalid dispatch intervals
    assert not timeutils.is_dispatch_interval(datetime.datetime(2023, 1, 1, 12, 1, 0))
    assert not timeutils.is_dispatch_interval(datetime.datetime(2023, 1, 1, 12, 0, 1))


def test_get_next_interval_5min():
    """Test getting next 5-minute interval."""
    # Within hour
    dt = datetime.datetime(2023, 1, 1, 12, 3, 30)
    result = timeutils.get_next_interval(dt, interval="5min")
    assert result == datetime.datetime(2023, 1, 1, 12, 5, 0)

    # Exactly on interval
    dt = datetime.datetime(2023, 1, 1, 12, 5, 0)
    result = timeutils.get_next_interval(dt, interval="5min")
    assert result == datetime.datetime(2023, 1, 1, 12, 10, 0)

    # Crossing hour boundary
    dt = datetime.datetime(2023, 1, 1, 12, 58, 30)
    result = timeutils.get_next_interval(dt, interval="5min")
    assert result == datetime.datetime(2023, 1, 1, 13, 0, 0)


def test_get_next_interval_30min():
    """Test getting next 30-minute interval."""
    # First half of hour
    dt = datetime.datetime(2023, 1, 1, 12, 15, 30)
    result = timeutils.get_next_interval(dt, interval="30min")
    assert result == datetime.datetime(2023, 1, 1, 12, 30, 0)

    # Second half of hour
    dt = datetime.datetime(2023, 1, 1, 12, 45, 30)
    result = timeutils.get_next_interval(dt, interval="30min")
    assert result == datetime.datetime(2023, 1, 1, 13, 0, 0)


def test_get_next_interval_invalid():
    """Test getting next interval with inalid interval other than 5min or 30min."""
    dt = datetime.datetime(2023, 1, 1, 12, 0, 0)

    with pytest.raises(ValueError):
        timeutils.get_next_interval(dt, interval="15min")


def test_get_interval_boundaries_5min():
    """Test adjusting boundaries to 5-minute intervals."""
    # Start and end already on boundaries
    start = datetime.datetime(2023, 1, 1, 12, 0, 0)
    end = datetime.datetime(2023, 1, 1, 12, 15, 0)
    start_adj, end_adj = timeutils.get_interval_boundaries(start, end, interval="5min")
    assert start_adj == start
    assert end_adj == end

    # Start and end need adjustment
    start = datetime.datetime(2023, 1, 1, 12, 2, 30)
    end = datetime.datetime(2023, 1, 1, 12, 17, 30)
    start_adj, end_adj = timeutils.get_interval_boundaries(start, end, interval="5min")
    assert start_adj == datetime.datetime(2023, 1, 1, 12, 5, 0)
    assert end_adj == datetime.datetime(2023, 1, 1, 12, 15, 0)


def test_get_interval_boundaries_30min():
    """Test adjusting boundaries to 30-minute intervals."""
    # Start and end already on boundaries (start on 0, end on 0)
    start = datetime.datetime(2023, 1, 1, 12, 0, 0)
    end = datetime.datetime(2023, 1, 1, 13, 0, 0)
    start_adj, end_adj = timeutils.get_interval_boundaries(start, end, interval="30min")
    assert start_adj == start
    assert end_adj == end

    # Start and end already on boundaries (start on 30, end on 30)
    start = datetime.datetime(2023, 1, 1, 12, 30, 0)
    end = datetime.datetime(2023, 1, 1, 13, 30, 0)
    start_adj, end_adj = timeutils.get_interval_boundaries(start, end, interval="30min")
    assert start_adj == start
    assert end_adj == end

    # Start and end need adjustment (start between 0 and 30, end between 0 and 30)
    start = datetime.datetime(2023, 1, 1, 12, 10, 30)
    end = datetime.datetime(2023, 1, 1, 13, 25, 30)
    start_adj, end_adj = timeutils.get_interval_boundaries(start, end, interval="30min")
    assert start_adj == datetime.datetime(2023, 1, 1, 12, 30, 0)
    assert end_adj == datetime.datetime(2023, 1, 1, 13, 0, 0)

    # Start and end need adjustment (start between 30 and 60, end between 30 and 60)
    start = datetime.datetime(2023, 1, 1, 12, 40, 30)
    end = datetime.datetime(2023, 1, 1, 13, 45, 30)
    start_adj, end_adj = timeutils.get_interval_boundaries(start, end, interval="30min")
    assert start_adj == datetime.datetime(2023, 1, 1, 13, 0, 0)
    assert end_adj == datetime.datetime(2023, 1, 1, 13, 30, 0)


def test_get_interval_boundaries_invalid():
    """Test adjusting boundaries with invalid interval raises ValueError."""
    start = datetime.datetime(2023, 1, 1, 12, 0, 0)
    end = datetime.datetime(2023, 1, 1, 13, 0, 0)

    with pytest.raises(ValueError):
        timeutils.get_interval_boundaries(start, end, interval="15min")
