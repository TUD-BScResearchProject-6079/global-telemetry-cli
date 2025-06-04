from datetime import datetime, timezone, timedelta
from unittest.mock import MagicMock, patch

import pytest

from src.utils import parse_date, parse_date_range
from src.custom_exceptions import InvalidDateError, InvalidDateRangeError


@patch("src.utils.logger")
@patch("src.utils.datetime")
def test_parse_date_valid(mock_datetime: MagicMock, mock_logger: MagicMock) -> None:
    expected_date = datetime(2023, 10, 1).date()
    date = expected_date.strftime("%Y-%m-%d")

    now_date = expected_date + timedelta(weeks = 20, days = 25) 
    mock_datetime.now.return_value = datetime(
        now_date.year, now_date.month, now_date.day, tzinfo=timezone.utc
    )
    mock_datetime.side_effect = lambda *args, **kwargs: datetime(*args, **kwargs)
    mock_datetime.strptime = datetime.strptime

    result = parse_date(date)
    assert result == expected_date
    mock_logger.info.assert_not_called()
    mock_logger.error.assert_not_called()



@patch("src.utils.logger")
@patch("src.utils.datetime")
def test_parse_date_in_the_future(mock_datetime: MagicMock, mock_logger: MagicMock) -> None:
    expected_date = datetime(2023, 10, 1).date()
    date_str = expected_date.strftime("%Y-%m-%d")

    now_date = expected_date - timedelta(weeks = 20, days = 25) 
    mock_datetime.now.return_value = datetime(
        now_date.year, now_date.month, now_date.day, tzinfo=timezone.utc
    )
    mock_datetime.side_effect = lambda *args, **kwargs: datetime(*args, **kwargs)
    mock_datetime.strptime = datetime.strptime

    with pytest.raises(InvalidDateError):
        result = parse_date(date_str)
        assert result is None
    mock_logger.info.assert_not_called()
    mock_logger.error.assert_called_once()
    mock_logger.error.assert_called_with(f"Invalid date: {date_str}. The script can only run on dates that have already completed (past UTC dates).")


@patch("src.utils.logger")
@patch("src.utils.datetime")
def test_parse_date_today(mock_datetime: MagicMock, mock_logger: MagicMock) -> None:
    expected_date = datetime(2023, 10, 1).date()
    date_str = expected_date.strftime("%Y-%m-%d")

    now_date = expected_date
    mock_datetime.now.return_value = datetime(
        now_date.year, now_date.month, now_date.day, tzinfo=timezone.utc
    )
    mock_datetime.side_effect = lambda *args, **kwargs: datetime(*args, **kwargs)
    mock_datetime.strptime = datetime.strptime

    with pytest.raises(InvalidDateError):
        result = parse_date(date_str)
        assert result is None
    mock_logger.info.assert_not_called()
    mock_logger.error.assert_called_once()
    mock_logger.error.assert_called_with(f"Invalid date: {date_str}. The script can only run on dates that have already completed (past UTC dates).")


@patch("src.utils.logger")
@patch("src.utils.datetime")
def test_parse_date_invalid_format(mock_datetime: MagicMock, mock_logger: MagicMock) -> None:
    date = datetime(2024, 11, 15).date()
    date_str = date.strftime("%d-%m-%Y")
    
    now_date = date + timedelta(weeks = 20, days = 25) 
    mock_datetime.now.return_value = datetime(
        now_date.year, now_date.month, now_date.day, tzinfo=timezone.utc
    )
    mock_datetime.side_effect = lambda *args, **kwargs: datetime(*args, **kwargs)
    mock_datetime.strptime = datetime.strptime

    with pytest.raises(ValueError):
        result = parse_date(date_str)
        assert result is None
    mock_logger.info.assert_not_called()
    mock_logger.error.assert_called_once()
    mock_logger.error.assert_called_with(f"Invalid date format: {date_str}. Expected date format: yyyy-mm-dd.")


@patch("src.utils.logger")
@patch("src.utils.datetime")
def test_parse_date_range_valid(mock_datetime: MagicMock, mock_logger: MagicMock) -> None:
    expected_start = datetime(2023, 10, 1).date()
    expected_end = datetime(2023, 10, 31).date()
    date_range = f"{expected_start.strftime('%Y-%m-%d')}:{expected_end.strftime('%Y-%m-%d')}"

    now_date = expected_end + timedelta(weeks = 20, days = 25) 
    mock_datetime.now.return_value = datetime(
        now_date.year, now_date.month, now_date.day, tzinfo=timezone.utc
    )
    mock_datetime.side_effect = lambda *args, **kwargs: datetime(*args, **kwargs)
    mock_datetime.strptime = datetime.strptime

    start_date, end_date = parse_date_range(date_range)
    assert start_date == expected_start
    assert end_date == expected_end
    mock_logger.info.assert_not_called()
    mock_logger.error.assert_not_called()


@patch("src.utils.logger")
@patch("src.utils.datetime")
def test_parse_date_range_valid_no_right(mock_datetime: MagicMock, mock_logger: MagicMock) -> None:
    expected_start = datetime(2024, 11, 12).date()
    expected_end = datetime(2025, 1, 24).date()
    date_range = expected_start.strftime("%Y-%m-%d")

    next_day = expected_end + timedelta(days=1)
    mock_datetime.now.return_value = datetime(
        next_day.year, next_day.month, next_day.day, tzinfo=timezone.utc
    )
    mock_datetime.side_effect = lambda *args, **kwargs: datetime(*args, **kwargs)
    mock_datetime.strptime = datetime.strptime

    start_date, end_date = parse_date_range(date_range)
    assert start_date == expected_start
    assert end_date == expected_end
    mock_logger.info.assert_not_called()
    mock_logger.error.assert_not_called()


@patch("src.utils.logger")
@patch("src.utils.datetime")
def test_parse_date_range_invalid_format(mock_datetime: MagicMock, mock_logger: MagicMock) -> None:
    expected_start = datetime(2024, 11, 12).date()
    expected_end = datetime(2025, 1, 24).date()
    date_range = f"{expected_start.strftime('%Y-%m-%d')} to {expected_end.strftime('%Y-%m-%d')}"

    now_date = expected_end + timedelta(weeks = 20, days = 25) 
    mock_datetime.now.return_value = datetime(
        now_date.year, now_date.month, now_date.day, tzinfo=timezone.utc
    )
    mock_datetime.side_effect = lambda *args, **kwargs: datetime(*args, **kwargs)
    mock_datetime.strptime = datetime.strptime

    with pytest.raises(ValueError):
        start_date, end_date = parse_date_range(date_range)
        assert start_date is None
        assert end_date is None
    mock_logger.info.assert_not_called()
    mock_logger.error.assert_called_once()
    mock_logger.error.assert_called_with(f"Invalid date format: {date_range}. Expected date format: yyyy-mm-dd.")


@patch("src.utils.logger")
@patch("src.utils.datetime")
def test_parse_date_range_start_greater_than_end(mock_datetime: MagicMock, mock_logger: MagicMock) -> None:
    expected_start = datetime(2024, 11, 12).date()
    expected_end = expected_start - timedelta(days=15)
    start_str = expected_start.strftime('%Y-%m-%d')
    end_str = expected_end.strftime('%Y-%m-%d')
    date_range = f"{start_str}:{end_str}"

    now_date = expected_start + timedelta(weeks = 20, days = 25) 
    mock_datetime.now.return_value = datetime(
        now_date.year, now_date.month, now_date.day, tzinfo=timezone.utc
    )
    mock_datetime.side_effect = lambda *args, **kwargs: datetime(*args, **kwargs)
    mock_datetime.strptime = datetime.strptime

    with pytest.raises(InvalidDateRangeError):
        start_date, end_date = parse_date_range(date_range)
        assert start_date is None
        assert end_date is None
    mock_logger.info.assert_not_called()
    mock_logger.error.assert_called_once()
    mock_logger.error.assert_called_with(f"Invalid date range: {start_str} to {end_str}. Start date cannot be after end date.")

@patch("src.utils.logger")
@patch("src.utils.datetime")
def test_parse_date_range_end_in_future(mock_datetime: MagicMock, mock_logger: MagicMock) -> None:
    expected_start = datetime(2024, 11, 12).date()
    expected_end = expected_start + timedelta(days=15)
    start_str = expected_start.strftime('%Y-%m-%d')
    end_str = expected_end.strftime('%Y-%m-%d')
    date_range = f"{start_str}:{end_str}"

    now_date = expected_start + timedelta(days = 10) 
    mock_datetime.now.return_value = datetime(
        now_date.year, now_date.month, now_date.day, tzinfo=timezone.utc
    )
    mock_datetime.side_effect = lambda *args, **kwargs: datetime(*args, **kwargs)
    mock_datetime.strptime = datetime.strptime

    with pytest.raises(InvalidDateError):
        start_date, end_date = parse_date_range(date_range)
        assert start_date is None
        assert end_date is None
    mock_logger.info.assert_not_called()
    mock_logger.error.assert_called_once()
    mock_logger.error.assert_called_with(f"Invalid date: {end_str}. The script can only run on dates that have already completed (past UTC dates).")

@patch("src.utils.logger")
@patch("src.utils.datetime")
def test_parse_date_range_start_today(mock_datetime: MagicMock, mock_logger: MagicMock) -> None:
    expected_start = datetime(2024, 11, 12).date()
    expected_end = expected_start + timedelta(days=15)
    start_str = expected_start.strftime('%Y-%m-%d')
    end_str = expected_end.strftime('%Y-%m-%d')
    date_range = f"{start_str}:{end_str}"

    now_date = expected_start 
    mock_datetime.now.return_value = datetime(
        now_date.year, now_date.month, now_date.day, tzinfo=timezone.utc
    )
    mock_datetime.side_effect = lambda *args, **kwargs: datetime(*args, **kwargs)
    mock_datetime.strptime = datetime.strptime

    with pytest.raises(InvalidDateError):
        start_date, end_date = parse_date_range(date_range)
        assert start_date is None
        assert end_date is None
    mock_logger.info.assert_not_called()
    mock_logger.error.assert_called_once()
    mock_logger.error.assert_called_with(f"Invalid date: {start_str}. The script can only run on dates that have already completed (past UTC dates).")
