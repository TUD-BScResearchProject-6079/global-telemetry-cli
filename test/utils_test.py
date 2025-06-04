from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from src.utils import parse_date, parse_date_range


@patch("src.utils.logger")
def test_parse_date_valid(mock_logger: MagicMock) -> None:
    expected = datetime(2023, 10, 1).date()
    date = expected.strftime("%Y-%m-%d")

    result = parse_date(date)
    assert result == expected
    mock_logger.info.assert_not_called()
    mock_logger.error.assert_not_called()


@patch("src.utils.logger")
def test_parse_date_invalid_format(mock_logger: MagicMock) -> None:
    date = "01-05-2027"

    with pytest.raises(ValueError):
        result = parse_date(date)
        assert result is None
    mock_logger.info.assert_not_called()
    mock_logger.error.assert_called_once()
    mock_logger.error.assert_called_with(f"Invalid date format: {date}. Expected date format: yyyy-mm-dd.")


@patch("src.utils.logger")
def test_parse_date_range_valid(mock_logger: MagicMock) -> None:
    expected_start = datetime(2023, 10, 1).date()
    expected_end = datetime(2023, 10, 31).date()
    date_range = f"{expected_start.strftime('%Y-%m-%d')}:{expected_end.strftime('%Y-%m-%d')}"

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

    mock_datetime.now.return_value = datetime(
        expected_end.year, expected_end.month, expected_end.day, tzinfo=timezone.utc
    )
    mock_datetime.side_effect = lambda *args, **kwargs: datetime(*args, **kwargs)
    mock_datetime.strptime = datetime.strptime

    start_date, end_date = parse_date_range(date_range)
    assert start_date == expected_start
    assert end_date == expected_end
    mock_logger.info.assert_not_called()
    mock_logger.error.assert_not_called()


@patch("src.utils.logger")
def test_parse_date_range_invalid_format(mock_logger: MagicMock) -> None:
    date_range = "2023-10-01 to 2023-10-31"

    with pytest.raises(ValueError):
        start_date, end_date = parse_date_range(date_range)
        assert start_date is None
        assert end_date is None
    mock_logger.info.assert_not_called()
    mock_logger.error.assert_called_once()
    mock_logger.error.assert_called_with(f"Invalid date format: {date_range}. Expected date format: yyyy-mm-dd.")
