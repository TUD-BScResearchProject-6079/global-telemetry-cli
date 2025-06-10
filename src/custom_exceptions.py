from datetime import date


class InvalidDateError(ValueError):
    """Raised when the input value is invalid in a custom way."""

    def __init__(
        self, message: str = "The script can only run on dates that have already completed (past UTC dates)."
    ) -> None:
        super().__init__(message)


class InvalidDateRangeError(ValueError):
    """Raised when the date range is invalid."""

    def __init__(self, start_date: date, end_date: date) -> None:
        message = f"Invalid date range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}. Start date must be before end date."
        super().__init__(message)
