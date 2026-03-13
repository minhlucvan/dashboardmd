"""Time intelligence: granularity, period comparison."""

from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any

_VALID_GRANULARITIES = {"day", "week", "month", "quarter", "year"}

_VALID_PRESETS = {
    "last_7_days",
    "last_30_days",
    "last_90_days",
    "this_month",
    "this_quarter",
    "this_year",
}


def time_trunc_sql(column: str, granularity: str) -> str:
    """Generate a DATE_TRUNC SQL expression.

    Args:
        column: The date/timestamp column name.
        granularity: One of 'day', 'week', 'month', 'quarter', 'year'.

    Returns:
        SQL expression string like DATE_TRUNC('month', order_date).
    """
    if granularity not in _VALID_GRANULARITIES:
        raise ValueError(
            f"Invalid granularity '{granularity}'. Must be one of: {', '.join(sorted(_VALID_GRANULARITIES))}"
        )
    return f"DATE_TRUNC('{granularity}', {column})"


def previous_period_range(current_start: str, current_end: str) -> tuple[str, str]:
    """Calculate the previous period date range matching the current period length.

    Args:
        current_start: Start date as YYYY-MM-DD string.
        current_end: End date as YYYY-MM-DD string.

    Returns:
        Tuple of (prev_start, prev_end) as YYYY-MM-DD strings.
    """
    start = _parse_date(current_start)
    end = _parse_date(current_end)
    delta = end - start
    prev_end = start - timedelta(days=1)
    prev_start = prev_end - delta
    return prev_start.isoformat(), prev_end.isoformat()


def previous_year_range(current_start: str, current_end: str) -> tuple[str, str]:
    """Shift dates back exactly one year.

    Args:
        current_start: Start date as YYYY-MM-DD string.
        current_end: End date as YYYY-MM-DD string.

    Returns:
        Tuple of (prev_start, prev_end) as YYYY-MM-DD strings.
    """
    start = _parse_date(current_start)
    end = _parse_date(current_end)
    prev_start = start.replace(year=start.year - 1)
    prev_end = end.replace(year=end.year - 1)
    return prev_start.isoformat(), prev_end.isoformat()


def period_comparison_sql(
    measure_sql: str,
    date_column: str,
    current_start: str,
    current_end: str,
    compare: str = "previous_period",
) -> str:
    """Generate SQL for fetching both current and previous period data.

    Args:
        measure_sql: The aggregation SQL expression (e.g. "SUM(amount)").
        date_column: The date column to filter on.
        current_start: Current period start date (YYYY-MM-DD).
        current_end: Current period end date (YYYY-MM-DD).
        compare: "previous_period" or "previous_year".

    Returns:
        SQL string with both current and previous period calculations.
    """
    if compare == "previous_year":
        prev_start, prev_end = previous_year_range(current_start, current_end)
    else:
        prev_start, prev_end = previous_period_range(current_start, current_end)

    return (
        f"SELECT "
        f"  {measure_sql} FILTER (WHERE {date_column} BETWEEN '{current_start}' AND '{current_end}') AS current_value, "
        f"  {measure_sql} FILTER (WHERE {date_column} BETWEEN '{prev_start}' AND '{prev_end}') AS previous_value"
    )


def compute_delta(current: float, previous: float) -> dict[str, Any]:
    """Compute absolute and percentage change between two values.

    Args:
        current: Current period value.
        previous: Previous period value.

    Returns:
        Dict with 'absolute' and 'percentage' keys.
    """
    absolute = current - previous
    if previous == 0:
        percentage = None
    else:
        percentage = (absolute / previous) * 100
    return {"absolute": absolute, "percentage": percentage}


def resolve_date_preset(preset: str) -> tuple[str, str]:
    """Resolve a named date preset to a (start, end) date range.

    Args:
        preset: One of 'last_7_days', 'last_30_days', 'last_90_days',
                'this_month', 'this_quarter', 'this_year'.

    Returns:
        Tuple of (start_date, end_date) as YYYY-MM-DD strings.
    """
    if preset not in _VALID_PRESETS:
        raise ValueError(
            f"Invalid preset '{preset}'. Must be one of: {', '.join(sorted(_VALID_PRESETS))}"
        )

    today = date.today()

    if preset == "last_7_days":
        start = today - timedelta(days=6)
        return start.isoformat(), today.isoformat()
    elif preset == "last_30_days":
        start = today - timedelta(days=29)
        return start.isoformat(), today.isoformat()
    elif preset == "last_90_days":
        start = today - timedelta(days=89)
        return start.isoformat(), today.isoformat()
    elif preset == "this_month":
        start = today.replace(day=1)
        return start.isoformat(), today.isoformat()
    elif preset == "this_quarter":
        quarter_start_month = ((today.month - 1) // 3) * 3 + 1
        start = today.replace(month=quarter_start_month, day=1)
        return start.isoformat(), today.isoformat()
    elif preset == "this_year":
        start = today.replace(month=1, day=1)
        return start.isoformat(), today.isoformat()

    raise ValueError(f"Unhandled preset: {preset}")


def _parse_date(date_str: str) -> date:
    """Parse a YYYY-MM-DD string to a date object."""
    return datetime.strptime(date_str, "%Y-%m-%d").date()
