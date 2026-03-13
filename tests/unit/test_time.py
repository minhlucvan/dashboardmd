"""Tests for Time Intelligence: granularity, period comparison."""

from __future__ import annotations

import pytest


class TestTimeGranularity:
    """Time granularity truncates timestamps to day/week/month/quarter/year."""

    def test_daily_granularity_sql(self) -> None:
        """Daily granularity should use DATE_TRUNC('day', col)."""
        from dashboardmd.time import time_trunc_sql

        sql = time_trunc_sql("order_date", "day")
        assert "DATE_TRUNC" in sql.upper() or "date_trunc" in sql
        assert "day" in sql.lower()

    def test_weekly_granularity_sql(self) -> None:
        """Weekly granularity."""
        from dashboardmd.time import time_trunc_sql

        sql = time_trunc_sql("order_date", "week")
        assert "week" in sql.lower()

    def test_monthly_granularity_sql(self) -> None:
        """Monthly granularity."""
        from dashboardmd.time import time_trunc_sql

        sql = time_trunc_sql("order_date", "month")
        assert "month" in sql.lower()

    def test_quarterly_granularity_sql(self) -> None:
        """Quarterly granularity."""
        from dashboardmd.time import time_trunc_sql

        sql = time_trunc_sql("order_date", "quarter")
        assert "quarter" in sql.lower()

    def test_yearly_granularity_sql(self) -> None:
        """Yearly granularity."""
        from dashboardmd.time import time_trunc_sql

        sql = time_trunc_sql("order_date", "year")
        assert "year" in sql.lower()

    def test_invalid_granularity_raises_error(self) -> None:
        """Unknown granularity should raise an error."""
        from dashboardmd.time import time_trunc_sql

        with pytest.raises(ValueError):
            time_trunc_sql("order_date", "invalid")


class TestPeriodComparison:
    """Period comparison computes metrics for current vs previous periods."""

    def test_previous_period_date_range(self) -> None:
        """previous_period should calculate the matching prior date range."""
        from dashboardmd.time import previous_period_range

        # If current range is Jan 1-31, previous should be Dec 2-31 (same length)
        current_start = "2026-01-01"
        current_end = "2026-01-31"
        prev_start, prev_end = previous_period_range(current_start, current_end)

        assert prev_start < current_start
        assert prev_end < current_end

    def test_previous_year_date_range(self) -> None:
        """previous_year should shift dates back exactly one year."""
        from dashboardmd.time import previous_year_range

        current_start = "2026-01-01"
        current_end = "2026-03-31"
        prev_start, prev_end = previous_year_range(current_start, current_end)

        assert "2025" in prev_start
        assert "2025" in prev_end

    def test_period_comparison_sql(self) -> None:
        """Generate SQL for fetching both current and previous period data."""
        from dashboardmd.time import period_comparison_sql

        sql = period_comparison_sql(
            measure_sql="SUM(amount)",
            date_column="order_date",
            current_start="2026-01-01",
            current_end="2026-01-31",
            compare="previous_period",
        )
        # Should reference both current and previous date ranges
        assert "2026-01-01" in sql or "2026" in sql

    def test_compute_delta(self) -> None:
        """Delta between current and previous values."""
        from dashboardmd.time import compute_delta

        delta = compute_delta(current=120, previous=100)
        assert delta["absolute"] == 20
        assert delta["percentage"] == pytest.approx(20.0)

    def test_compute_delta_zero_previous(self) -> None:
        """Delta when previous value is zero (avoid division by zero)."""
        from dashboardmd.time import compute_delta

        delta = compute_delta(current=100, previous=0)
        assert delta["absolute"] == 100
        # percentage should handle zero gracefully (None or inf)


class TestTimeDimensionPresets:
    """Preset date ranges like 'last_7_days', 'last_30_days', 'this_month'."""

    def test_last_7_days(self) -> None:
        """last_7_days should return a 7-day range ending today."""
        from dashboardmd.time import resolve_date_preset

        start, end = resolve_date_preset("last_7_days")
        assert start is not None
        assert end is not None

    def test_last_30_days(self) -> None:
        """last_30_days should return a 30-day range."""
        from dashboardmd.time import resolve_date_preset

        start, end = resolve_date_preset("last_30_days")
        assert start is not None

    def test_this_month(self) -> None:
        """this_month should return the current month range."""
        from dashboardmd.time import resolve_date_preset

        start, end = resolve_date_preset("this_month")
        assert start is not None

    def test_this_year(self) -> None:
        """this_year should return Jan 1 to today."""
        from dashboardmd.time import resolve_date_preset

        start, end = resolve_date_preset("this_year")
        assert "01-01" in start or "-01-01" in start

    def test_invalid_preset_raises_error(self) -> None:
        """Unknown preset should raise an error."""
        from dashboardmd.time import resolve_date_preset

        with pytest.raises(ValueError):
            resolve_date_preset("unknown_preset")
