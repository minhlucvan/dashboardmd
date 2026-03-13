"""Tests for Refresh: re-run dashboards and track metric diffs."""

from __future__ import annotations

from pathlib import Path

import pytest


class TestRefresh:
    """refresh() re-runs a dashboard and compares metrics to the previous run."""

    def test_refresh_produces_new_output(self, tmp_output_dir: Path) -> None:
        """refresh() should re-render the dashboard to the output path."""
        from dashboardmd.refresh import refresh

        # First need a dashboard to refresh
        # This test verifies that the refresh mechanism can be invoked
        # and produces a new output file
        pytest.skip("Requires full Dashboard implementation")

    def test_refresh_returns_metric_diff(self) -> None:
        """refresh() should return a diff of current vs previous metric values."""
        from dashboardmd.refresh import MetricDiff

        diff = MetricDiff(
            metric_name="orders.revenue",
            previous_value=10000,
            current_value=12000,
        )
        assert diff.absolute_change == 2000
        assert diff.percentage_change == pytest.approx(20.0)

    def test_metric_diff_negative_change(self) -> None:
        """MetricDiff should handle decreases correctly."""
        from dashboardmd.refresh import MetricDiff

        diff = MetricDiff(
            metric_name="orders.count",
            previous_value=100,
            current_value=80,
        )
        assert diff.absolute_change == -20
        assert diff.percentage_change == pytest.approx(-20.0)

    def test_metric_diff_no_change(self) -> None:
        """MetricDiff with no change should show zero delta."""
        from dashboardmd.refresh import MetricDiff

        diff = MetricDiff(
            metric_name="orders.count",
            previous_value=100,
            current_value=100,
        )
        assert diff.absolute_change == 0
        assert diff.percentage_change == pytest.approx(0.0)

    def test_metric_diff_from_zero(self) -> None:
        """MetricDiff from zero previous should handle gracefully."""
        from dashboardmd.refresh import MetricDiff

        diff = MetricDiff(
            metric_name="orders.count",
            previous_value=0,
            current_value=50,
        )
        assert diff.absolute_change == 50

    def test_metric_diff_to_string(self) -> None:
        """MetricDiff should have a human-readable representation."""
        from dashboardmd.refresh import MetricDiff

        diff = MetricDiff(
            metric_name="orders.revenue",
            previous_value=10000,
            current_value=12000,
        )
        text = str(diff)
        assert "revenue" in text.lower() or "orders.revenue" in text
