"""Tests for Refresh: re-run dashboards and track metric diffs."""

from __future__ import annotations

from pathlib import Path

import pytest


class TestRefresh:
    """refresh() re-runs a dashboard and compares metrics to the previous run."""

    def test_refresh_produces_new_output(self, tmp_output_dir: Path, orders_csv: Path) -> None:
        """refresh() should re-render the dashboard to the output path."""
        from dashboardmd.dashboard import Dashboard
        from dashboardmd.model import Dimension, Entity, Measure
        from dashboardmd.refresh import refresh

        orders = Entity(
            name="orders",
            source=orders_csv,
            dimensions=[Dimension("id", type="number", primary_key=True)],
            measures=[Measure("count", type="count")],
        )
        output_path = tmp_output_dir / "refresh_test.md"
        dash = Dashboard(title="Test", entities=[orders], output=str(output_path))
        dash.section("Overview")
        dash.tile("orders.count")
        dash.save()

        # First refresh — no previous snapshot, returns empty diffs
        diffs = refresh(dash, snapshot_path=tmp_output_dir / "snapshot.json")
        assert diffs == []
        assert output_path.exists()

        # Second refresh — compares to previous snapshot
        diffs = refresh(dash, snapshot_path=tmp_output_dir / "snapshot.json")
        assert len(diffs) >= 1
        assert diffs[0].absolute_change == 0  # Same data, no change

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


class TestSnapshot:
    """Snapshot save/load for metric tracking."""

    def test_snapshot_save_and_load(self, tmp_path: Path) -> None:
        """Snapshot should save to JSON and load back."""
        from dashboardmd.refresh import Snapshot

        snap = Snapshot(title="Test", metrics={"orders.count": 20.0})
        path = tmp_path / "snap.json"
        snap.save(path)
        assert path.exists()

        loaded = Snapshot.load(path)
        assert loaded.title == "Test"
        assert loaded.metrics["orders.count"] == 20.0
