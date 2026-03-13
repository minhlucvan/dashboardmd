"""Re-run + metric diff tracking.

Refresh re-executes a dashboard and compares metric values to a previous
snapshot, producing MetricDiff instances that track absolute and percentage
changes.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any


@dataclass
class MetricDiff:
    """Tracks the change between two metric values."""

    metric_name: str
    previous_value: float
    current_value: float

    @property
    def absolute_change(self) -> float:
        """Absolute change (current - previous)."""
        return self.current_value - self.previous_value

    @property
    def percentage_change(self) -> float:
        """Percentage change relative to previous value.

        Returns 0.0 if previous_value is zero.
        """
        if self.previous_value == 0:
            return float("inf") if self.current_value != 0 else 0.0
        return ((self.current_value - self.previous_value) / self.previous_value) * 100

    def __str__(self) -> str:
        sign = "+" if self.absolute_change >= 0 else ""
        return (
            f"{self.metric_name}: {self.previous_value} → {self.current_value} "
            f"({sign}{self.absolute_change}, {sign}{self.percentage_change:.1f}%)"
        )


@dataclass
class Snapshot:
    """A point-in-time snapshot of metric values from a dashboard."""

    title: str
    metrics: dict[str, float]
    timestamp: str | None = None

    def save(self, path: str | Path) -> None:
        """Save snapshot to a JSON file."""
        p = Path(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(json.dumps(asdict(self), indent=2))

    @classmethod
    def load(cls, path: str | Path) -> Snapshot:
        """Load a snapshot from a JSON file."""
        data = json.loads(Path(path).read_text())
        return cls(**data)


def refresh(dashboard: Any, snapshot_path: str | Path | None = None) -> list[MetricDiff]:
    """Re-run a dashboard and compare metrics to the previous run.

    Executes all metric tiles (tiles with no dimension), captures their
    scalar values, compares to the previous snapshot if available, and
    saves a new snapshot.

    Args:
        dashboard: A Dashboard instance to refresh.
        snapshot_path: Path to store/load snapshots. Defaults to
                       <dashboard.output>.snapshot.json.

    Returns:
        List of MetricDiff instances showing changes (empty on first run).
    """
    from dashboardmd.dashboard import Tile

    # Determine snapshot path
    if snapshot_path is None:
        snapshot_path = Path(str(dashboard.output) + ".snapshot.json")
    snapshot_path = Path(snapshot_path)

    # Load previous snapshot if it exists
    previous: Snapshot | None = None
    if snapshot_path.exists():
        previous = Snapshot.load(snapshot_path)

    # Execute all metric tiles (no dimension = scalar value)
    current_metrics: dict[str, float] = {}
    for section in dashboard.sections:
        for tile in section.tiles:
            if tile.by is None and not tile.sql:
                # This is a metric tile — execute and capture scalar
                for measure_ref in tile.measures:
                    try:
                        result = dashboard.query(measures=[measure_ref])
                        value = result.scalar()
                        if value is not None:
                            current_metrics[measure_ref] = float(value)
                    except Exception:
                        pass

    # Re-render the dashboard
    dashboard.save()

    # Save current snapshot
    from datetime import datetime

    current = Snapshot(
        title=dashboard.title,
        metrics=current_metrics,
        timestamp=datetime.now().isoformat(),
    )
    current.save(snapshot_path)

    # Compute diffs
    diffs: list[MetricDiff] = []
    if previous is not None:
        for metric_name, current_value in current_metrics.items():
            previous_value = previous.metrics.get(metric_name, 0.0)
            diffs.append(
                MetricDiff(
                    metric_name=metric_name,
                    previous_value=previous_value,
                    current_value=current_value,
                )
            )

    return diffs
