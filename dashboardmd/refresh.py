"""Re-run + metric diff tracking."""

from __future__ import annotations

from dataclasses import dataclass
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


def refresh(dashboard: Any) -> list[MetricDiff]:
    """Re-run a dashboard and compare metrics to the previous run.

    Args:
        dashboard: A Dashboard instance to refresh.

    Returns:
        List of MetricDiff instances showing changes.
    """
    raise NotImplementedError("refresh() requires full Dashboard tracking implementation")
