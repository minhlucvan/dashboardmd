"""Core data model: Entity, Dimension, Measure, Relationship."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

_VALID_DIMENSION_TYPES = {"string", "number", "time", "boolean"}
_VALID_MEASURE_TYPES = {"sum", "count", "count_distinct", "avg", "min", "max", "number"}
_VALID_RELATIONSHIP_TYPES = {"one_to_one", "one_to_many", "many_to_one", "many_to_many"}


@dataclass
class Dimension:
    """An attribute you group and filter by."""

    name: str
    type: str = "string"
    sql: str | None = None
    primary_key: bool = False
    format: str | None = None

    def __post_init__(self) -> None:
        if self.type not in _VALID_DIMENSION_TYPES:
            raise ValueError(
                f"Invalid dimension type '{self.type}'. Must be one of: {', '.join(sorted(_VALID_DIMENSION_TYPES))}"
            )


@dataclass
class Measure:
    """An aggregation you compute (SUM, COUNT, AVG, etc.)."""

    name: str
    type: str = "count"
    sql: str | None = None
    format: str | None = None
    filters: list[tuple[str, str, str]] = field(default_factory=list)

    def __post_init__(self) -> None:
        if self.type not in _VALID_MEASURE_TYPES:
            raise ValueError(
                f"Invalid measure type '{self.type}'. Must be one of: {', '.join(sorted(_VALID_MEASURE_TYPES))}"
            )


@dataclass
class Relationship:
    """How entities join together."""

    from_entity: str
    to_entity: str
    on: tuple[str, str]
    type: str = "many_to_one"

    def __post_init__(self) -> None:
        if self.type not in _VALID_RELATIONSHIP_TYPES:
            raise ValueError(
                f"Invalid relationship type '{self.type}'. "
                f"Must be one of: {', '.join(sorted(_VALID_RELATIONSHIP_TYPES))}"
            )


@dataclass
class Entity:
    """A logical table with semantic meaning — dimensions, measures, and a source."""

    name: str
    source: Any = None
    dimensions: list[Dimension] = field(default_factory=list)
    measures: list[Measure] = field(default_factory=list)

    def get_dimension(self, name: str) -> Dimension | None:
        """Look up a dimension by name."""
        for d in self.dimensions:
            if d.name == name:
                return d
        return None

    def get_measure(self, name: str) -> Measure | None:
        """Look up a measure by name."""
        for m in self.measures:
            if m.name == name:
                return m
        return None

    @property
    def primary_key(self) -> Dimension | None:
        """Return the primary key dimension, if any."""
        for d in self.dimensions:
            if d.primary_key:
                return d
        return None
