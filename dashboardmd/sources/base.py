"""Base class for all data source handlers."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import duckdb


class SourceHandler(ABC):
    """Base class for all data source handlers.

    Each handler knows how to register its data as a DuckDB table/view.
    Inspired by MindsDB's handler pattern but much simpler — we just
    need to get data into DuckDB, not build a full middleware.
    """

    @abstractmethod
    def register(self, conn: duckdb.DuckDBPyConnection, table_name: str) -> None:
        """Register this source as a table/view in the DuckDB connection."""
        ...

    @abstractmethod
    def describe(self) -> dict[str, Any]:
        """Return schema metadata: column names, types, row count estimate."""
        ...
