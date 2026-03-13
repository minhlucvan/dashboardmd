"""Base class for all data source handlers."""

from __future__ import annotations

import json
import os
import tempfile
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

    def _register_rows(
        self, conn: duckdb.DuckDBPyConnection, table_name: str, rows: list[dict[str, Any]]
    ) -> None:
        """Helper: load a list of dicts into DuckDB as a table.

        Connector authors can use this to avoid temp-file boilerplate::

            class MySource(SourceHandler):
                def register(self, conn, table_name):
                    rows = self._fetch_data()
                    self._register_rows(conn, table_name, rows)
        """
        if not rows:
            conn.execute(f'CREATE OR REPLACE TABLE "{table_name}" (placeholder INT)')
            conn.execute(f'DELETE FROM "{table_name}"')
            return

        tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False)
        try:
            json.dump(rows, tmp, default=str)
            tmp.close()
            conn.execute(
                f'CREATE OR REPLACE TABLE "{table_name}" AS '
                f"SELECT * FROM read_json_auto('{tmp.name}')"
            )
        finally:
            os.unlink(tmp.name)
