"""Raw SQL source handler."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from dashboardmd.sources.base import SourceHandler

if TYPE_CHECKING:
    import duckdb


@dataclass
class RawSQLSource(SourceHandler):
    """Register an arbitrary SQL expression as a DuckDB view."""

    sql: str

    def register(self, conn: duckdb.DuckDBPyConnection, table_name: str) -> None:
        conn.execute(f"CREATE OR REPLACE VIEW \"{table_name}\" AS {self.sql}")

    def describe(self) -> dict[str, Any]:
        import duckdb

        conn = duckdb.connect()
        try:
            cols = conn.execute(f"DESCRIBE ({self.sql})").fetchall()
            return {"columns": [(c[0], c[1]) for c in cols]}
        finally:
            conn.close()
