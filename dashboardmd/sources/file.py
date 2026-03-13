"""File-based source handlers: CSV, Parquet, JSON."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from dashboardmd.sources.base import SourceHandler

if TYPE_CHECKING:
    import duckdb


@dataclass
class CSVSource(SourceHandler):
    """Register a CSV file as a queryable DuckDB view."""

    path: str

    def register(self, conn: duckdb.DuckDBPyConnection, table_name: str) -> None:
        conn.execute(f"CREATE OR REPLACE VIEW \"{table_name}\" AS SELECT * FROM read_csv_auto('{self.path}')")

    def describe(self) -> dict[str, Any]:
        import duckdb

        conn = duckdb.connect()
        try:
            cols = conn.execute(f"DESCRIBE SELECT * FROM read_csv_auto('{self.path}')").fetchall()
            return {"columns": [(c[0], c[1]) for c in cols]}
        finally:
            conn.close()


@dataclass
class ParquetSource(SourceHandler):
    """Register a Parquet file as a queryable DuckDB view."""

    path: str

    def register(self, conn: duckdb.DuckDBPyConnection, table_name: str) -> None:
        conn.execute(f"CREATE OR REPLACE VIEW \"{table_name}\" AS SELECT * FROM read_parquet('{self.path}')")

    def describe(self) -> dict[str, Any]:
        import duckdb

        conn = duckdb.connect()
        try:
            cols = conn.execute(f"DESCRIBE SELECT * FROM read_parquet('{self.path}')").fetchall()
            return {"columns": [(c[0], c[1]) for c in cols]}
        finally:
            conn.close()


@dataclass
class JSONSource(SourceHandler):
    """Register a JSON file as a queryable DuckDB view."""

    path: str

    def register(self, conn: duckdb.DuckDBPyConnection, table_name: str) -> None:
        conn.execute(f"CREATE OR REPLACE VIEW \"{table_name}\" AS SELECT * FROM read_json_auto('{self.path}')")

    def describe(self) -> dict[str, Any]:
        import duckdb

        conn = duckdb.connect()
        try:
            cols = conn.execute(f"DESCRIBE SELECT * FROM read_json_auto('{self.path}')").fetchall()
            return {"columns": [(c[0], c[1]) for c in cols]}
        finally:
            conn.close()
