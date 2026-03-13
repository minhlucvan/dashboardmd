"""Database source handlers: PostgreSQL, MySQL, SQLite, DuckDB."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from dashboardmd.sources.base import SourceHandler

if TYPE_CHECKING:
    import duckdb


@dataclass
class DuckDBSource(SourceHandler):
    """Attach an existing .duckdb file and expose a table."""

    path: str
    table: str

    def register(self, conn: duckdb.DuckDBPyConnection, table_name: str) -> None:
        alias = f"_attached_{table_name}"
        conn.execute(f"ATTACH '{self.path}' AS \"{alias}\" (READ_ONLY)")
        conn.execute(f"CREATE OR REPLACE VIEW \"{table_name}\" AS SELECT * FROM \"{alias}\".{self.table}")

    def describe(self) -> dict[str, Any]:
        import duckdb

        conn = duckdb.connect(self.path, read_only=True)
        try:
            cols = conn.execute(f"DESCRIBE {self.table}").fetchall()
            return {"columns": [(c[0], c[1]) for c in cols]}
        finally:
            conn.close()


@dataclass
class SQLiteSource(SourceHandler):
    """Attach a SQLite database and expose a table."""

    path: str
    table: str

    def register(self, conn: duckdb.DuckDBPyConnection, table_name: str) -> None:
        conn.execute("INSTALL sqlite; LOAD sqlite;")
        alias = f"_sqlite_{table_name}"
        conn.execute(f"ATTACH '{self.path}' AS \"{alias}\" (TYPE sqlite, READ_ONLY)")
        conn.execute(f"CREATE OR REPLACE VIEW \"{table_name}\" AS SELECT * FROM \"{alias}\".{self.table}")

    def describe(self) -> dict[str, Any]:
        import sqlite3

        db = sqlite3.connect(self.path)
        try:
            cursor = db.execute(f"PRAGMA table_info({self.table})")
            cols = [(row[1], row[2]) for row in cursor.fetchall()]
            return {"columns": cols}
        finally:
            db.close()


@dataclass
class PostgresSource(SourceHandler):
    """Connect to PostgreSQL via DuckDB's postgres_scanner extension."""

    dsn: str
    table: str
    schema: str = "public"

    def register(self, conn: duckdb.DuckDBPyConnection, table_name: str) -> None:
        conn.execute("INSTALL postgres; LOAD postgres;")
        alias = f"_pg_{table_name}"
        conn.execute(f"ATTACH '{self.dsn}' AS \"{alias}\" (TYPE postgres, READ_ONLY)")
        conn.execute(
            f"CREATE OR REPLACE VIEW \"{table_name}\" AS SELECT * FROM \"{alias}\".{self.schema}.{self.table}"
        )

    def describe(self) -> dict[str, Any]:
        return {"columns": []}  # Requires live connection


@dataclass
class MySQLSource(SourceHandler):
    """Connect to MySQL via DuckDB's mysql_scanner extension."""

    dsn: str
    table: str

    def register(self, conn: duckdb.DuckDBPyConnection, table_name: str) -> None:
        conn.execute("INSTALL mysql; LOAD mysql;")
        alias = f"_mysql_{table_name}"
        conn.execute(f"ATTACH '{self.dsn}' AS \"{alias}\" (TYPE mysql, READ_ONLY)")
        conn.execute(f"CREATE OR REPLACE VIEW \"{table_name}\" AS SELECT * FROM \"{alias}\".{self.table}")

    def describe(self) -> dict[str, Any]:
        return {"columns": []}  # Requires live connection
