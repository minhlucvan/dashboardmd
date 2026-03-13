"""Tests for database source handlers."""

from __future__ import annotations

from pathlib import Path

import pytest


class TestDuckDBSource:
    """DuckDBSource attaches an existing .duckdb file."""

    def test_register_from_duckdb_file(self, duckdb_conn, tmp_duckdb: Path) -> None:
        """Should attach a .duckdb file and make its tables queryable."""
        from dashboardmd.sources.database import DuckDBSource

        source = DuckDBSource(path=str(tmp_duckdb), table="orders")
        source.register(duckdb_conn, "orders")

        result = duckdb_conn.execute("SELECT count(*) FROM orders").fetchone()
        assert result is not None
        assert result[0] == 20

    def test_query_values_from_duckdb_file(self, duckdb_conn, tmp_duckdb: Path) -> None:
        """Data from attached DuckDB file should have correct values."""
        from dashboardmd.sources.database import DuckDBSource

        source = DuckDBSource(path=str(tmp_duckdb), table="orders")
        source.register(duckdb_conn, "orders")

        row = duckdb_conn.execute("SELECT id, amount FROM orders WHERE id = 4").fetchone()
        assert row is not None
        assert row[0] == 4
        assert row[1] == pytest.approx(150.00)

    def test_missing_duckdb_file_raises_error(self, duckdb_conn) -> None:
        """Attaching a non-existent .duckdb file should raise an error."""
        from dashboardmd.sources.database import DuckDBSource

        source = DuckDBSource(path="/nonexistent/test.duckdb", table="orders")
        with pytest.raises(Exception):
            source.register(duckdb_conn, "orders")

    def test_missing_table_raises_error(self, duckdb_conn, tmp_duckdb: Path) -> None:
        """Referencing a non-existent table should raise an error."""
        from dashboardmd.sources.database import DuckDBSource

        source = DuckDBSource(path=str(tmp_duckdb), table="nonexistent_table")
        with pytest.raises(Exception):
            source.register(duckdb_conn, "data")

    def test_describe_returns_schema(self, duckdb_conn, tmp_duckdb: Path) -> None:
        """describe() should return column metadata from the DuckDB table."""
        from dashboardmd.sources.database import DuckDBSource

        source = DuckDBSource(path=str(tmp_duckdb), table="orders")
        metadata = source.describe()
        assert "columns" in metadata


class TestSQLiteSource:
    """SQLiteSource attaches an existing SQLite database."""

    def test_register_from_sqlite(self, duckdb_conn, tmp_path: Path, orders_csv: Path) -> None:
        """Should attach a SQLite file and make its tables queryable."""
        import sqlite3

        import duckdb as _duckdb

        from dashboardmd.sources.database import SQLiteSource

        # Skip if sqlite extension can't be loaded (e.g., no network)
        try:
            test_conn = _duckdb.connect()
            test_conn.execute("INSTALL sqlite; LOAD sqlite;")
            test_conn.close()
        except _duckdb.IOException:
            pytest.skip("DuckDB sqlite_scanner extension not available")

        # Create a SQLite DB from CSV data
        db_path = tmp_path / "test.sqlite"
        sqlite_conn = sqlite3.connect(str(db_path))
        sqlite_conn.execute(
            "CREATE TABLE orders (id INTEGER, date TEXT, customer_id INTEGER, "
            "product_id INTEGER, status TEXT, amount REAL)"
        )
        sqlite_conn.execute(
            "INSERT INTO orders VALUES (1, '2026-01-05', 101, 201, 'completed', 150.00)"
        )
        sqlite_conn.commit()
        sqlite_conn.close()

        source = SQLiteSource(path=str(db_path), table="orders")
        source.register(duckdb_conn, "orders")

        result = duckdb_conn.execute("SELECT count(*) FROM orders").fetchone()
        assert result is not None
        assert result[0] == 1


class TestPostgresSource:
    """PostgresSource connects to a PostgreSQL database via DuckDB postgres_scanner.

    These tests require an external PostgreSQL instance and are skipped by default.
    Run with: pytest -m integration --postgres-dsn=postgresql://...
    """

    @pytest.mark.integration
    def test_placeholder(self) -> None:
        """Placeholder — real tests require a running PostgreSQL instance."""
        pytest.skip("Requires external PostgreSQL database")


class TestMySQLSource:
    """MySQLSource connects to a MySQL database via DuckDB mysql_scanner.

    These tests require an external MySQL instance and are skipped by default.
    """

    @pytest.mark.integration
    def test_placeholder(self) -> None:
        """Placeholder — real tests require a running MySQL instance."""
        pytest.skip("Requires external MySQL database")
