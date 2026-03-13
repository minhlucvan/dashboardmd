"""Tests for file-based source handlers: CSV, Parquet, JSON."""

from __future__ import annotations

from pathlib import Path

import pytest


class TestCSVSource:
    """CSVSource registers a CSV file as a queryable DuckDB view."""

    def test_register_creates_queryable_view(self, duckdb_conn, orders_csv: Path) -> None:
        """After register(), the table should be queryable via SQL."""
        from dashboardmd.sources.file import CSVSource

        source = CSVSource(path=str(orders_csv))
        source.register(duckdb_conn, "orders")

        result = duckdb_conn.execute("SELECT count(*) FROM orders").fetchone()
        assert result is not None
        assert result[0] == 20  # 20 rows in sample data

    def test_column_values_are_correct(self, duckdb_conn, orders_csv: Path) -> None:
        """Registered CSV data should have correct values."""
        from dashboardmd.sources.file import CSVSource

        source = CSVSource(path=str(orders_csv))
        source.register(duckdb_conn, "orders")

        row = duckdb_conn.execute("SELECT id, status, amount FROM orders WHERE id = 1").fetchone()
        assert row is not None
        assert row[0] == 1
        assert row[1] == "completed"
        assert row[2] == pytest.approx(150.00)

    def test_schema_detection(self, duckdb_conn, orders_csv: Path) -> None:
        """describe() should return column names and inferred types."""
        from dashboardmd.sources.file import CSVSource

        source = CSVSource(path=str(orders_csv))
        metadata = source.describe()

        assert "columns" in metadata
        col_names = [c[0] for c in metadata["columns"]]
        assert "id" in col_names
        assert "date" in col_names
        assert "amount" in col_names

    def test_missing_file_raises_error(self, duckdb_conn) -> None:
        """Registering a non-existent file should raise an error."""
        from dashboardmd.sources.file import CSVSource

        source = CSVSource(path="/nonexistent/path/data.csv")
        with pytest.raises(Exception):
            source.register(duckdb_conn, "missing")

    def test_empty_csv_file(self, duckdb_conn, tmp_path: Path) -> None:
        """An empty CSV (headers only) should register with zero rows."""
        from dashboardmd.sources.file import CSVSource

        empty_csv = tmp_path / "empty.csv"
        empty_csv.write_text("id,name,value\n")

        source = CSVSource(path=str(empty_csv))
        source.register(duckdb_conn, "empty")

        result = duckdb_conn.execute("SELECT count(*) FROM empty").fetchone()
        assert result is not None
        assert result[0] == 0

    def test_aggregation_query(self, duckdb_conn, orders_csv: Path) -> None:
        """Should support GROUP BY and aggregate functions on registered data."""
        from dashboardmd.sources.file import CSVSource

        source = CSVSource(path=str(orders_csv))
        source.register(duckdb_conn, "orders")

        result = duckdb_conn.execute(
            "SELECT status, SUM(amount) as total FROM orders GROUP BY status ORDER BY total DESC"
        ).fetchall()
        assert len(result) == 3  # completed, pending, cancelled
        # completed should have the highest total
        assert result[0][0] == "completed"


class TestParquetSource:
    """ParquetSource registers a Parquet file as a queryable DuckDB view."""

    def test_register_creates_queryable_view(self, duckdb_conn, tmp_parquet: Path) -> None:
        """After register(), parquet data should be queryable."""
        from dashboardmd.sources.file import ParquetSource

        source = ParquetSource(path=str(tmp_parquet))
        source.register(duckdb_conn, "orders")

        result = duckdb_conn.execute("SELECT count(*) FROM orders").fetchone()
        assert result is not None
        assert result[0] == 20

    def test_column_values_match_csv_source(self, duckdb_conn, tmp_parquet: Path) -> None:
        """Parquet data should contain the same values as the original CSV."""
        from dashboardmd.sources.file import ParquetSource

        source = ParquetSource(path=str(tmp_parquet))
        source.register(duckdb_conn, "orders")

        row = duckdb_conn.execute("SELECT id, amount FROM orders WHERE id = 1").fetchone()
        assert row is not None
        assert row[0] == 1
        assert row[1] == pytest.approx(150.00)

    def test_describe_returns_schema(self, duckdb_conn, tmp_parquet: Path) -> None:
        """describe() should return parquet column metadata."""
        from dashboardmd.sources.file import ParquetSource

        source = ParquetSource(path=str(tmp_parquet))
        metadata = source.describe()
        assert "columns" in metadata

    def test_missing_parquet_raises_error(self, duckdb_conn) -> None:
        """Registering a non-existent parquet file should raise an error."""
        from dashboardmd.sources.file import ParquetSource

        source = ParquetSource(path="/nonexistent/data.parquet")
        with pytest.raises(Exception):
            source.register(duckdb_conn, "missing")


class TestJSONSource:
    """JSONSource registers a JSON file as a queryable DuckDB view."""

    def test_register_creates_queryable_view(self, duckdb_conn, tmp_json: Path) -> None:
        """After register(), JSON data should be queryable."""
        from dashboardmd.sources.file import JSONSource

        source = JSONSource(path=str(tmp_json))
        source.register(duckdb_conn, "orders")

        result = duckdb_conn.execute("SELECT count(*) FROM orders").fetchone()
        assert result is not None
        assert result[0] == 20

    def test_column_values_are_correct(self, duckdb_conn, tmp_json: Path) -> None:
        """JSON data should have correct values after registration."""
        from dashboardmd.sources.file import JSONSource

        source = JSONSource(path=str(tmp_json))
        source.register(duckdb_conn, "orders")

        row = duckdb_conn.execute("SELECT id, status FROM orders WHERE id = 5").fetchone()
        assert row is not None
        assert row[0] == 5
        assert row[1] == "cancelled"

    def test_missing_json_raises_error(self, duckdb_conn) -> None:
        """Registering a non-existent JSON file should raise an error."""
        from dashboardmd.sources.file import JSONSource

        source = JSONSource(path="/nonexistent/data.json")
        with pytest.raises(Exception):
            source.register(duckdb_conn, "missing")
