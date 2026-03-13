"""Tests for RawSQLSource handler."""

from __future__ import annotations

from pathlib import Path

import pytest


class TestRawSQLSource:
    """RawSQLSource registers an arbitrary SQL expression as a view."""

    def test_simple_sql_query(self, duckdb_conn, orders_csv: Path) -> None:
        """A simple SELECT * SQL should register all data."""
        from dashboardmd.sources.sql import RawSQLSource

        source = RawSQLSource(sql=f"SELECT * FROM read_csv_auto('{orders_csv}')")
        source.register(duckdb_conn, "orders")

        result = duckdb_conn.execute("SELECT count(*) FROM orders").fetchone()
        assert result is not None
        assert result[0] == 20

    def test_sql_with_where_clause(self, duckdb_conn, orders_csv: Path) -> None:
        """SQL with a WHERE clause should filter data."""
        from dashboardmd.sources.sql import RawSQLSource

        source = RawSQLSource(
            sql=f"SELECT * FROM read_csv_auto('{orders_csv}') WHERE status = 'completed'"
        )
        source.register(duckdb_conn, "completed_orders")

        result = duckdb_conn.execute("SELECT count(*) FROM completed_orders").fetchone()
        assert result is not None
        assert result[0] < 20  # Fewer than all orders
        # Verify all rows are completed
        statuses = duckdb_conn.execute("SELECT DISTINCT status FROM completed_orders").fetchall()
        assert len(statuses) == 1
        assert statuses[0][0] == "completed"

    def test_sql_with_aggregation(self, duckdb_conn, orders_csv: Path) -> None:
        """SQL with GROUP BY should register aggregated data."""
        from dashboardmd.sources.sql import RawSQLSource

        source = RawSQLSource(
            sql=f"SELECT status, SUM(amount) as total FROM read_csv_auto('{orders_csv}') GROUP BY status"
        )
        source.register(duckdb_conn, "order_summary")

        result = duckdb_conn.execute("SELECT count(*) FROM order_summary").fetchone()
        assert result is not None
        assert result[0] == 3  # 3 status values

    def test_invalid_sql_raises_error(self, duckdb_conn) -> None:
        """Invalid SQL should raise an error during registration."""
        from dashboardmd.sources.sql import RawSQLSource

        source = RawSQLSource(sql="THIS IS NOT VALID SQL")
        with pytest.raises(Exception):
            source.register(duckdb_conn, "bad_query")

    def test_describe_returns_schema(self, duckdb_conn, orders_csv: Path) -> None:
        """describe() should return column info inferred from the SQL result."""
        from dashboardmd.sources.sql import RawSQLSource

        source = RawSQLSource(sql=f"SELECT id, amount FROM read_csv_auto('{orders_csv}')")
        metadata = source.describe()
        assert "columns" in metadata
        col_names = [c[0] for c in metadata["columns"]]
        assert "id" in col_names
        assert "amount" in col_names
