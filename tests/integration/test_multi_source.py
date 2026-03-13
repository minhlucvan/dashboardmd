"""Integration test: Multiple data source types in a single dashboard.

Tests that the engine can register and join data from different source
types (CSV, Parquet, DataFrame, DuckDB file) in a single DuckDB connection.
"""

from __future__ import annotations

from pathlib import Path

import pytest

pytestmark = pytest.mark.integration


class TestMultiSourceRegistration:
    """Engine registers multiple source types and enables cross-source queries."""

    def test_csv_and_parquet_together(
        self, duckdb_conn, orders_csv: Path, tmp_parquet: Path
    ) -> None:
        """Register both CSV and Parquet sources, query across them."""
        from dashboardmd.sources.file import CSVSource, ParquetSource

        CSVSource(path=str(orders_csv)).register(duckdb_conn, "orders_csv")
        ParquetSource(path=str(tmp_parquet)).register(duckdb_conn, "orders_parquet")

        # Both should have the same data
        csv_count = duckdb_conn.execute("SELECT count(*) FROM orders_csv").fetchone()[0]
        parquet_count = duckdb_conn.execute("SELECT count(*) FROM orders_parquet").fetchone()[0]
        assert csv_count == parquet_count == 20

    def test_csv_and_duckdb_file_together(
        self, duckdb_conn, customers_csv: Path, tmp_duckdb: Path
    ) -> None:
        """Register CSV + DuckDB file sources and join them."""
        from dashboardmd.sources.database import DuckDBSource
        from dashboardmd.sources.file import CSVSource

        CSVSource(path=str(customers_csv)).register(duckdb_conn, "customers")
        DuckDBSource(path=str(tmp_duckdb), table="orders").register(duckdb_conn, "orders")

        # Cross-source join
        result = duckdb_conn.execute(
            "SELECT c.name, SUM(o.amount) "
            "FROM orders o JOIN customers c ON o.customer_id = c.id "
            "GROUP BY c.name ORDER BY SUM(o.amount) DESC"
        ).fetchall()
        assert len(result) > 0

    @pytest.mark.requires_pandas
    def test_csv_and_dataframe_together(
        self, duckdb_conn, customers_csv: Path, sample_dataframe
    ) -> None:
        """Register CSV + pandas DataFrame and join them."""
        from dashboardmd.sources.dataframe import DataFrameSource
        from dashboardmd.sources.file import CSVSource

        CSVSource(path=str(customers_csv)).register(duckdb_conn, "customers")
        DataFrameSource(df=sample_dataframe).register(duckdb_conn, "orders")

        result = duckdb_conn.execute(
            "SELECT c.segment, COUNT(*) "
            "FROM orders o JOIN customers c ON o.customer_id = c.id "
            "GROUP BY c.segment"
        ).fetchall()
        assert len(result) > 0

    def test_three_source_types(
        self, duckdb_conn, orders_csv: Path, customers_csv: Path, tmp_parquet: Path
    ) -> None:
        """Register three different source types and query across all of them."""
        from dashboardmd.sources.file import CSVSource, ParquetSource

        CSVSource(path=str(orders_csv)).register(duckdb_conn, "orders")
        CSVSource(path=str(customers_csv)).register(duckdb_conn, "customers")
        # Use parquet as a second copy of orders (simulating a different source)
        ParquetSource(path=str(tmp_parquet)).register(duckdb_conn, "orders_archive")

        # Query across all registered tables
        result = duckdb_conn.execute(
            "SELECT 'current' as src, count(*) FROM orders "
            "UNION ALL "
            "SELECT 'archive', count(*) FROM orders_archive"
        ).fetchall()
        assert len(result) == 2


class TestMultiSourceDashboard:
    """Full dashboard from multiple source types."""

    def test_dashboard_from_csv_and_parquet(
        self,
        orders_csv: Path,
        customers_csv: Path,
        tmp_output_dir: Path,
    ) -> None:
        """Build a dashboard with entities backed by different source types."""
        from dashboardmd.dashboard import Dashboard
        from dashboardmd.model import Dimension, Entity, Measure, Relationship

        orders = Entity(
            name="orders",
            source=orders_csv,
            dimensions=[
                Dimension("id", type="number", primary_key=True),
                Dimension("customer_id", type="number"),
                Dimension("status", type="string"),
            ],
            measures=[
                Measure("revenue", type="sum", sql="amount"),
                Measure("count", type="count"),
            ],
        )
        customers = Entity(
            name="customers",
            source=customers_csv,
            dimensions=[
                Dimension("id", type="number", primary_key=True),
                Dimension("segment", type="string"),
            ],
        )
        rels = [
            Relationship("orders", "customers", on=("customer_id", "id"), type="many_to_one"),
        ]

        output_path = tmp_output_dir / "multi_source.md"
        dash = Dashboard(
            title="Multi-Source Report",
            entities=[orders, customers],
            relationships=rels,
            output=str(output_path),
        )
        dash.section("Overview")
        dash.tile("orders.revenue")
        dash.tile("orders.revenue", by="customers.segment")
        dash.save()

        assert output_path.exists()
