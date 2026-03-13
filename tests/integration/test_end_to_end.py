"""End-to-end integration tests: complete user scenarios.

These tests mirror the examples from README.md and the design proposal,
verifying the full user workflow works as documented.
"""

from __future__ import annotations

from pathlib import Path

import pytest

pytestmark = pytest.mark.integration


class TestReadmeQuickStart:
    """Test the exact Quick Start example from README.md."""

    def test_quick_start_example(
        self,
        orders_csv: Path,
        customers_csv: Path,
        tmp_output_dir: Path,
    ) -> None:
        """The README Quick Start code should produce a valid dashboard."""
        from dashboardmd import Dashboard, Dimension, Entity, Measure, Relationship

        orders = Entity(
            "orders",
            source=orders_csv,
            dimensions=[
                Dimension("id", type="number", primary_key=True),
                Dimension("date", type="time"),
                Dimension("customer_id", type="number"),
                Dimension("status", type="string"),
            ],
            measures=[
                Measure("revenue", type="sum", sql="amount", format="$,.0f"),
                Measure("count", type="count"),
            ],
        )

        customers = Entity(
            "customers",
            source=customers_csv,
            dimensions=[
                Dimension("id", type="number", primary_key=True),
                Dimension("name", type="string"),
                Dimension("segment", type="string"),
                Dimension("region", type="string"),
            ],
        )

        rels = [
            Relationship("orders", "customers", on=("customer_id", "id"), type="many_to_one"),
        ]

        output_path = tmp_output_dir / "weekly.md"
        dash = Dashboard(
            title="Weekly Business Review",
            entities=[orders, customers],
            relationships=rels,
            output=str(output_path),
        )

        dash.filter("date_range", dimension="orders.date", default="last_30_days")

        dash.section("Key Metrics")
        dash.tile("orders.revenue", compare="previous_period")
        dash.tile("orders.count", compare="previous_period")

        dash.section("Revenue by Segment")
        dash.tile("orders.revenue", by="customers.segment", type="bar_chart")

        dash.save()

        assert output_path.exists()
        content = output_path.read_text()
        assert "Weekly Business Review" in content
        assert "Key Metrics" in content
        assert "Revenue by Segment" in content


class TestAgentWorkflow:
    """Test the agent workflow: discover → build → save."""

    def test_discover_and_auto_dashboard(
        self, samples_dir: Path, tmp_output_dir: Path
    ) -> None:
        """Agent discovers data sources and auto-generates a dashboard."""
        from dashboardmd.dashboard import Dashboard
        from dashboardmd.suggest import auto_join, discover

        entities = discover(str(samples_dir))
        assert len(entities) >= 3

        relationships = auto_join(entities)

        output_path = tmp_output_dir / "auto.md"
        dash = Dashboard(
            title="Auto-Generated Dashboard",
            entities=entities,
            relationships=relationships,
            output=str(output_path),
        )
        dash.auto_dashboard()
        dash.save()

        assert output_path.exists()

    def test_programmatic_query(
        self, orders_csv: Path, customers_csv: Path
    ) -> None:
        """Agent uses the query API to get results as a DataFrame."""
        from dashboardmd.dashboard import Dashboard
        from dashboardmd.model import Dimension, Entity, Measure, Relationship

        orders = Entity(
            "orders",
            source=orders_csv,
            dimensions=[
                Dimension("id", type="number", primary_key=True),
                Dimension("customer_id", type="number"),
                Dimension("status", type="string"),
            ],
            measures=[Measure("revenue", type="sum", sql="amount")],
        )
        customers = Entity(
            "customers",
            source=customers_csv,
            dimensions=[
                Dimension("id", type="number", primary_key=True),
                Dimension("segment", type="string"),
            ],
        )
        rels = [
            Relationship("orders", "customers", on=("customer_id", "id"), type="many_to_one"),
        ]

        dash = Dashboard(
            title="Query Test",
            entities=[orders, customers],
            relationships=rels,
            output="/dev/null",
        )

        result = dash.query(
            measures=["orders.revenue"],
            dimensions=["customers.segment"],
            filters=[("orders.status", "equals", "completed")],
        )
        assert result is not None
        # Result should have rows grouped by segment


class TestSourceFactory:
    """Test the Source.xxx() factory methods for creating source handlers."""

    def test_source_csv_factory(self, orders_csv: Path) -> None:
        """Source.csv() should create a CSVSource."""
        from dashboardmd.sources import Source

        source = Source.csv(str(orders_csv))
        assert source is not None

    def test_source_parquet_factory(self, tmp_parquet: Path) -> None:
        """Source.parquet() should create a ParquetSource."""
        from dashboardmd.sources import Source

        source = Source.parquet(str(tmp_parquet))
        assert source is not None

    def test_source_duckdb_factory(self, tmp_duckdb: Path) -> None:
        """Source.duckdb() should create a DuckDBSource."""
        from dashboardmd.sources import Source

        source = Source.duckdb(str(tmp_duckdb), table="orders")
        assert source is not None

    @pytest.mark.requires_pandas
    def test_source_dataframe_factory(self, sample_dataframe) -> None:
        """Source.dataframe() should create a DataFrameSource."""
        from dashboardmd.sources import Source

        source = Source.dataframe(sample_dataframe)
        assert source is not None

    def test_source_sql_factory(self) -> None:
        """Source.sql() should create a RawSQLSource."""
        from dashboardmd.sources import Source

        source = Source.sql("SELECT 1 as id, 'test' as name")
        assert source is not None
