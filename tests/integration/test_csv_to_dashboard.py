"""Integration test: CSV files → Entity model → Dashboard → Markdown output.

Tests the full pipeline from loading CSV data through DuckDB to rendering
a complete Markdown dashboard via notebookmd.
"""

from __future__ import annotations

from pathlib import Path

import pytest

pytestmark = pytest.mark.integration


class TestCSVToDashboard:
    """Full pipeline: CSV → Entity → Dashboard → .md file."""

    def test_single_entity_dashboard(
        self, orders_csv: Path, tmp_output_dir: Path
    ) -> None:
        """Build a dashboard from a single CSV source and verify Markdown output."""
        from dashboardmd.dashboard import Dashboard
        from dashboardmd.model import Dimension, Entity, Measure

        orders = Entity(
            name="orders",
            source=orders_csv,
            dimensions=[
                Dimension("id", type="number", primary_key=True),
                Dimension("date", type="time"),
                Dimension("status", type="string"),
            ],
            measures=[
                Measure("revenue", type="sum", sql="amount", format="$,.0f"),
                Measure("count", type="count"),
            ],
        )

        output_path = tmp_output_dir / "single_entity.md"
        dash = Dashboard(
            title="Orders Report",
            entities=[orders],
            output=str(output_path),
        )
        dash.section("Summary")
        dash.tile("orders.revenue")
        dash.tile("orders.count")
        dash.section("By Status")
        dash.tile("orders.revenue", by="orders.status", type="bar_chart")
        dash.save()

        assert output_path.exists()
        content = output_path.read_text()
        assert "Orders Report" in content
        assert "Summary" in content
        assert "By Status" in content

    def test_multi_entity_joined_dashboard(
        self,
        orders_csv: Path,
        customers_csv: Path,
        products_csv: Path,
        tmp_output_dir: Path,
    ) -> None:
        """Build a dashboard joining orders, customers, and products."""
        from dashboardmd.dashboard import Dashboard
        from dashboardmd.model import Dimension, Entity, Measure, Relationship

        orders = Entity(
            name="orders",
            source=orders_csv,
            dimensions=[
                Dimension("id", type="number", primary_key=True),
                Dimension("date", type="time"),
                Dimension("customer_id", type="number"),
                Dimension("product_id", type="number"),
                Dimension("status", type="string"),
            ],
            measures=[
                Measure("revenue", type="sum", sql="amount", format="$,.0f"),
                Measure("count", type="count"),
            ],
        )
        customers = Entity(
            name="customers",
            source=customers_csv,
            dimensions=[
                Dimension("id", type="number", primary_key=True),
                Dimension("name", type="string"),
                Dimension("segment", type="string"),
                Dimension("region", type="string"),
            ],
        )
        products = Entity(
            name="products",
            source=products_csv,
            dimensions=[
                Dimension("id", type="number", primary_key=True),
                Dimension("category", type="string"),
            ],
        )
        rels = [
            Relationship("orders", "customers", on=("customer_id", "id"), type="many_to_one"),
            Relationship("orders", "products", on=("product_id", "id"), type="many_to_one"),
        ]

        output_path = tmp_output_dir / "multi_entity.md"
        dash = Dashboard(
            title="Business Review",
            entities=[orders, customers, products],
            relationships=rels,
            output=str(output_path),
        )
        dash.filter("date_range", dimension="orders.date", default="last_30_days")
        dash.section("Key Metrics")
        dash.tile("orders.revenue", compare="previous_period")
        dash.tile("orders.count")
        dash.section("By Segment")
        dash.tile("orders.revenue", by="customers.segment", type="bar_chart")
        dash.section("By Category")
        dash.tile("orders.revenue", by="products.category", type="bar_chart")
        dash.save()

        assert output_path.exists()
        content = output_path.read_text()
        assert "Business Review" in content

    def test_dashboard_with_filters_applied(
        self, orders_csv: Path, tmp_output_dir: Path
    ) -> None:
        """Filters should restrict the data used in all tiles."""
        from dashboardmd.dashboard import Dashboard
        from dashboardmd.model import Dimension, Entity, Measure

        orders = Entity(
            name="orders",
            source=orders_csv,
            dimensions=[
                Dimension("id", type="number", primary_key=True),
                Dimension("status", type="string"),
            ],
            measures=[Measure("count", type="count")],
        )

        output_path = tmp_output_dir / "filtered.md"
        dash = Dashboard(
            title="Completed Orders",
            entities=[orders],
            output=str(output_path),
        )
        dash.filter("status", dimension="orders.status", default="completed")
        dash.section("Filtered Count")
        dash.tile("orders.count")
        dash.save()

        assert output_path.exists()
        content = output_path.read_text()
        # The count should reflect only completed orders, not all 20
        assert "Completed Orders" in content
