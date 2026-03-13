"""Tests for the Render Layer: Dashboard, tiles, filters, sections, save()."""

from __future__ import annotations

from pathlib import Path

import pytest


# ---------------------------------------------------------------------------
# Dashboard Construction
# ---------------------------------------------------------------------------


class TestDashboardConstruction:
    """Dashboard is the top-level container for tiles, filters, and sections."""

    def test_create_dashboard(self) -> None:
        """Create a dashboard with a title and output path."""
        from dashboardmd.dashboard import Dashboard

        dash = Dashboard(title="Test Dashboard", output="output/test.md")
        assert dash.title == "Test Dashboard"
        assert dash.output == "output/test.md"

    def test_dashboard_with_entities(self) -> None:
        """Dashboard accepts a list of entities."""
        from dashboardmd.dashboard import Dashboard
        from dashboardmd.model import Dimension, Entity, Measure

        orders = Entity(
            name="orders",
            dimensions=[Dimension("id", type="number", primary_key=True)],
            measures=[Measure("count", type="count")],
        )
        dash = Dashboard(
            title="Test",
            entities=[orders],
            output="output/test.md",
        )
        assert len(dash.entities) == 1

    def test_dashboard_with_relationships(self) -> None:
        """Dashboard accepts a list of relationships."""
        from dashboardmd.dashboard import Dashboard
        from dashboardmd.model import Relationship

        rels = [
            Relationship("orders", "customers", on=("customer_id", "id"), type="many_to_one"),
        ]
        dash = Dashboard(
            title="Test",
            relationships=rels,
            output="output/test.md",
        )
        assert len(dash.relationships) == 1


# ---------------------------------------------------------------------------
# Sections
# ---------------------------------------------------------------------------


class TestDashboardSections:
    """Sections organize tiles into labeled groups."""

    def test_add_section(self) -> None:
        """Add a named section to the dashboard."""
        from dashboardmd.dashboard import Dashboard

        dash = Dashboard(title="Test", output="output/test.md")
        dash.section("Key Metrics")

        assert len(dash.sections) == 1
        assert dash.sections[0].title == "Key Metrics"

    def test_multiple_sections(self) -> None:
        """Multiple sections maintain order."""
        from dashboardmd.dashboard import Dashboard

        dash = Dashboard(title="Test", output="output/test.md")
        dash.section("Section A")
        dash.section("Section B")
        dash.section("Section C")

        assert len(dash.sections) == 3
        assert dash.sections[1].title == "Section B"


# ---------------------------------------------------------------------------
# Tiles
# ---------------------------------------------------------------------------


class TestDashboardTiles:
    """Tiles are individual visualizations bound to queries."""

    def test_add_metric_tile(self) -> None:
        """Add a single-value metric tile."""
        from dashboardmd.dashboard import Dashboard

        dash = Dashboard(title="Test", output="output/test.md")
        dash.section("Metrics")
        dash.tile("orders.revenue")

        section = dash.sections[0]
        assert len(section.tiles) == 1

    def test_add_tile_with_dimension(self) -> None:
        """Add a tile grouped by a dimension."""
        from dashboardmd.dashboard import Dashboard

        dash = Dashboard(title="Test", output="output/test.md")
        dash.section("Breakdown")
        dash.tile("orders.revenue", by="customers.segment")

        tile = dash.sections[0].tiles[0]
        assert tile.by == "customers.segment" or "customers.segment" in str(tile)

    def test_add_tile_with_explicit_type(self) -> None:
        """Tile can have an explicit visualization type."""
        from dashboardmd.dashboard import Dashboard

        dash = Dashboard(title="Test", output="output/test.md")
        dash.section("Charts")
        dash.tile("orders.revenue", by="customers.segment", type="bar_chart")

        tile = dash.sections[0].tiles[0]
        assert tile.type == "bar_chart" or tile.viz_type == "bar_chart"

    def test_add_tile_with_compare(self) -> None:
        """Tile can request period comparison."""
        from dashboardmd.dashboard import Dashboard

        dash = Dashboard(title="Test", output="output/test.md")
        dash.section("Metrics")
        dash.tile("orders.revenue", compare="previous_period")

        tile = dash.sections[0].tiles[0]
        assert "previous_period" in str(tile.compare) or tile.compare == "previous_period"

    def test_add_tile_with_top_n(self) -> None:
        """Tile can limit to top N results."""
        from dashboardmd.dashboard import Dashboard

        dash = Dashboard(title="Test", output="output/test.md")
        dash.section("Top Items")
        dash.tile("orders.revenue", by="products.category", top=10)

    def test_add_multiple_measures_tile(self) -> None:
        """Tile can display multiple measures (renders as table)."""
        from dashboardmd.dashboard import Dashboard

        dash = Dashboard(title="Test", output="output/test.md")
        dash.section("Comparison")
        dash.tile(["orders.revenue", "orders.count"], by="customers.segment", type="table")

    def test_tile_with_granularity(self) -> None:
        """Tile can specify time granularity."""
        from dashboardmd.dashboard import Dashboard

        dash = Dashboard(title="Test", output="output/test.md")
        dash.section("Trends")
        dash.tile("orders.revenue", by="orders.date", granularity="weekly")


# ---------------------------------------------------------------------------
# Filters
# ---------------------------------------------------------------------------


class TestDashboardFilters:
    """Global filters applied across all tiles."""

    def test_add_date_filter(self) -> None:
        """Add a date range filter."""
        from dashboardmd.dashboard import Dashboard

        dash = Dashboard(title="Test", output="output/test.md")
        dash.filter("date_range", dimension="orders.date", default="last_30_days")

        assert len(dash.filters) == 1

    def test_add_categorical_filter(self) -> None:
        """Add a categorical filter (e.g., region)."""
        from dashboardmd.dashboard import Dashboard

        dash = Dashboard(title="Test", output="output/test.md")
        dash.filter("region", dimension="customers.region")

        assert len(dash.filters) == 1

    def test_multiple_filters(self) -> None:
        """Dashboard can have multiple global filters."""
        from dashboardmd.dashboard import Dashboard

        dash = Dashboard(title="Test", output="output/test.md")
        dash.filter("date_range", dimension="orders.date", default="last_30_days")
        dash.filter("region", dimension="customers.region")
        dash.filter("status", dimension="orders.status")

        assert len(dash.filters) == 3


# ---------------------------------------------------------------------------
# Smart Viz Selection
# ---------------------------------------------------------------------------


class TestSmartVizSelection:
    """Auto-pick visualization type based on query shape."""

    def test_single_measure_no_dimension_is_metric(self) -> None:
        """Single measure with no dimension → metric card."""
        from dashboardmd.dashboard import infer_viz_type

        viz = infer_viz_type(measures=["revenue"], dimensions=[], has_time=False)
        assert viz == "metric"

    def test_measure_with_time_dimension_is_line(self) -> None:
        """Measure + time dimension → line chart."""
        from dashboardmd.dashboard import infer_viz_type

        viz = infer_viz_type(measures=["revenue"], dimensions=["date"], has_time=True)
        assert viz == "line_chart"

    def test_measure_with_categorical_dimension_is_bar(self) -> None:
        """Measure + categorical dimension → bar chart."""
        from dashboardmd.dashboard import infer_viz_type

        viz = infer_viz_type(measures=["revenue"], dimensions=["segment"], has_time=False)
        assert viz == "bar_chart"

    def test_multiple_measures_multiple_dimensions_is_table(self) -> None:
        """Multiple measures + multiple dimensions → table."""
        from dashboardmd.dashboard import infer_viz_type

        viz = infer_viz_type(
            measures=["revenue", "count"],
            dimensions=["segment", "region"],
            has_time=False,
        )
        assert viz == "table"

    def test_metric_with_compare_is_metric_delta(self) -> None:
        """Single measure with compare → metric card with delta."""
        from dashboardmd.dashboard import infer_viz_type

        viz = infer_viz_type(
            measures=["revenue"],
            dimensions=[],
            has_time=False,
            compare="previous_period",
        )
        assert viz in ("metric", "metric_delta")


# ---------------------------------------------------------------------------
# Save / Render
# ---------------------------------------------------------------------------


class TestDashboardSave:
    """Dashboard.save() renders to Markdown via notebookmd."""

    def test_save_creates_output_file(
        self, tmp_output_dir: Path, orders_csv: Path
    ) -> None:
        """save() should create a .md file at the output path."""
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
        output_path = tmp_output_dir / "test.md"
        dash = Dashboard(
            title="Test Dashboard",
            entities=[orders],
            output=str(output_path),
        )
        dash.section("Overview")
        dash.tile("orders.count")
        dash.save()

        assert output_path.exists()

    def test_save_includes_title(
        self, tmp_output_dir: Path, orders_csv: Path
    ) -> None:
        """The rendered Markdown should include the dashboard title."""
        from dashboardmd.dashboard import Dashboard
        from dashboardmd.model import Dimension, Entity, Measure

        orders = Entity(
            name="orders",
            source=orders_csv,
            dimensions=[Dimension("id", type="number", primary_key=True)],
            measures=[Measure("count", type="count")],
        )
        output_path = tmp_output_dir / "test.md"
        dash = Dashboard(
            title="Weekly Business Review",
            entities=[orders],
            output=str(output_path),
        )
        dash.section("Overview")
        dash.tile("orders.count")
        dash.save()

        content = output_path.read_text()
        assert "Weekly Business Review" in content

    def test_save_includes_section_headings(
        self, tmp_output_dir: Path, orders_csv: Path
    ) -> None:
        """Section titles should appear as headings in the Markdown."""
        from dashboardmd.dashboard import Dashboard
        from dashboardmd.model import Dimension, Entity, Measure

        orders = Entity(
            name="orders",
            source=orders_csv,
            dimensions=[
                Dimension("id", type="number", primary_key=True),
                Dimension("status", type="string"),
            ],
            measures=[
                Measure("count", type="count"),
                Measure("revenue", type="sum", sql="amount"),
            ],
        )
        output_path = tmp_output_dir / "test.md"
        dash = Dashboard(
            title="Test",
            entities=[orders],
            output=str(output_path),
        )
        dash.section("Key Metrics")
        dash.tile("orders.count")
        dash.section("Revenue Analysis")
        dash.tile("orders.revenue", by="orders.status")
        dash.save()

        content = output_path.read_text()
        assert "Key Metrics" in content
        assert "Revenue Analysis" in content

    def test_save_renders_metric_value(
        self, tmp_output_dir: Path, orders_csv: Path
    ) -> None:
        """A metric tile should render the computed value in the output."""
        from dashboardmd.dashboard import Dashboard
        from dashboardmd.model import Dimension, Entity, Measure

        orders = Entity(
            name="orders",
            source=orders_csv,
            dimensions=[Dimension("id", type="number", primary_key=True)],
            measures=[Measure("count", type="count")],
        )
        output_path = tmp_output_dir / "test.md"
        dash = Dashboard(
            title="Test",
            entities=[orders],
            output=str(output_path),
        )
        dash.section("Overview")
        dash.tile("orders.count")
        dash.save()

        content = output_path.read_text()
        # The count should be 20 (from our sample data)
        assert "20" in content

    def test_save_with_raw_sql_tile(
        self, tmp_output_dir: Path, orders_csv: Path
    ) -> None:
        """Raw SQL tiles bypass the semantic layer and render directly."""
        from dashboardmd.dashboard import Dashboard
        from dashboardmd.model import Dimension, Entity, Measure

        orders = Entity(
            name="orders",
            source=orders_csv,
            dimensions=[Dimension("id", type="number", primary_key=True)],
            measures=[Measure("count", type="count")],
        )
        output_path = tmp_output_dir / "raw_sql.md"
        dash = Dashboard(
            title="Raw SQL Test",
            entities=[orders],
            output=str(output_path),
        )
        dash.section("Custom Query")
        dash.tile_sql("Status Counts", "SELECT status, COUNT(*) AS cnt FROM orders GROUP BY 1")
        dash.save()

        content = output_path.read_text()
        assert "Custom Query" in content
        # Should contain status values from the CSV
        assert "cnt" in content or "status" in content


# ---------------------------------------------------------------------------
# Analyst Integration
# ---------------------------------------------------------------------------


class TestDashboardAnalystIntegration:
    """Dashboard delegates all query execution to the underlying Analyst."""

    def test_dashboard_exposes_analyst(self) -> None:
        """Dashboard provides access to its underlying Analyst."""
        from dashboardmd.dashboard import Dashboard

        dash = Dashboard(title="Test", output="output/test.md")
        assert dash.analyst is not None

    def test_dashboard_with_custom_analyst(self, orders_csv: Path) -> None:
        """Dashboard can use a pre-configured Analyst."""
        from dashboardmd.analyst import Analyst
        from dashboardmd.dashboard import Dashboard

        analyst = Analyst()
        analyst.add("orders", str(orders_csv))

        dash = Dashboard(title="Test", output="output/test.md", analyst=analyst)
        assert dash.analyst is analyst
        assert "orders" in dash.analyst.tables()

    def test_dashboard_query_delegates_to_analyst(self, orders_csv: Path) -> None:
        """Dashboard.query() delegates to the underlying Analyst.query()."""
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
        dash = Dashboard(title="Test", entities=[orders], output="output/test.md")
        result = dash.query(measures=["orders.count"], dimensions=["orders.status"])
        assert result.row_count > 0

    def test_dashboard_execute_sql(self, orders_csv: Path) -> None:
        """Dashboard.execute_sql() delegates to Analyst.sql()."""
        from dashboardmd.dashboard import Dashboard
        from dashboardmd.model import Dimension, Entity, Measure

        orders = Entity(
            name="orders",
            source=orders_csv,
            dimensions=[Dimension("id", type="number", primary_key=True)],
            measures=[Measure("count", type="count")],
        )
        dash = Dashboard(title="Test", entities=[orders], output="output/test.md")
        result = dash.execute_sql("SELECT COUNT(*) FROM orders")
        assert result.scalar() == 20
