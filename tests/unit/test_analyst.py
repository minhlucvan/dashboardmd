"""Tests for Analyst: direct SQL analytics interface for AI agents."""

from __future__ import annotations

from pathlib import Path

import pytest


# ---------------------------------------------------------------------------
# Source Registration
# ---------------------------------------------------------------------------


class TestAnalystRegistration:
    """Analyst registers data sources for SQL access."""

    def test_add_csv_by_path(self, orders_csv: Path) -> None:
        """Register a CSV file by path string."""
        from dashboardmd import Analyst

        analyst = Analyst()
        analyst.add("orders", str(orders_csv))
        assert "orders" in analyst.tables()
        analyst.close()

    def test_add_csv_by_source(self, orders_csv: Path) -> None:
        """Register a CSV file via Source.csv()."""
        from dashboardmd import Analyst, Source

        analyst = Analyst()
        analyst.add("orders", Source.csv(str(orders_csv)))
        assert "orders" in analyst.tables()
        analyst.close()

    def test_add_parquet(self, tmp_parquet: Path) -> None:
        """Register a Parquet file."""
        from dashboardmd import Analyst

        analyst = Analyst()
        analyst.add("orders", str(tmp_parquet))
        assert "orders" in analyst.tables()
        analyst.close()

    @pytest.mark.requires_pandas
    def test_add_dataframe(self, sample_dataframe) -> None:
        """Register a pandas DataFrame."""
        from dashboardmd import Analyst

        analyst = Analyst()
        analyst.add("orders", sample_dataframe)
        assert "orders" in analyst.tables()
        analyst.close()

    def test_add_multiple_sources(self, orders_csv: Path, customers_csv: Path) -> None:
        """Register multiple sources."""
        from dashboardmd import Analyst

        analyst = Analyst()
        analyst.add("orders", str(orders_csv))
        analyst.add("customers", str(customers_csv))
        tables = analyst.tables()
        assert "orders" in tables
        assert "customers" in tables
        analyst.close()

    def test_chaining(self, orders_csv: Path, customers_csv: Path) -> None:
        """add() returns self for chaining."""
        from dashboardmd import Analyst

        analyst = Analyst()
        result = analyst.add("orders", str(orders_csv)).add("customers", str(customers_csv))
        assert result is analyst
        assert len(analyst.tables()) == 2
        analyst.close()

    def test_convenience_add_csv(self, orders_csv: Path) -> None:
        """add_csv() registers a CSV file."""
        from dashboardmd import Analyst

        analyst = Analyst()
        analyst.add_csv("orders", str(orders_csv))
        assert "orders" in analyst.tables()
        analyst.close()

    def test_convenience_add_parquet(self, tmp_parquet: Path) -> None:
        """add_parquet() registers a Parquet file."""
        from dashboardmd import Analyst

        analyst = Analyst()
        analyst.add_parquet("orders", str(tmp_parquet))
        assert "orders" in analyst.tables()
        analyst.close()


# ---------------------------------------------------------------------------
# SQL Execution
# ---------------------------------------------------------------------------


class TestAnalystSQL:
    """Analyst.sql() executes arbitrary SQL against registered data."""

    def test_simple_select(self, orders_csv: Path) -> None:
        """Execute a simple SELECT query."""
        from dashboardmd import Analyst

        with Analyst() as a:
            a.add("orders", str(orders_csv))
            result = a.sql("SELECT * FROM orders")
            assert result.row_count == 20

    def test_aggregation(self, orders_csv: Path) -> None:
        """Execute aggregation queries."""
        from dashboardmd import Analyst

        with Analyst() as a:
            a.add("orders", str(orders_csv))
            result = a.sql("SELECT SUM(amount) as total FROM orders")
            total = result.scalar()
            assert total > 0

    def test_group_by(self, orders_csv: Path) -> None:
        """Execute GROUP BY queries."""
        from dashboardmd import Analyst

        with Analyst() as a:
            a.add("orders", str(orders_csv))
            result = a.sql(
                "SELECT status, COUNT(*) as cnt, SUM(amount) as total "
                "FROM orders GROUP BY status ORDER BY total DESC"
            )
            rows = result.fetchall()
            assert len(rows) == 3  # completed, pending, cancelled

    def test_cross_source_join(self, orders_csv: Path, customers_csv: Path) -> None:
        """JOIN across two different registered sources."""
        from dashboardmd import Analyst

        with Analyst() as a:
            a.add("orders", str(orders_csv))
            a.add("customers", str(customers_csv))
            result = a.sql(
                "SELECT c.segment, SUM(o.amount) as revenue "
                "FROM orders o JOIN customers c ON o.customer_id = c.id "
                "GROUP BY c.segment ORDER BY revenue DESC"
            )
            rows = result.fetchall()
            assert len(rows) == 3  # enterprise, smb, mid_market
            # All segments should have positive revenue
            for row in rows:
                assert row[1] > 0

    def test_three_way_join(
        self, orders_csv: Path, customers_csv: Path, products_csv: Path
    ) -> None:
        """JOIN three data sources."""
        from dashboardmd import Analyst

        with Analyst() as a:
            a.add("orders", str(orders_csv))
            a.add("customers", str(customers_csv))
            a.add("products", str(products_csv))
            result = a.sql(
                "SELECT p.category, c.segment, SUM(o.amount) as revenue "
                "FROM orders o "
                "JOIN customers c ON o.customer_id = c.id "
                "JOIN products p ON o.product_id = p.id "
                "GROUP BY p.category, c.segment "
                "ORDER BY revenue DESC"
            )
            assert result.row_count > 0

    def test_window_functions(self, orders_csv: Path) -> None:
        """DuckDB window functions should work."""
        from dashboardmd import Analyst

        with Analyst() as a:
            a.add("orders", str(orders_csv))
            result = a.sql(
                "SELECT id, amount, "
                "SUM(amount) OVER (ORDER BY date) as running_total "
                "FROM orders ORDER BY date"
            )
            rows = result.fetchall()
            assert len(rows) == 20
            # Running total should increase
            assert rows[-1][2] >= rows[0][2]

    def test_cte(self, orders_csv: Path) -> None:
        """Common Table Expressions should work."""
        from dashboardmd import Analyst

        with Analyst() as a:
            a.add("orders", str(orders_csv))
            result = a.sql(
                "WITH monthly AS ("
                "  SELECT DATE_TRUNC('month', date::DATE) as month, SUM(amount) as total "
                "  FROM orders GROUP BY 1"
                ") SELECT * FROM monthly ORDER BY month"
            )
            assert result.row_count >= 2

    def test_subquery(self, orders_csv: Path) -> None:
        """Subqueries should work."""
        from dashboardmd import Analyst

        with Analyst() as a:
            a.add("orders", str(orders_csv))
            result = a.sql(
                "SELECT * FROM orders WHERE amount > "
                "(SELECT AVG(amount) FROM orders)"
            )
            assert result.row_count > 0
            assert result.row_count < 20

    def test_invalid_sql_raises_error(self, orders_csv: Path) -> None:
        """Invalid SQL should raise a clear error."""
        from dashboardmd import Analyst

        with Analyst() as a:
            a.add("orders", str(orders_csv))
            with pytest.raises(Exception):
                a.sql("SELECT nonexistent_column FROM orders")


# ---------------------------------------------------------------------------
# Result Formats
# ---------------------------------------------------------------------------


class TestQueryResult:
    """QueryResult provides multiple ways to consume results."""

    def test_fetchall(self, orders_csv: Path) -> None:
        """fetchall() returns list of tuples."""
        from dashboardmd import Analyst

        with Analyst() as a:
            a.add("orders", str(orders_csv))
            result = a.sql("SELECT id, amount FROM orders LIMIT 3")
            rows = result.fetchall()
            assert isinstance(rows, list)
            assert len(rows) == 3
            assert isinstance(rows[0], tuple)

    def test_fetchone(self, orders_csv: Path) -> None:
        """fetchone() returns the first row."""
        from dashboardmd import Analyst

        with Analyst() as a:
            a.add("orders", str(orders_csv))
            result = a.sql("SELECT COUNT(*) FROM orders")
            row = result.fetchone()
            assert row is not None
            assert row[0] == 20

    def test_scalar(self, orders_csv: Path) -> None:
        """scalar() returns a single value."""
        from dashboardmd import Analyst

        with Analyst() as a:
            a.add("orders", str(orders_csv))
            count = a.sql("SELECT COUNT(*) FROM orders").scalar()
            assert count == 20

    def test_columns(self, orders_csv: Path) -> None:
        """columns property returns column names."""
        from dashboardmd import Analyst

        with Analyst() as a:
            a.add("orders", str(orders_csv))
            result = a.sql("SELECT id, status, amount FROM orders LIMIT 1")
            assert result.columns == ["id", "status", "amount"]

    @pytest.mark.requires_pandas
    def test_df(self, orders_csv: Path) -> None:
        """df() returns a pandas DataFrame."""
        pd = pytest.importorskip("pandas")
        from dashboardmd import Analyst

        with Analyst() as a:
            a.add("orders", str(orders_csv))
            df = a.sql("SELECT * FROM orders").df()
            assert isinstance(df, pd.DataFrame)
            assert len(df) == 20

    def test_to_markdown_table(self, orders_csv: Path) -> None:
        """to_markdown_table() renders a Markdown table string."""
        from dashboardmd import Analyst

        with Analyst() as a:
            a.add("orders", str(orders_csv))
            result = a.sql("SELECT status, COUNT(*) as cnt FROM orders GROUP BY status ORDER BY cnt DESC")
            md = result.to_markdown_table()
            assert "| status | cnt |" in md
            assert "| --- | --- |" in md
            assert "completed" in md

    def test_row_count(self, orders_csv: Path) -> None:
        """row_count property returns the number of rows."""
        from dashboardmd import Analyst

        with Analyst() as a:
            a.add("orders", str(orders_csv))
            result = a.sql("SELECT * FROM orders WHERE status = 'completed'")
            assert result.row_count > 0
            assert result.row_count < 20

    def test_repr(self, orders_csv: Path) -> None:
        """QueryResult has a readable repr."""
        from dashboardmd import Analyst

        with Analyst() as a:
            a.add("orders", str(orders_csv))
            result = a.sql("SELECT id, amount FROM orders LIMIT 5")
            r = repr(result)
            assert "QueryResult" in r
            assert "rows=5" in r


# ---------------------------------------------------------------------------
# Schema Inspection
# ---------------------------------------------------------------------------


class TestAnalystInspection:
    """Analyst provides schema inspection for AI agents to explore data."""

    def test_tables(self, orders_csv: Path, customers_csv: Path) -> None:
        """tables() lists all registered tables."""
        from dashboardmd import Analyst

        with Analyst() as a:
            a.add("orders", str(orders_csv))
            a.add("customers", str(customers_csv))
            tables = a.tables()
            assert "orders" in tables
            assert "customers" in tables

    def test_schema(self, orders_csv: Path) -> None:
        """schema() returns column names and types."""
        from dashboardmd import Analyst

        with Analyst() as a:
            a.add("orders", str(orders_csv))
            cols = a.schema("orders")
            col_names = [c[0] for c in cols]
            assert "id" in col_names
            assert "amount" in col_names
            assert "status" in col_names

    def test_sample(self, orders_csv: Path) -> None:
        """sample() returns first N rows."""
        from dashboardmd import Analyst

        with Analyst() as a:
            a.add("orders", str(orders_csv))
            result = a.sample("orders", n=3)
            assert result.row_count == 3

    def test_count(self, orders_csv: Path) -> None:
        """count() returns row count for a table."""
        from dashboardmd import Analyst

        with Analyst() as a:
            a.add("orders", str(orders_csv))
            assert a.count("orders") == 20

    def test_describe_table(self, orders_csv: Path) -> None:
        """describe_table() returns summary statistics."""
        from dashboardmd import Analyst

        with Analyst() as a:
            a.add("orders", str(orders_csv))
            result = a.describe_table("orders")
            assert result.row_count > 0


# ---------------------------------------------------------------------------
# Markdown Output
# ---------------------------------------------------------------------------


class TestAnalystMarkdown:
    """Analyst.to_md() renders SQL results to Markdown files."""

    def test_to_md_creates_file(
        self, orders_csv: Path, tmp_output_dir: Path
    ) -> None:
        """to_md() should create a Markdown file."""
        from dashboardmd import Analyst

        output_path = tmp_output_dir / "report.md"
        with Analyst() as a:
            a.add("orders", str(orders_csv))
            a.to_md(
                str(output_path),
                title="Revenue Report",
                queries=[
                    ("Total Revenue", "SELECT SUM(amount) as total FROM orders"),
                ],
            )
        assert output_path.exists()

    def test_to_md_content(
        self, orders_csv: Path, customers_csv: Path, tmp_output_dir: Path
    ) -> None:
        """to_md() should include title, sections, and data."""
        from dashboardmd import Analyst

        output_path = tmp_output_dir / "report.md"
        with Analyst() as a:
            a.add("orders", str(orders_csv))
            a.add("customers", str(customers_csv))
            content = a.to_md(
                str(output_path),
                title="Business Review",
                queries=[
                    ("Summary", "SELECT COUNT(*) as orders, SUM(amount) as revenue FROM orders"),
                    (
                        "By Segment",
                        "SELECT c.segment, SUM(o.amount) as revenue "
                        "FROM orders o JOIN customers c ON o.customer_id = c.id "
                        "GROUP BY 1 ORDER BY 2 DESC",
                    ),
                ],
            )
        assert "# Business Review" in content
        assert "## Summary" in content
        assert "## By Segment" in content
        assert "enterprise" in content

    def test_to_md_multiple_queries(
        self, orders_csv: Path, tmp_output_dir: Path
    ) -> None:
        """to_md() handles multiple query sections."""
        from dashboardmd import Analyst

        output_path = tmp_output_dir / "multi.md"
        with Analyst() as a:
            a.add("orders", str(orders_csv))
            a.to_md(
                str(output_path),
                title="Multi-Section Report",
                queries=[
                    ("Total", "SELECT SUM(amount) as total FROM orders"),
                    ("By Status", "SELECT status, COUNT(*) FROM orders GROUP BY 1"),
                    ("Top 5", "SELECT * FROM orders ORDER BY amount DESC LIMIT 5"),
                ],
            )
        content = output_path.read_text()
        assert content.count("## ") == 3

    def test_to_md_handles_query_error(
        self, orders_csv: Path, tmp_output_dir: Path
    ) -> None:
        """to_md() should handle SQL errors gracefully in individual sections."""
        from dashboardmd import Analyst

        output_path = tmp_output_dir / "error.md"
        with Analyst() as a:
            a.add("orders", str(orders_csv))
            content = a.to_md(
                str(output_path),
                title="Error Test",
                queries=[
                    ("Good Query", "SELECT COUNT(*) FROM orders"),
                    ("Bad Query", "SELECT nonexistent FROM orders"),
                ],
            )
        assert "## Good Query" in content
        assert "## Bad Query" in content
        assert "Error" in content


# ---------------------------------------------------------------------------
# Context Manager
# ---------------------------------------------------------------------------


class TestAnalystContextManager:
    """Analyst supports with-statement usage."""

    def test_context_manager(self, orders_csv: Path) -> None:
        """Analyst works as a context manager."""
        from dashboardmd import Analyst

        with Analyst() as a:
            a.add("orders", str(orders_csv))
            count = a.sql("SELECT COUNT(*) FROM orders").scalar()
            assert count == 20

    def test_repr(self, orders_csv: Path) -> None:
        """Analyst has a readable repr."""
        from dashboardmd import Analyst

        with Analyst() as a:
            a.add("orders", str(orders_csv))
            r = repr(a)
            assert "Analyst" in r
            assert "orders" in r
