"""Analyst: direct SQL analytics interface for AI agents.

The Analyst class gives AI agents maximum analytical power by providing
direct SQL access to DuckDB over any registered data source. No semantic
layer required — just register sources and write SQL.

Usage:
    from dashboardmd import Analyst, Source

    analyst = Analyst()
    analyst.add("orders", Source.csv("data/orders.csv"))
    analyst.add("customers", Source.csv("data/customers.csv"))

    # Direct SQL — full DuckDB power
    result = analyst.sql("SELECT segment, SUM(amount) FROM orders o JOIN customers c ON o.customer_id = c.id GROUP BY 1")

    # Inspect what's available
    analyst.tables()          # → ['orders', 'customers']
    analyst.schema("orders")  # → [('id', 'BIGINT'), ('amount', 'DOUBLE'), ...]

    # Get results in different formats
    analyst.sql("...").df()          # → pandas DataFrame
    analyst.sql("...").fetchall()    # → list of tuples
    analyst.sql("...").show()        # → print to stdout

    # Save results to Markdown
    analyst.to_md("output/report.md", title="Revenue Analysis", queries=[
        ("Key Metrics", "SELECT COUNT(*) as orders, SUM(amount) as revenue FROM orders"),
        ("By Segment", "SELECT segment, SUM(amount) as revenue FROM orders o JOIN customers c ON o.customer_id = c.id GROUP BY 1 ORDER BY 2 DESC"),
    ])
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import duckdb

from dashboardmd.sources.base import SourceHandler


class QueryResult:
    """Wraps a DuckDB query result with convenience methods."""

    def __init__(self, relation: duckdb.DuckDBPyRelation, sql: str) -> None:
        self._relation = relation
        self.sql = sql
        self._rows: list[tuple[Any, ...]] | None = None
        self._columns: list[str] | None = None

    @property
    def columns(self) -> list[str]:
        """Column names from the result."""
        if self._columns is None:
            self._columns = [desc[0] for desc in self._relation.description]
        return self._columns

    def fetchall(self) -> list[tuple[Any, ...]]:
        """Fetch all rows as a list of tuples."""
        if self._rows is None:
            self._rows = self._relation.fetchall()
        return self._rows

    def fetchone(self) -> tuple[Any, ...] | None:
        """Fetch the first row."""
        rows = self.fetchall()
        return rows[0] if rows else None

    def df(self) -> Any:
        """Return results as a pandas DataFrame."""
        return self._relation.fetchdf()

    def show(self, limit: int = 50) -> None:
        """Print results to stdout."""
        self._relation.show(max_rows=limit)

    def to_markdown_table(self) -> str:
        """Render results as a Markdown table."""
        rows = self.fetchall()
        cols = self.columns

        if not cols:
            return ""

        # Header
        lines = [
            "| " + " | ".join(cols) + " |",
            "| " + " | ".join("---" for _ in cols) + " |",
        ]
        # Rows
        for row in rows:
            cells = [_format_cell(v) for v in row]
            lines.append("| " + " | ".join(cells) + " |")

        return "\n".join(lines)

    @property
    def row_count(self) -> int:
        """Number of rows in the result."""
        return len(self.fetchall())

    def scalar(self) -> Any:
        """Return the single value from a single-row, single-column result."""
        row = self.fetchone()
        if row is None:
            return None
        return row[0]

    def __repr__(self) -> str:
        return f"QueryResult(columns={self.columns}, rows={self.row_count})"


class Analyst:
    """Direct SQL analytics interface for AI agents.

    Registers data sources into an in-memory DuckDB instance and provides
    raw SQL access with full DuckDB analytical power.
    """

    def __init__(self, db_path: str | None = None) -> None:
        """Create an Analyst with a DuckDB connection.

        Args:
            db_path: Optional path to persist the DuckDB database.
                     None (default) uses an in-memory database.
        """
        self.conn: duckdb.DuckDBPyConnection = duckdb.connect(db_path or ":memory:")
        self._sources: dict[str, SourceHandler | str] = {}

    def add(self, name: str, source: SourceHandler | str | Any) -> Analyst:
        """Register a data source as a named table.

        Args:
            name: Table name to use in SQL queries.
            source: A SourceHandler, file path string, or pandas DataFrame.

        Returns:
            self, for chaining.
        """
        if isinstance(source, SourceHandler):
            source.register(self.conn, name)
            self._sources[name] = source
        elif isinstance(source, str):
            self._register_path(name, source)
            self._sources[name] = source
        elif hasattr(source, "dtypes"):
            # pandas/polars DataFrame
            self.conn.register(name, source)
            self._sources[name] = f"<DataFrame:{name}>"
        else:
            raise TypeError(f"Unsupported source type: {type(source)}")
        return self

    def add_csv(self, name: str, path: str) -> Analyst:
        """Register a CSV file."""
        self.conn.execute(f"CREATE OR REPLACE VIEW \"{name}\" AS SELECT * FROM read_csv_auto('{path}')")
        self._sources[name] = path
        return self

    def add_parquet(self, name: str, path: str) -> Analyst:
        """Register a Parquet file."""
        self.conn.execute(f"CREATE OR REPLACE VIEW \"{name}\" AS SELECT * FROM read_parquet('{path}')")
        self._sources[name] = path
        return self

    def add_json(self, name: str, path: str) -> Analyst:
        """Register a JSON file."""
        self.conn.execute(f"CREATE OR REPLACE VIEW \"{name}\" AS SELECT * FROM read_json_auto('{path}')")
        self._sources[name] = path
        return self

    def add_dataframe(self, name: str, df: Any) -> Analyst:
        """Register a pandas/polars DataFrame."""
        self.conn.register(name, df)
        self._sources[name] = f"<DataFrame:{name}>"
        return self

    def _register_path(self, name: str, path: str) -> None:
        """Auto-detect file type from extension and register."""
        if path.endswith(".parquet"):
            self.conn.execute(f"CREATE OR REPLACE VIEW \"{name}\" AS SELECT * FROM read_parquet('{path}')")
        elif path.endswith(".json"):
            self.conn.execute(f"CREATE OR REPLACE VIEW \"{name}\" AS SELECT * FROM read_json_auto('{path}')")
        else:
            self.conn.execute(f"CREATE OR REPLACE VIEW \"{name}\" AS SELECT * FROM read_csv_auto('{path}')")

    def sql(self, query: str) -> QueryResult:
        """Execute SQL and return a QueryResult.

        Full DuckDB SQL is available — aggregations, window functions,
        CTEs, JOINs, UNNEST, PIVOT, regex, date math, etc.

        Args:
            query: Any valid DuckDB SQL query.

        Returns:
            QueryResult with .fetchall(), .df(), .to_markdown_table(), etc.
        """
        result = self.conn.execute(query)
        return QueryResult(result, sql=query)

    def query(self, query: str) -> QueryResult:
        """Alias for sql()."""
        return self.sql(query)

    def tables(self) -> list[str]:
        """List all registered tables/views."""
        result = self.conn.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
        ).fetchall()
        return [row[0] for row in result]

    def schema(self, table_name: str) -> list[tuple[str, str]]:
        """Get column names and types for a table.

        Returns:
            List of (column_name, column_type) tuples.
        """
        result = self.conn.execute(f"DESCRIBE \"{table_name}\"").fetchall()
        return [(row[0], row[1]) for row in result]

    def sample(self, table_name: str, n: int = 5) -> QueryResult:
        """Get a sample of rows from a table."""
        return self.sql(f"SELECT * FROM \"{table_name}\" LIMIT {n}")

    def count(self, table_name: str) -> int:
        """Count rows in a table."""
        result = self.conn.execute(f"SELECT count(*) FROM \"{table_name}\"").fetchone()
        return result[0] if result else 0

    def describe_table(self, table_name: str) -> QueryResult:
        """Get summary statistics for all columns in a table."""
        return self.sql(f"SUMMARIZE SELECT * FROM \"{table_name}\"")

    def to_md(
        self,
        output: str,
        title: str = "Analysis",
        queries: list[tuple[str, str]] | None = None,
    ) -> str:
        """Render SQL results to a Markdown file.

        Args:
            output: Path to write the .md file.
            title: Dashboard/report title.
            queries: List of (section_title, sql) tuples to execute and render.

        Returns:
            The generated Markdown string.
        """
        lines: list[str] = [f"# {title}", ""]

        for section_title, sql in queries or []:
            lines.append(f"## {section_title}")
            lines.append("")

            try:
                result = self.sql(sql)
                lines.append(result.to_markdown_table())
            except Exception as e:
                lines.append(f"> Error: {e}")

            lines.append("")

        md_content = "\n".join(lines)

        # Write to file
        path = Path(output)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(md_content)

        return md_content

    def close(self) -> None:
        """Close the DuckDB connection."""
        self.conn.close()

    def __enter__(self) -> Analyst:
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()

    def __repr__(self) -> str:
        tables = self.tables()
        return f"Analyst(tables={tables})"


def _format_cell(value: Any) -> str:
    """Format a cell value for Markdown display."""
    if value is None:
        return ""
    if isinstance(value, float):
        # Avoid overly precise floats
        if value == int(value):
            return str(int(value))
        return f"{value:,.2f}"
    return str(value)
