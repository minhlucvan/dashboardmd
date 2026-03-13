"""Analyst: the core query engine for dashboardmd.

Analyst is the foundation of dashboardmd. Everything else — Entity,
Dashboard, semantic queries — is built on top of it. It wraps DuckDB
and provides both raw SQL and semantic query capabilities.

Two ways to use it:

1. Direct SQL (maximum agent power):
    analyst = Analyst()
    analyst.add("orders", "data/orders.csv")
    result = analyst.sql("SELECT status, SUM(amount) FROM orders GROUP BY 1")

2. Semantic queries (BI-style):
    analyst = Analyst()
    analyst.add_entity(orders_entity)
    result = analyst.query(measures=["orders.revenue"], dimensions=["orders.status"])
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
    """The core query engine for dashboardmd.

    Registers data sources into DuckDB and provides both raw SQL and
    semantic query access. All other components (Dashboard, Engine)
    delegate to Analyst for query execution.
    """

    def __init__(self, db_path: str | None = None) -> None:
        """Create an Analyst with a DuckDB connection.

        Args:
            db_path: Optional path to persist the DuckDB database.
                     None (default) uses an in-memory database.
        """
        self.conn: duckdb.DuckDBPyConnection = duckdb.connect(db_path or ":memory:")
        self._sources: dict[str, SourceHandler | str] = {}
        self._entities: dict[str, Any] = {}  # name → Entity
        self._relationships: list[Any] = []
        self._query_builder: Any = None  # Lazy-initialized QueryBuilder

    # ------------------------------------------------------------------
    # Source registration
    # ------------------------------------------------------------------

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

    # ------------------------------------------------------------------
    # Entity registration (semantic layer)
    # ------------------------------------------------------------------

    def add_entity(self, entity: Any) -> Analyst:
        """Register an Entity with its source and semantic metadata.

        The Entity's source is registered in DuckDB, and its dimensions/
        measures become available for semantic queries via query().

        Args:
            entity: An Entity instance with source, dimensions, and measures.

        Returns:
            self, for chaining.
        """
        source = entity.source
        if source is not None:
            if isinstance(source, SourceHandler):
                source.register(self.conn, entity.name)
            elif isinstance(source, (str, Path)):
                self._register_path(entity.name, str(source))
            elif hasattr(source, "dtypes"):
                self.conn.register(entity.name, source)

        self._entities[entity.name] = entity
        self._sources[entity.name] = source
        self._query_builder = None  # Reset cached builder
        return self

    def add_entities(self, entities: list[Any]) -> Analyst:
        """Register multiple entities."""
        for entity in entities:
            self.add_entity(entity)
        return self

    def set_relationships(self, relationships: list[Any]) -> Analyst:
        """Set entity relationships for automatic join resolution.

        Args:
            relationships: List of Relationship instances.

        Returns:
            self, for chaining.
        """
        self._relationships = relationships
        self._query_builder = None  # Reset cached builder
        return self

    @property
    def entities(self) -> dict[str, Any]:
        """Registered entities."""
        return self._entities

    @property
    def relationships(self) -> list[Any]:
        """Registered relationships."""
        return self._relationships

    # ------------------------------------------------------------------
    # SQL execution (direct — full DuckDB power)
    # ------------------------------------------------------------------

    def sql(self, query_str: str) -> QueryResult:
        """Execute SQL and return a QueryResult.

        Full DuckDB SQL is available — aggregations, window functions,
        CTEs, JOINs, UNNEST, PIVOT, regex, date math, etc.

        Args:
            query_str: Any valid DuckDB SQL query.

        Returns:
            QueryResult with .fetchall(), .df(), .to_markdown_table(), etc.
        """
        result = self.conn.execute(query_str)
        return QueryResult(result, sql=query_str)

    # ------------------------------------------------------------------
    # Semantic query (BI-style — measures + dimensions → SQL → results)
    # ------------------------------------------------------------------

    def query(
        self,
        measures: list[str] | str | None = None,
        dimensions: list[str] | str | None = None,
        filters: list[tuple[str, str, str]] | None = None,
        time_granularity: str | None = None,
        sort: tuple[str, str] | None = None,
        limit: int | None = None,
        compare: str | None = None,
    ) -> QueryResult:
        """Execute a semantic query using measures and dimensions.

        This is the BI-style interface — specify what you want to measure
        and how to break it down, and the Analyst resolves joins, generates
        SQL, and executes against DuckDB.

        Args:
            measures: ["orders.revenue", "orders.count"] or a single string
            dimensions: ["customers.segment"] or a single string
            filters: [("orders.status", "equals", "completed")]
            time_granularity: "day" | "week" | "month" | "quarter" | "year"
            sort: ("orders.revenue", "desc")
            limit: Maximum number of rows
            compare: "previous_period" | "previous_year"

        Returns:
            QueryResult with .fetchall(), .df(), .to_markdown_table(), etc.
        """
        from dashboardmd.query import Query

        # Normalize string args to lists
        if isinstance(measures, str):
            measures = [measures]
        if isinstance(dimensions, str):
            dimensions = [dimensions]

        q = Query(
            measures=measures or [],
            dimensions=dimensions,
            filters=filters or [],
            time_granularity=time_granularity,
            sort=sort,
            limit=limit,
            compare=compare,
        )

        builder = self._get_query_builder()
        sql = builder.build_sql(q)
        return self.sql(sql)

    def _get_query_builder(self) -> Any:
        """Get or create the QueryBuilder (cached, reset on entity changes)."""
        if self._query_builder is None:
            from dashboardmd.query import QueryBuilder

            self._query_builder = QueryBuilder(
                entities=list(self._entities.values()),
                relationships=self._relationships,
            )
        return self._query_builder

    # ------------------------------------------------------------------
    # Schema inspection (agent exploration)
    # ------------------------------------------------------------------

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

    # ------------------------------------------------------------------
    # Markdown output
    # ------------------------------------------------------------------

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

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

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
