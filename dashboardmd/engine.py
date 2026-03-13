"""Execution engine: run queries against data sources via DuckDB."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import duckdb

from dashboardmd.model import Entity
from dashboardmd.sources.base import SourceHandler


class Engine:
    """DuckDB-based execution engine.

    Registers source handlers as DuckDB views and executes queries.
    """

    def __init__(
        self,
        entities: list[Entity] | None = None,
        relationships: list[Any] | None = None,
        db_path: str | None = None,
    ) -> None:
        self.conn: duckdb.DuckDBPyConnection = duckdb.connect(db_path or ":memory:")
        self.entities: dict[str, Entity] = {}
        self.relationships = relationships or []

        for entity in entities or []:
            self._register_entity(entity)

    def _register_entity(self, entity: Entity) -> None:
        """Register an entity's source in DuckDB."""
        source = entity.source
        if source is None:
            return

        if isinstance(source, SourceHandler):
            source.register(self.conn, entity.name)
        elif isinstance(source, (str, Path)):
            # Convenience: treat string paths as CSV
            path = str(source)
            if path.endswith(".parquet"):
                self.conn.execute(f"CREATE OR REPLACE VIEW \"{entity.name}\" AS SELECT * FROM read_parquet('{path}')")
            elif path.endswith(".json"):
                self.conn.execute(
                    f"CREATE OR REPLACE VIEW \"{entity.name}\" AS SELECT * FROM read_json_auto('{path}')"
                )
            else:
                self.conn.execute(
                    f"CREATE OR REPLACE VIEW \"{entity.name}\" AS SELECT * FROM read_csv_auto('{path}')"
                )
        self.entities[entity.name] = entity

    def execute(self, query: Any) -> duckdb.DuckDBPyRelation:
        """Execute a Query object and return results."""
        # For now, delegate to sql() with built SQL
        from dashboardmd.query import QueryBuilder

        builder = QueryBuilder(
            entities=list(self.entities.values()),
            relationships=self.relationships,
        )
        sql = builder.build_sql(query)
        return self.conn.execute(sql)

    def sql(self, query: str) -> duckdb.DuckDBPyRelation:
        """Execute raw SQL directly against DuckDB."""
        return self.conn.execute(query)

    def register_source(self, name: str, source: SourceHandler) -> None:
        """Register a standalone source (no Entity wrapper)."""
        source.register(self.conn, name)

    def register_csv(self, name: str, path: str) -> None:
        """Convenience: register a CSV file."""
        self.conn.execute(f"CREATE OR REPLACE VIEW \"{name}\" AS SELECT * FROM read_csv_auto('{path}')")

    def register_parquet(self, name: str, path: str) -> None:
        """Convenience: register a Parquet file."""
        self.conn.execute(f"CREATE OR REPLACE VIEW \"{name}\" AS SELECT * FROM read_parquet('{path}')")

    def tables(self) -> list[str]:
        """List all registered tables/views."""
        result = self.conn.execute("SELECT table_name FROM information_schema.tables").fetchall()
        return [row[0] for row in result]

    def schema(self, table_name: str) -> list[tuple[str, str]]:
        """Get column names and types for a table."""
        result = self.conn.execute(f"DESCRIBE \"{table_name}\"").fetchall()
        return [(row[0], row[1]) for row in result]

    def close(self) -> None:
        """Close the DuckDB connection."""
        self.conn.close()
