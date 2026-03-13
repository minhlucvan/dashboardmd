"""Execution engine: backward-compatible wrapper around Analyst.

Engine exists for backward compatibility. Internally it delegates
everything to Analyst, which is the real query engine.

Prefer using Analyst directly for new code.
"""

from __future__ import annotations

from typing import Any

from dashboardmd.analyst import Analyst, QueryResult
from dashboardmd.model import Entity


class Engine:
    """DuckDB-based execution engine.

    Thin wrapper around Analyst that accepts entities and relationships
    at construction time. For new code, use Analyst directly.
    """

    def __init__(
        self,
        entities: list[Entity] | None = None,
        relationships: list[Any] | None = None,
        db_path: str | None = None,
    ) -> None:
        self._analyst = Analyst(db_path=db_path)

        if relationships:
            self._analyst.set_relationships(relationships)

        for entity in entities or []:
            self._analyst.add_entity(entity)

    @property
    def conn(self) -> Any:
        """Underlying DuckDB connection."""
        return self._analyst.conn

    def execute(self, query: Any) -> Any:
        """Execute a Query object and return results."""
        from dashboardmd.query import QueryBuilder

        builder = QueryBuilder(
            entities=list(self._analyst.entities.values()),
            relationships=self._analyst.relationships,
        )
        sql = builder.build_sql(query)
        return self._analyst.conn.execute(sql)

    def sql(self, query: str) -> QueryResult:
        """Execute raw SQL via Analyst."""
        return self._analyst.sql(query)

    def tables(self) -> list[str]:
        """List all registered tables/views."""
        return self._analyst.tables()

    def schema(self, table_name: str) -> list[tuple[str, str]]:
        """Get column names and types for a table."""
        return self._analyst.schema(table_name)

    def close(self) -> None:
        """Close the DuckDB connection."""
        self._analyst.close()
