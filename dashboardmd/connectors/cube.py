"""Cube.js connector: import/export Cube data models.

Cube.js schema maps to dashboardmd as follows:
  - Cube → Entity
  - Cube dimension → Dimension
  - Cube measure → Measure
  - Cube join → Relationship

Usage:
    connector = CubeConnector(schema_dict)
    analyst.use(connector)

    # Export back to Cube format
    cube_dict = connector.export()
"""

from __future__ import annotations

from typing import Any

from dashboardmd.connector import Connector
from dashboardmd.interop.cube import from_cube, to_cube_schema
from dashboardmd.model import Entity, Relationship
from dashboardmd.sources.base import SourceHandler


class CubeConnector(Connector):
    """Import a Cube.js schema as a dashboardmd connector.

    Parses Cube cubes, dimensions, measures, and joins
    into entities, dimensions, measures, and relationships.

    Args:
        schema: Cube.js schema dict with "cubes" key.
        data_sources: Optional mapping of entity name → SourceHandler
            for actual data.
    """

    def __init__(
        self,
        schema: dict[str, Any],
        data_sources: dict[str, SourceHandler | str] | None = None,
    ) -> None:
        self._schema = schema
        self._data_sources = data_sources or {}
        self._entities, self._relationships = from_cube(schema)

    def name(self) -> str:
        return "cube"

    def sources(self) -> dict[str, SourceHandler]:
        result: dict[str, SourceHandler] = {}
        for name, source in self._data_sources.items():
            if isinstance(source, SourceHandler):
                result[name] = source
        return result

    def entities(self) -> list[Entity]:
        return self._entities

    def relationships(self) -> list[Relationship]:
        return self._relationships

    def register(self, analyst: Any) -> None:
        """Register model into an Analyst.

        Only registers actual data sources if data_sources were provided.
        Entity source fields from Cube (sql) are SQL expressions,
        not file paths, so they are not registered as data sources.
        """
        for table_name, source in self._data_sources.items():
            analyst.add(table_name, source)

        for entity in self._entities:
            entity_copy = Entity(
                name=entity.name,
                source=None,
                dimensions=entity.dimensions,
                measures=entity.measures,
            )
            analyst._entities[entity.name] = entity_copy
            analyst._query_builder = None

        if self._relationships:
            analyst._relationships.extend(self._relationships)
            analyst._query_builder = None

    def export(self) -> dict[str, Any]:
        """Export the current model back to Cube.js format."""
        return to_cube_schema(self._entities, self._relationships)
