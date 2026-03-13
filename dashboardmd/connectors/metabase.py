"""Metabase connector: import/export Metabase data models.

Metabase models map to dashboardmd as follows:
  - Metabase Table → Entity
  - Metabase Field (dimension) → Dimension
  - Metabase Metric → Measure
  - Metabase ForeignKey → Relationship

Usage:
    connector = MetabaseConnector(metadata_dict)
    analyst.use(connector)  # registers entities + relationships

    # Export back to Metabase format
    metabase_dict = connector.export()
"""

from __future__ import annotations

from typing import Any

from dashboardmd.connector import Connector
from dashboardmd.interop.metabase import from_metabase, to_metabase
from dashboardmd.model import Entity, Relationship
from dashboardmd.sources.base import SourceHandler


class MetabaseConnector(Connector):
    """Import a Metabase data model as a dashboardmd connector.

    Parses Metabase metadata (tables, fields, metrics, foreign keys)
    into entities, dimensions, measures, and relationships.

    Composes with any other connector::

        analyst = Analyst()
        analyst.use(MetabaseConnector(metabase_export))
        analyst.use(GitHubConnector(token="...", repo="org/repo"))
        analyst.add_relationship(Relationship(
            "metabase_orders", "pull_requests", on=("id", "order_id")
        ))

    Args:
        metadata: Metabase metadata dict with "tables" key.
        data_sources: Optional mapping of entity name → SourceHandler
            for actual data. Without this, only the model is registered.
    """

    def __init__(
        self,
        metadata: dict[str, Any],
        data_sources: dict[str, SourceHandler | str] | None = None,
    ) -> None:
        self._metadata = metadata
        self._data_sources = data_sources or {}
        self._entities, self._relationships = from_metabase(metadata)

    def name(self) -> str:
        return "metabase"

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

        If data_sources were provided, registers those first.
        Then registers entities and relationships.
        String-based data sources are registered via analyst.add().
        """
        # Register data sources (both SourceHandler and string paths)
        for table_name, source in self._data_sources.items():
            analyst.add(table_name, source)

        # Register entities (model only — data sources come from data_sources param)
        for entity in self._entities:
            entity_copy = Entity(
                name=entity.name,
                source=None,
                dimensions=entity.dimensions,
                measures=entity.measures,
            )
            analyst._entities[entity.name] = entity_copy
            analyst._query_builder = None

        # Register relationships
        if self._relationships:
            analyst._relationships.extend(self._relationships)
            analyst._query_builder = None

    def export(self) -> dict[str, Any]:
        """Export the current model back to Metabase format."""
        return to_metabase(self._entities, self._relationships)
