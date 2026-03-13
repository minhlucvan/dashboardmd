"""LookML/Looker connector: import/export LookML data models.

LookML models map to dashboardmd as follows:
  - LookML view → Entity
  - LookML dimension → Dimension
  - LookML measure → Measure
  - LookML explore join → Relationship

Usage:
    connector = LookMLConnector(model_dict)
    analyst.use(connector)

    # Export back to LookML format
    lookml_dict = connector.export()
"""

from __future__ import annotations

from typing import Any

from dashboardmd.connector import Connector
from dashboardmd.interop.lookml import from_lookml, to_lookml
from dashboardmd.model import Entity, Relationship
from dashboardmd.sources.base import SourceHandler


class LookMLConnector(Connector):
    """Import a LookML model as a dashboardmd connector.

    Parses LookML views, dimensions, measures, and explore joins
    into entities, dimensions, measures, and relationships.

    Args:
        model: LookML model dict with "views" and "explores" keys.
        data_sources: Optional mapping of entity name → SourceHandler
            for actual data.
    """

    def __init__(
        self,
        model: dict[str, Any],
        data_sources: dict[str, SourceHandler | str] | None = None,
    ) -> None:
        self._model = model
        self._data_sources = data_sources or {}
        self._entities, self._relationships = from_lookml(model)

    def name(self) -> str:
        return "lookml"

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
        Entity source fields from LookML (sql_table_name) are SQL references,
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
        """Export the current model back to LookML format."""
        return to_lookml(self._entities, self._relationships)
