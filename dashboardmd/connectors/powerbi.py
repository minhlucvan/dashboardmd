"""PowerBI connector: import/export PowerBI tabular models.

PowerBI Tabular Model maps to dashboardmd as follows:
  - PowerBI Table → Entity
  - PowerBI Column → Dimension
  - PowerBI Measure (DAX) → Measure
  - PowerBI Relationship → Relationship

Usage:
    connector = PowerBIConnector(model_dict)
    analyst.use(connector)

    # Export back to PowerBI format
    powerbi_dict = connector.export()
"""

from __future__ import annotations

from typing import Any

from dashboardmd.connector import Connector
from dashboardmd.interop.powerbi import from_powerbi, to_powerbi
from dashboardmd.model import Entity, Relationship
from dashboardmd.sources.base import SourceHandler


class PowerBIConnector(Connector):
    """Import a PowerBI tabular model as a dashboardmd connector.

    Parses PowerBI tables, columns, DAX measures, and relationships
    into entities, dimensions, measures, and relationships.

    Args:
        model: PowerBI tabular model dict with "tables" and "relationships" keys.
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
        self._entities, self._relationships = from_powerbi(model)

    def name(self) -> str:
        return "powerbi"

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
        PowerBI entity sources are not file paths.
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
        """Export the current model back to PowerBI format."""
        return to_powerbi(self._entities, self._relationships)
