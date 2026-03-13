"""PowerBI connector: from_powerbi() imports PowerBI tabular models.

PowerBI Tabular Model maps to dashboardmd as follows:
  - PowerBI Table → Entity
  - PowerBI Column → Dimension
  - PowerBI Measure (DAX) → Measure
  - PowerBI Relationship → Relationship
"""

from __future__ import annotations

from typing import Any

from dashboardmd.model import Dimension, Entity, Measure, Relationship

# PowerBI data type → dashboardmd type
_POWERBI_TYPE_MAP: dict[str, str] = {
    "int64": "number",
    "double": "number",
    "decimal": "number",
    "currency": "number",
    "string": "string",
    "boolean": "boolean",
    "dateTime": "time",
    "date": "time",
    "time": "time",
}

# Simple DAX pattern → measure type mapping
_DAX_AGG_PATTERNS: dict[str, str] = {
    "SUM(": "sum",
    "COUNT(": "count",
    "COUNTROWS(": "count",
    "DISTINCTCOUNT(": "count_distinct",
    "AVERAGE(": "avg",
    "MIN(": "min",
    "MAX(": "max",
}

_POWERBI_CARDINALITY_MAP: dict[str, str] = {
    "oneToMany": "one_to_many",
    "manyToOne": "many_to_one",
    "oneToOne": "one_to_one",
    "manyToMany": "many_to_many",
}


def from_powerbi(model: dict[str, Any]) -> tuple[list[Entity], list[Relationship]]:
    """Convert a PowerBI tabular model dict to dashboardmd entities and relationships.

    Args:
        model: A dict with:
            - "tables": list of table dicts with "name", "columns", "measures"
            - "relationships": list of relationship dicts

    Returns:
        Tuple of (entities, relationships).
    """
    entities: list[Entity] = []
    relationships: list[Relationship] = []

    for table in model.get("tables", []):
        dimensions: list[Dimension] = []
        measures: list[Measure] = []

        for col in table.get("columns", []):
            data_type = col.get("dataType", "string")
            dim_type = _POWERBI_TYPE_MAP.get(data_type, "string")
            is_pk = col.get("isKey", False)
            dimensions.append(
                Dimension(
                    name=col["name"],
                    type=dim_type,
                    primary_key=is_pk,
                )
            )

        for msr in table.get("measures", []):
            dax = msr.get("expression", "")
            measure_type = _infer_measure_type_from_dax(dax)
            sql_col = _extract_column_from_dax(dax)
            measures.append(
                Measure(
                    name=msr["name"],
                    type=measure_type,
                    sql=sql_col,
                )
            )

        entities.append(
            Entity(
                name=table["name"],
                dimensions=dimensions,
                measures=measures,
            )
        )

    for rel in model.get("relationships", []):
        cardinality = rel.get("crossFilteringBehavior", rel.get("cardinality", "manyToOne"))
        rel_type = _POWERBI_CARDINALITY_MAP.get(cardinality, "many_to_one")
        relationships.append(
            Relationship(
                from_entity=rel["fromTable"],
                to_entity=rel["toTable"],
                on=(rel["fromColumn"], rel["toColumn"]),
                type=rel_type,
            )
        )

    return entities, relationships


def _infer_measure_type_from_dax(dax: str) -> str:
    """Infer measure type from a DAX expression."""
    upper = dax.upper().strip()
    for pattern, measure_type in _DAX_AGG_PATTERNS.items():
        if upper.startswith(pattern):
            return measure_type
    return "number"


def _extract_column_from_dax(dax: str) -> str | None:
    """Extract column reference from a simple DAX expression.

    Handles patterns like:
        SUM(Orders[Amount])
        AVERAGE([Price])
    """
    import re

    # Match Table[Column] or [Column]
    match = re.search(r"\[(\w+)\]", dax)
    if match:
        return match.group(1)
    return None
