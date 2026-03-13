"""Cube.js connector: from_cube() imports Cube data models.

Cube.js schema maps to dashboardmd as follows:
  - Cube → Entity
  - Cube dimension → Dimension
  - Cube measure → Measure
  - Cube join → Relationship
"""

from __future__ import annotations

from typing import Any

from dashboardmd.model import Dimension, Entity, Measure, Relationship

# Cube type → dashboardmd type
_CUBE_DIM_TYPE_MAP: dict[str, str] = {
    "string": "string",
    "number": "number",
    "time": "time",
    "boolean": "boolean",
    "geo": "string",
}

_CUBE_MEASURE_TYPE_MAP: dict[str, str] = {
    "sum": "sum",
    "count": "count",
    "countDistinct": "count_distinct",
    "countDistinctApprox": "count_distinct",
    "avg": "avg",
    "min": "min",
    "max": "max",
    "number": "number",
    "runningTotal": "sum",
}

_CUBE_JOIN_TYPE_MAP: dict[str, str] = {
    "hasOne": "many_to_one",
    "hasMany": "one_to_many",
    "belongsTo": "many_to_one",
}


def from_cube(schema: dict[str, Any]) -> tuple[list[Entity], list[Relationship]]:
    """Convert a Cube.js schema dict to dashboardmd entities and relationships.

    Args:
        schema: A dict with "cubes" key containing a list of cube dicts.
            Each cube has "name", "sql", "dimensions", "measures", "joins".

    Returns:
        Tuple of (entities, relationships).
    """
    entities: list[Entity] = []
    relationships: list[Relationship] = []

    for cube in schema.get("cubes", []):
        dimensions: list[Dimension] = []
        measures: list[Measure] = []

        for dim in cube.get("dimensions", []):
            dim_type = _CUBE_DIM_TYPE_MAP.get(dim.get("type", "string"), "string")
            dimensions.append(
                Dimension(
                    name=dim["name"],
                    type=dim_type,
                    sql=dim.get("sql"),
                    primary_key=dim.get("primaryKey", False),
                )
            )

        for msr in cube.get("measures", []):
            measure_type = _CUBE_MEASURE_TYPE_MAP.get(msr.get("type", "count"), "count")
            measures.append(
                Measure(
                    name=msr["name"],
                    type=measure_type,
                    sql=msr.get("sql"),
                )
            )

        entities.append(
            Entity(
                name=cube["name"],
                source=cube.get("sql"),
                dimensions=dimensions,
                measures=measures,
            )
        )

        # Parse joins
        for join_name, join_def in cube.get("joins", {}).items():
            rel_type = _CUBE_JOIN_TYPE_MAP.get(
                join_def.get("relationship", "belongsTo"), "many_to_one"
            )
            from_col, to_col = _parse_cube_sql(join_def.get("sql", ""), cube["name"], join_name)
            relationships.append(
                Relationship(
                    from_entity=cube["name"],
                    to_entity=join_name,
                    on=(from_col, to_col),
                    type=rel_type,
                )
            )

    return entities, relationships


def _parse_cube_sql(sql: str, from_cube: str, to_cube: str) -> tuple[str, str]:
    """Parse a Cube.js join SQL expression.

    Handles patterns like:
        "{CUBE}.customer_id = {Customers}.id"
        "{from_cube.customer_id} = {to_cube.id}"
    """
    clean = sql.replace("{CUBE}", from_cube).replace("{", "").replace("}", "").strip()
    parts = clean.split("=")
    if len(parts) == 2:
        left = parts[0].strip().split(".")[-1]
        right = parts[1].strip().split(".")[-1]
        return left, right
    return "id", "id"
