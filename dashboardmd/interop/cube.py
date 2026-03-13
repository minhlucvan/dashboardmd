"""Cube import/export: from_cube() / to_cube_schema().

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


def to_cube_schema(
    entities: list[Entity],
    relationships: list[Relationship] | None = None,
) -> dict[str, Any]:
    """Convert dashboardmd entities to a Cube.js schema dict.

    Args:
        entities: List of Entity instances.
        relationships: Optional list of Relationship instances.

    Returns:
        Dict in Cube.js schema format with "cubes" key.
    """
    dim_type_to_cube: dict[str, str] = {
        "string": "string",
        "number": "number",
        "time": "time",
        "boolean": "boolean",
    }

    measure_type_to_cube: dict[str, str] = {
        "sum": "sum",
        "count": "count",
        "count_distinct": "countDistinct",
        "avg": "avg",
        "min": "min",
        "max": "max",
        "number": "number",
    }

    rel_type_to_cube: dict[str, str] = {
        "many_to_one": "belongsTo",
        "one_to_many": "hasMany",
        "one_to_one": "hasOne",
        "many_to_many": "hasMany",
    }

    # Build relationship lookup
    from collections import defaultdict

    rel_by_entity: dict[str, list[Relationship]] = defaultdict(list)
    for rel in relationships or []:
        rel_by_entity[rel.from_entity].append(rel)

    cubes: list[dict[str, Any]] = []
    for entity in entities:
        dims: list[dict[str, Any]] = []
        for dim in entity.dimensions:
            d: dict[str, Any] = {
                "name": dim.name,
                "type": dim_type_to_cube.get(dim.type, "string"),
            }
            if dim.sql:
                d["sql"] = dim.sql
            if dim.primary_key:
                d["primaryKey"] = True
            dims.append(d)

        msrs: list[dict[str, Any]] = []
        for measure in entity.measures:
            m: dict[str, Any] = {
                "name": measure.name,
                "type": measure_type_to_cube.get(measure.type, "count"),
            }
            if measure.sql:
                m["sql"] = measure.sql
            msrs.append(m)

        joins: dict[str, dict[str, Any]] = {}
        for rel in rel_by_entity.get(entity.name, []):
            joins[rel.to_entity] = {
                "sql": f"{{CUBE}}.{rel.on[0]} = {{{rel.to_entity}}}.{rel.on[1]}",
                "relationship": rel_type_to_cube.get(rel.type, "belongsTo"),
            }

        cube: dict[str, Any] = {
            "name": entity.name,
            "dimensions": dims,
            "measures": msrs,
        }
        if entity.source:
            cube["sql"] = str(entity.source)
        if joins:
            cube["joins"] = joins
        cubes.append(cube)

    return {"cubes": cubes}
