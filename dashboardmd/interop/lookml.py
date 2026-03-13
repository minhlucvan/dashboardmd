"""Looker/LookML import/export: from_lookml() / to_lookml().

LookML models map to dashboardmd as follows:
  - LookML view → Entity
  - LookML dimension → Dimension
  - LookML measure → Measure
  - LookML explore join → Relationship
"""

from __future__ import annotations

from typing import Any

from dashboardmd.model import Dimension, Entity, Measure, Relationship

# LookML type → dashboardmd type
_LOOKML_DIM_TYPE_MAP: dict[str, str] = {
    "string": "string",
    "number": "number",
    "yesno": "boolean",
    "date": "time",
    "time": "time",
    "date_time": "time",
    "date_raw": "time",
    "date_date": "time",
    "date_week": "time",
    "date_month": "time",
    "date_quarter": "time",
    "date_year": "time",
    "tier": "string",
    "zipcode": "string",
    "location": "string",
}

_LOOKML_MEASURE_TYPE_MAP: dict[str, str] = {
    "sum": "sum",
    "count": "count",
    "count_distinct": "count_distinct",
    "average": "avg",
    "min": "min",
    "max": "max",
    "number": "number",
    "sum_distinct": "sum",
}


def from_lookml(model: dict[str, Any]) -> tuple[list[Entity], list[Relationship]]:
    """Convert a LookML model dict to dashboardmd entities and relationships.

    Args:
        model: A dict with:
            - "views": list of view dicts with "name", "sql_table_name",
              "dimensions", "measures"
            - "explores": list of explore dicts with "joins"

    Returns:
        Tuple of (entities, relationships).
    """
    entities: list[Entity] = []
    relationships: list[Relationship] = []

    for view in model.get("views", []):
        dimensions: list[Dimension] = []
        measures: list[Measure] = []

        for dim in view.get("dimensions", []):
            dim_type = _LOOKML_DIM_TYPE_MAP.get(dim.get("type", "string"), "string")
            dimensions.append(
                Dimension(
                    name=dim["name"],
                    type=dim_type,
                    sql=dim.get("sql"),
                    primary_key=dim.get("primary_key", False),
                )
            )

        for msr in view.get("measures", []):
            measure_type = _LOOKML_MEASURE_TYPE_MAP.get(msr.get("type", "count"), "count")
            measures.append(
                Measure(
                    name=msr["name"],
                    type=measure_type,
                    sql=msr.get("sql"),
                )
            )

        entities.append(
            Entity(
                name=view["name"],
                source=view.get("sql_table_name"),
                dimensions=dimensions,
                measures=measures,
            )
        )

    # Parse explores for relationships
    for explore in model.get("explores", []):
        base_view = explore.get("from", explore.get("name"))
        for join in explore.get("joins", []):
            join_view = join.get("from", join["name"])
            sql_on = join.get("sql_on", "")
            rel_type = join.get("relationship", "many_to_one")

            # Parse simple sql_on patterns like "${orders.customer_id} = ${customers.id}"
            from_col, to_col = _parse_lookml_sql_on(sql_on, base_view, join_view)

            relationships.append(
                Relationship(
                    from_entity=base_view,
                    to_entity=join_view,
                    on=(from_col, to_col),
                    type=rel_type,
                )
            )

    return entities, relationships


def _parse_lookml_sql_on(sql_on: str, from_view: str, to_view: str) -> tuple[str, str]:
    """Parse a LookML sql_on expression to extract column names.

    Handles patterns like:
        "${orders.customer_id} = ${customers.id}"
        "orders.customer_id = customers.id"
    """
    # Strip LookML variable syntax
    clean = sql_on.replace("${", "").replace("}", "").strip()
    parts = clean.split("=")
    if len(parts) == 2:
        left = parts[0].strip().split(".")[-1]
        right = parts[1].strip().split(".")[-1]
        return left, right
    return "id", "id"


def to_lookml(
    entities: list[Entity],
    relationships: list[Relationship] | None = None,
) -> dict[str, Any]:
    """Convert dashboardmd entities to LookML-compatible model dict.

    Args:
        entities: List of Entity instances.
        relationships: Optional list of Relationship instances.

    Returns:
        Dict in LookML model format with "views" and "explores".
    """
    dim_type_to_lookml: dict[str, str] = {
        "string": "string",
        "number": "number",
        "time": "date_time",
        "boolean": "yesno",
    }

    measure_type_to_lookml: dict[str, str] = {
        "sum": "sum",
        "count": "count",
        "count_distinct": "count_distinct",
        "avg": "average",
        "min": "min",
        "max": "max",
        "number": "number",
    }

    views: list[dict[str, Any]] = []
    for entity in entities:
        dims: list[dict[str, Any]] = []
        for dim in entity.dimensions:
            d: dict[str, Any] = {
                "name": dim.name,
                "type": dim_type_to_lookml.get(dim.type, "string"),
            }
            if dim.sql:
                d["sql"] = dim.sql
            if dim.primary_key:
                d["primary_key"] = True
            dims.append(d)

        msrs: list[dict[str, Any]] = []
        for measure in entity.measures:
            m: dict[str, Any] = {
                "name": measure.name,
                "type": measure_type_to_lookml.get(measure.type, "count"),
            }
            if measure.sql:
                m["sql"] = measure.sql
            msrs.append(m)

        view: dict[str, Any] = {"name": entity.name, "dimensions": dims, "measures": msrs}
        if entity.source:
            view["sql_table_name"] = str(entity.source)
        views.append(view)

    # Build explores from relationships
    explores: list[dict[str, Any]] = []
    if relationships:
        # Group relationships by from_entity
        from collections import defaultdict

        rel_groups: dict[str, list[Relationship]] = defaultdict(list)
        for rel in relationships:
            rel_groups[rel.from_entity].append(rel)

        for base, rels in rel_groups.items():
            joins = []
            for rel in rels:
                joins.append({
                    "name": rel.to_entity,
                    "sql_on": f"${{{base}.{rel.on[0]}}} = ${{{rel.to_entity}.{rel.on[1]}}}",
                    "relationship": rel.type,
                })
            explores.append({"name": base, "joins": joins})

    return {"views": views, "explores": explores}
