"""Looker/LookML connector: from_lookml() imports LookML data models.

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
