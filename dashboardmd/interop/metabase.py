"""Metabase import/export: from_metabase() / to_metabase().

Metabase models map to dashboardmd as follows:
  - Metabase Table → Entity
  - Metabase Field (dimension) → Dimension
  - Metabase Metric → Measure
  - Metabase ForeignKey → Relationship
"""

from __future__ import annotations

from typing import Any

from dashboardmd.model import Dimension, Entity, Measure, Relationship

# Metabase field type → dashboardmd dimension type
_METABASE_TYPE_MAP: dict[str, str] = {
    "type/Integer": "number",
    "type/BigInteger": "number",
    "type/Float": "number",
    "type/Decimal": "number",
    "type/Number": "number",
    "type/DateTime": "time",
    "type/Date": "time",
    "type/Time": "time",
    "type/DateTimeWithLocalTZ": "time",
    "type/Text": "string",
    "type/Name": "string",
    "type/Category": "string",
    "type/City": "string",
    "type/State": "string",
    "type/Country": "string",
    "type/ZipCode": "string",
    "type/Email": "string",
    "type/URL": "string",
    "type/Boolean": "boolean",
}

# Metabase aggregation → dashboardmd measure type
_METABASE_AGG_MAP: dict[str, str] = {
    "sum": "sum",
    "count": "count",
    "avg": "avg",
    "distinct": "count_distinct",
    "min": "min",
    "max": "max",
}


def from_metabase(metadata: dict[str, Any]) -> tuple[list[Entity], list[Relationship]]:
    """Convert Metabase metadata export to dashboardmd entities and relationships.

    Args:
        metadata: A dict representing Metabase metadata, expected to have:
            - "tables": list of table dicts with "name", "fields", "metrics"
            - Each field has "name", "base_type", "semantic_type", "fk_target_field_id"
            - Each metric has "name", "aggregation", "field" (optional)

    Returns:
        Tuple of (entities, relationships).
    """
    entities: list[Entity] = []
    relationships: list[Relationship] = []
    field_id_to_table: dict[int, tuple[str, str]] = {}  # field_id → (table_name, field_name)

    tables = metadata.get("tables", [])

    # First pass: build field ID map
    for table in tables:
        for fld in table.get("fields", []):
            fid = fld.get("id")
            if fid is not None:
                field_id_to_table[fid] = (table["name"], fld["name"])

    # Second pass: build entities and relationships
    for table in tables:
        dimensions: list[Dimension] = []
        measures: list[Measure] = []

        for fld in table.get("fields", []):
            base_type = fld.get("base_type", "type/Text")
            dim_type = _METABASE_TYPE_MAP.get(base_type, "string")
            is_pk = fld.get("semantic_type") == "type/PK"

            dimensions.append(
                Dimension(
                    name=fld["name"],
                    type=dim_type,
                    primary_key=is_pk,
                )
            )

            # Foreign key → relationship
            fk_target = fld.get("fk_target_field_id")
            if fk_target is not None and fk_target in field_id_to_table:
                target_table, target_field = field_id_to_table[fk_target]
                relationships.append(
                    Relationship(
                        from_entity=table["name"],
                        to_entity=target_table,
                        on=(fld["name"], target_field),
                        type="many_to_one",
                    )
                )

        for metric in table.get("metrics", []):
            agg = metric.get("aggregation", "count")
            measure_type = _METABASE_AGG_MAP.get(agg, "count")
            measures.append(
                Measure(
                    name=metric["name"],
                    type=measure_type,
                    sql=metric.get("field"),
                )
            )

        entities.append(
            Entity(
                name=table["name"],
                dimensions=dimensions,
                measures=measures,
            )
        )

    return entities, relationships


def to_metabase(
    entities: list[Entity],
    relationships: list[Relationship] | None = None,
) -> dict[str, Any]:
    """Convert dashboardmd entities to Metabase-compatible metadata.

    Args:
        entities: List of Entity instances.
        relationships: Optional list of Relationship instances.

    Returns:
        Dict in Metabase metadata format.
    """
    # Build reverse type map
    dim_type_to_metabase: dict[str, str] = {
        "number": "type/Number",
        "string": "type/Text",
        "time": "type/DateTime",
        "boolean": "type/Boolean",
    }

    measure_type_to_metabase: dict[str, str] = {
        "sum": "sum",
        "count": "count",
        "count_distinct": "distinct",
        "avg": "avg",
        "min": "min",
        "max": "max",
        "number": "sum",
    }

    # Build FK lookup: (from_entity, from_col) → field_id of target
    fk_map: dict[tuple[str, str], tuple[str, str]] = {}
    for rel in relationships or []:
        fk_map[(rel.from_entity, rel.on[0])] = (rel.to_entity, rel.on[1])

    tables: list[dict[str, Any]] = []
    field_counter = 1

    for entity in entities:
        fields: list[dict[str, Any]] = []
        metrics: list[dict[str, Any]] = []

        for dim in entity.dimensions:
            field_dict: dict[str, Any] = {
                "id": field_counter,
                "name": dim.name,
                "base_type": dim_type_to_metabase.get(dim.type, "type/Text"),
            }
            if dim.primary_key:
                field_dict["semantic_type"] = "type/PK"

            fk_target = fk_map.get((entity.name, dim.name))
            if fk_target is not None:
                field_dict["semantic_type"] = "type/FK"
                # We'd need to resolve the target field ID in a real export
                field_dict["fk_target_field_id"] = None

            fields.append(field_dict)
            field_counter += 1

        for measure in entity.measures:
            metrics.append({
                "name": measure.name,
                "aggregation": measure_type_to_metabase.get(measure.type, "count"),
                "field": measure.sql,
            })

        tables.append({
            "name": entity.name,
            "fields": fields,
            "metrics": metrics,
        })

    return {"tables": tables}
