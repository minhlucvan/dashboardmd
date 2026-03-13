"""Auto-discovery: detect dimensions, suggest measures."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import duckdb

from dashboardmd.model import Dimension, Entity, Measure, Relationship


def discover(directory: str) -> list[Entity]:
    """Scan a directory for data files and create entities with inferred dimensions.

    Args:
        directory: Path to a directory containing CSV, Parquet, or JSON files.

    Returns:
        List of Entity instances with inferred dimensions and sources.
    """
    data_extensions = {".csv", ".parquet", ".json"}
    dir_path = Path(directory)
    entities: list[Entity] = []

    if not dir_path.is_dir():
        return []

    for file_path in sorted(dir_path.iterdir()):
        if file_path.suffix.lower() not in data_extensions:
            continue

        name = file_path.stem
        dimensions = _infer_dimensions(file_path)
        entities.append(
            Entity(
                name=name,
                source=str(file_path),
                dimensions=dimensions,
            )
        )

    return entities


def _infer_dimensions(file_path: Path) -> list[Dimension]:
    """Infer dimensions from a data file by reading its schema."""
    conn = duckdb.connect(":memory:")
    try:
        suffix = file_path.suffix.lower()
        if suffix == ".csv":
            rel = conn.execute(f"SELECT * FROM read_csv_auto('{file_path}') LIMIT 0")
        elif suffix == ".parquet":
            rel = conn.execute(f"SELECT * FROM read_parquet('{file_path}') LIMIT 0")
        elif suffix == ".json":
            rel = conn.execute(f"SELECT * FROM read_json_auto('{file_path}') LIMIT 0")
        else:
            return []

        columns = rel.description
        dimensions: list[Dimension] = []

        for col_name, col_type, *_ in columns:
            dim_type = _duckdb_type_to_dimension_type(str(col_type))
            is_pk = col_name.lower() == "id"
            dimensions.append(
                Dimension(
                    name=col_name,
                    type=dim_type,
                    primary_key=is_pk,
                )
            )

        return dimensions
    finally:
        conn.close()


def _duckdb_type_to_dimension_type(duckdb_type: str) -> str:
    """Map a DuckDB type string to a Dimension type."""
    t = duckdb_type.upper()
    if any(kw in t for kw in ("INT", "BIGINT", "SMALLINT", "TINYINT", "FLOAT", "DOUBLE", "DECIMAL", "NUMERIC", "HUGEINT")):
        return "number"
    if any(kw in t for kw in ("DATE", "TIME", "TIMESTAMP")):
        return "time"
    if "BOOL" in t:
        return "boolean"
    return "string"


def suggest_measures(entity: Entity) -> list[Measure]:
    """Suggest measures based on an entity's dimensions.

    Numeric non-PK columns get SUM/AVG suggestions.
    Every entity gets a COUNT measure.

    Args:
        entity: An Entity with dimensions defined.

    Returns:
        List of suggested Measure instances.
    """
    measures: list[Measure] = []

    # Always suggest count
    measures.append(Measure(name="count", type="count"))

    for dim in entity.dimensions:
        if dim.type == "number" and not dim.primary_key:
            measures.append(Measure(name=f"{dim.name}_sum", type="sum", sql=dim.name))
            measures.append(Measure(name=f"{dim.name}_avg", type="avg", sql=dim.name))

    return measures


def auto_join(entities: list[Entity]) -> list[Relationship]:
    """Detect foreign key relationships from naming patterns.

    Looks for columns named '<entity_singular>_id' and matches them
    to entities with that name (pluralized).

    Args:
        entities: List of entities to analyze.

    Returns:
        List of inferred Relationship instances.
    """
    entity_names = {e.name for e in entities}
    # Build a map from singular forms to entity names
    singular_to_entity: dict[str, str] = {}
    for name in entity_names:
        # Simple depluralization: remove trailing 's'
        if name.endswith("s"):
            singular_to_entity[name[:-1]] = name
        singular_to_entity[name] = name

    relationships: list[Relationship] = []

    for entity in entities:
        for dim in entity.dimensions:
            col = dim.name.lower()
            if col.endswith("_id") and col != "id":
                # Extract the referenced entity name
                ref_singular = col[:-3]  # Remove '_id'
                ref_entity = singular_to_entity.get(ref_singular)
                if ref_entity and ref_entity != entity.name and ref_entity in entity_names:
                    relationships.append(
                        Relationship(
                            from_entity=entity.name,
                            to_entity=ref_entity,
                            on=(col, "id"),
                            type="many_to_one",
                        )
                    )

    return relationships
