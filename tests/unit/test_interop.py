"""Tests for BI platform interop modules."""

from __future__ import annotations

import pytest

from dashboardmd.model import Dimension, Entity, Measure, Relationship


# ---------------------------------------------------------------------------
# Test fixtures
# ---------------------------------------------------------------------------

def _sample_entities() -> list[Entity]:
    """Create sample entities for interop testing."""
    orders = Entity(
        name="orders",
        source="data/orders.csv",
        dimensions=[
            Dimension("id", type="number", primary_key=True),
            Dimension("date", type="time"),
            Dimension("status", type="string"),
            Dimension("customer_id", type="number"),
        ],
        measures=[
            Measure("revenue", type="sum", sql="amount"),
            Measure("count", type="count"),
        ],
    )
    customers = Entity(
        name="customers",
        source="data/customers.csv",
        dimensions=[
            Dimension("id", type="number", primary_key=True),
            Dimension("name", type="string"),
            Dimension("segment", type="string"),
        ],
        measures=[
            Measure("count", type="count"),
        ],
    )
    return [orders, customers]


def _sample_relationships() -> list[Relationship]:
    return [
        Relationship("orders", "customers", on=("customer_id", "id"), type="many_to_one"),
    ]


# ---------------------------------------------------------------------------
# Metabase
# ---------------------------------------------------------------------------


class TestMetabaseInterop:
    """Metabase import/export."""

    def test_to_metabase_produces_tables(self) -> None:
        """to_metabase() should produce a dict with tables."""
        from dashboardmd.interop.metabase import to_metabase

        result = to_metabase(_sample_entities(), _sample_relationships())
        assert "tables" in result
        assert len(result["tables"]) == 2

    def test_to_metabase_preserves_entity_names(self) -> None:
        """Table names should match entity names."""
        from dashboardmd.interop.metabase import to_metabase

        result = to_metabase(_sample_entities())
        names = [t["name"] for t in result["tables"]]
        assert "orders" in names
        assert "customers" in names

    def test_to_metabase_includes_fields_and_metrics(self) -> None:
        """Tables should have fields (dimensions) and metrics (measures)."""
        from dashboardmd.interop.metabase import to_metabase

        result = to_metabase(_sample_entities())
        orders = next(t for t in result["tables"] if t["name"] == "orders")
        assert len(orders["fields"]) == 4
        assert len(orders["metrics"]) == 2

    def test_from_metabase_roundtrip(self) -> None:
        """from_metabase(to_metabase(entities)) should preserve structure."""
        from dashboardmd.interop.metabase import from_metabase, to_metabase

        exported = to_metabase(_sample_entities(), _sample_relationships())
        entities, rels = from_metabase(exported)

        assert len(entities) == 2
        orders = next(e for e in entities if e.name == "orders")
        assert len(orders.dimensions) == 4
        assert len(orders.measures) == 2

    def test_from_metabase_field_types(self) -> None:
        """Metabase field types should map to correct dimension types."""
        from dashboardmd.interop.metabase import from_metabase

        metadata = {
            "tables": [{
                "name": "test",
                "fields": [
                    {"id": 1, "name": "id", "base_type": "type/Integer", "semantic_type": "type/PK"},
                    {"id": 2, "name": "created_at", "base_type": "type/DateTime"},
                    {"id": 3, "name": "active", "base_type": "type/Boolean"},
                ],
                "metrics": [],
            }]
        }
        entities, _ = from_metabase(metadata)
        dims = {d.name: d for d in entities[0].dimensions}
        assert dims["id"].type == "number"
        assert dims["id"].primary_key is True
        assert dims["created_at"].type == "time"
        assert dims["active"].type == "boolean"


# ---------------------------------------------------------------------------
# LookML
# ---------------------------------------------------------------------------


class TestLookMLInterop:
    """LookML import/export."""

    def test_to_lookml_produces_views(self) -> None:
        """to_lookml() should produce views and explores."""
        from dashboardmd.interop.lookml import to_lookml

        result = to_lookml(_sample_entities(), _sample_relationships())
        assert "views" in result
        assert len(result["views"]) == 2

    def test_to_lookml_preserves_measures(self) -> None:
        """LookML views should include measures."""
        from dashboardmd.interop.lookml import to_lookml

        result = to_lookml(_sample_entities())
        orders_view = next(v for v in result["views"] if v["name"] == "orders")
        assert len(orders_view["measures"]) == 2

    def test_to_lookml_generates_explores(self) -> None:
        """Relationships should generate LookML explores with joins."""
        from dashboardmd.interop.lookml import to_lookml

        result = to_lookml(_sample_entities(), _sample_relationships())
        assert len(result["explores"]) >= 1
        explore = result["explores"][0]
        assert len(explore["joins"]) >= 1

    def test_from_lookml_roundtrip(self) -> None:
        """from_lookml(to_lookml(entities)) should preserve structure."""
        from dashboardmd.interop.lookml import from_lookml, to_lookml

        exported = to_lookml(_sample_entities(), _sample_relationships())
        entities, rels = from_lookml(exported)

        assert len(entities) == 2
        orders = next(e for e in entities if e.name == "orders")
        assert len(orders.dimensions) == 4

    def test_from_lookml_dimension_types(self) -> None:
        """LookML dimension types should map correctly."""
        from dashboardmd.interop.lookml import from_lookml

        model = {
            "views": [{
                "name": "test",
                "dimensions": [
                    {"name": "id", "type": "number", "primary_key": True},
                    {"name": "created_at", "type": "date_time"},
                    {"name": "active", "type": "yesno"},
                ],
                "measures": [{"name": "count", "type": "count"}],
            }],
            "explores": [],
        }
        entities, _ = from_lookml(model)
        dims = {d.name: d for d in entities[0].dimensions}
        assert dims["id"].type == "number"
        assert dims["id"].primary_key is True
        assert dims["created_at"].type == "time"
        assert dims["active"].type == "boolean"


# ---------------------------------------------------------------------------
# Cube
# ---------------------------------------------------------------------------


class TestCubeInterop:
    """Cube.js import/export."""

    def test_to_cube_schema_produces_cubes(self) -> None:
        """to_cube_schema() should produce cubes."""
        from dashboardmd.interop.cube import to_cube_schema

        result = to_cube_schema(_sample_entities(), _sample_relationships())
        assert "cubes" in result
        assert len(result["cubes"]) == 2

    def test_to_cube_schema_includes_joins(self) -> None:
        """Cubes with relationships should have joins."""
        from dashboardmd.interop.cube import to_cube_schema

        result = to_cube_schema(_sample_entities(), _sample_relationships())
        orders_cube = next(c for c in result["cubes"] if c["name"] == "orders")
        assert "joins" in orders_cube
        assert "customers" in orders_cube["joins"]

    def test_from_cube_roundtrip(self) -> None:
        """from_cube(to_cube_schema(entities)) should preserve structure."""
        from dashboardmd.interop.cube import from_cube, to_cube_schema

        exported = to_cube_schema(_sample_entities(), _sample_relationships())
        entities, rels = from_cube(exported)

        assert len(entities) == 2
        orders = next(e for e in entities if e.name == "orders")
        assert len(orders.dimensions) == 4
        assert len(orders.measures) == 2

    def test_from_cube_measure_types(self) -> None:
        """Cube measure types should map correctly."""
        from dashboardmd.interop.cube import from_cube

        schema = {
            "cubes": [{
                "name": "test",
                "dimensions": [{"name": "id", "type": "number", "primaryKey": True}],
                "measures": [
                    {"name": "total", "type": "sum", "sql": "amount"},
                    {"name": "unique_users", "type": "countDistinct", "sql": "user_id"},
                ],
            }]
        }
        entities, _ = from_cube(schema)
        measures = {m.name: m for m in entities[0].measures}
        assert measures["total"].type == "sum"
        assert measures["unique_users"].type == "count_distinct"


# ---------------------------------------------------------------------------
# PowerBI
# ---------------------------------------------------------------------------


class TestPowerBIInterop:
    """PowerBI import/export."""

    def test_to_powerbi_produces_tables(self) -> None:
        """to_powerbi() should produce tables and relationships."""
        from dashboardmd.interop.powerbi import to_powerbi

        result = to_powerbi(_sample_entities(), _sample_relationships())
        assert "tables" in result
        assert "relationships" in result
        assert len(result["tables"]) == 2

    def test_to_powerbi_generates_dax_measures(self) -> None:
        """PowerBI measures should have DAX expressions."""
        from dashboardmd.interop.powerbi import to_powerbi

        result = to_powerbi(_sample_entities())
        orders = next(t for t in result["tables"] if t["name"] == "orders")
        revenue = next(m for m in orders["measures"] if m["name"] == "revenue")
        assert "SUM" in revenue["expression"]

    def test_to_powerbi_includes_relationships(self) -> None:
        """Relationships should appear in the PowerBI model."""
        from dashboardmd.interop.powerbi import to_powerbi

        result = to_powerbi(_sample_entities(), _sample_relationships())
        assert len(result["relationships"]) == 1
        rel = result["relationships"][0]
        assert rel["fromTable"] == "orders"
        assert rel["toTable"] == "customers"

    def test_from_powerbi_roundtrip(self) -> None:
        """from_powerbi(to_powerbi(entities)) should preserve structure."""
        from dashboardmd.interop.powerbi import from_powerbi, to_powerbi

        exported = to_powerbi(_sample_entities(), _sample_relationships())
        entities, rels = from_powerbi(exported)

        assert len(entities) == 2
        assert len(rels) == 1
        orders = next(e for e in entities if e.name == "orders")
        assert len(orders.dimensions) == 4

    def test_from_powerbi_dax_parsing(self) -> None:
        """PowerBI DAX expressions should be parsed to measure types."""
        from dashboardmd.interop.powerbi import from_powerbi

        model = {
            "tables": [{
                "name": "test",
                "columns": [{"name": "id", "dataType": "int64", "isKey": True}],
                "measures": [
                    {"name": "total", "expression": "SUM('test'[amount])"},
                    {"name": "avg_price", "expression": "AVERAGE('test'[price])"},
                    {"name": "n_customers", "expression": "DISTINCTCOUNT('test'[customer_id])"},
                ],
            }],
            "relationships": [],
        }
        entities, _ = from_powerbi(model)
        measures = {m.name: m for m in entities[0].measures}
        assert measures["total"].type == "sum"
        assert measures["avg_price"].type == "avg"
        assert measures["n_customers"].type == "count_distinct"


# ---------------------------------------------------------------------------
# Cross-platform
# ---------------------------------------------------------------------------


class TestInteropImports:
    """All interop functions should be importable from the interop package."""

    def test_import_all_from_interop(self) -> None:
        """All interop functions should be accessible."""
        from dashboardmd.interop import (
            from_cube,
            from_lookml,
            from_metabase,
            from_powerbi,
            to_cube_schema,
            to_lookml,
            to_metabase,
            to_powerbi,
        )

        assert callable(from_metabase)
        assert callable(to_metabase)
        assert callable(from_lookml)
        assert callable(to_lookml)
        assert callable(from_cube)
        assert callable(to_cube_schema)
        assert callable(from_powerbi)
        assert callable(to_powerbi)
