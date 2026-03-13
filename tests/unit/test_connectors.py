"""Tests for Connector base class and built-in BI platform connectors."""

from __future__ import annotations

from typing import Any

import pytest

from dashboardmd.connector import Connector, DashboardWidget
from dashboardmd.model import Dimension, Entity, Measure, Relationship
from dashboardmd.sources.base import SourceHandler


# ---------------------------------------------------------------------------
# Test fixtures
# ---------------------------------------------------------------------------


def _metabase_metadata() -> dict[str, Any]:
    return {
        "tables": [
            {
                "name": "orders",
                "fields": [
                    {"id": 1, "name": "id", "base_type": "type/Integer", "semantic_type": "type/PK"},
                    {"id": 2, "name": "date", "base_type": "type/DateTime"},
                    {"id": 3, "name": "status", "base_type": "type/Text"},
                    {"id": 4, "name": "customer_id", "base_type": "type/Integer", "fk_target_field_id": 5},
                ],
                "metrics": [
                    {"name": "revenue", "aggregation": "sum", "field": "amount"},
                    {"name": "count", "aggregation": "count"},
                ],
            },
            {
                "name": "customers",
                "fields": [
                    {"id": 5, "name": "id", "base_type": "type/Integer", "semantic_type": "type/PK"},
                    {"id": 6, "name": "name", "base_type": "type/Text"},
                    {"id": 7, "name": "segment", "base_type": "type/Text"},
                ],
                "metrics": [{"name": "count", "aggregation": "count"}],
            },
        ]
    }


def _lookml_model() -> dict[str, Any]:
    return {
        "views": [
            {
                "name": "orders",
                "sql_table_name": "public.orders",
                "dimensions": [
                    {"name": "id", "type": "number", "primary_key": True},
                    {"name": "date", "type": "date_time"},
                    {"name": "status", "type": "string"},
                ],
                "measures": [
                    {"name": "revenue", "type": "sum", "sql": "amount"},
                    {"name": "count", "type": "count"},
                ],
            },
        ],
        "explores": [],
    }


def _cube_schema() -> dict[str, Any]:
    return {
        "cubes": [
            {
                "name": "orders",
                "sql": "SELECT * FROM orders",
                "dimensions": [
                    {"name": "id", "type": "number", "primaryKey": True},
                    {"name": "status", "type": "string"},
                ],
                "measures": [
                    {"name": "revenue", "type": "sum", "sql": "amount"},
                    {"name": "count", "type": "count"},
                ],
            },
        ]
    }


def _powerbi_model() -> dict[str, Any]:
    return {
        "tables": [
            {
                "name": "orders",
                "columns": [
                    {"name": "id", "dataType": "int64", "isKey": True},
                    {"name": "status", "dataType": "string"},
                ],
                "measures": [
                    {"name": "revenue", "expression": "SUM('orders'[amount])"},
                    {"name": "count", "expression": "COUNTROWS('orders')"},
                ],
            },
        ],
        "relationships": [],
    }


# ---------------------------------------------------------------------------
# Connector base class tests
# ---------------------------------------------------------------------------


class TestConnectorBase:
    """Test the Connector abstract base class."""

    def test_cannot_instantiate_abstract(self) -> None:
        """Connector is abstract — can't instantiate directly."""
        with pytest.raises(TypeError):
            Connector()  # type: ignore[abstract]

    def test_custom_connector(self) -> None:
        """A concrete Connector subclass should work."""

        class TestConnector(Connector):
            def name(self) -> str:
                return "test"

            def sources(self) -> dict[str, SourceHandler]:
                return {}

            def entities(self) -> list[Entity]:
                return [Entity("test_table", dimensions=[
                    Dimension("id", type="number", primary_key=True),
                ], measures=[
                    Measure("count", type="count"),
                ])]

        c = TestConnector()
        assert c.name() == "test"
        assert len(c.entities()) == 1
        assert c.relationships() == []
        assert c.widgets() == []
        assert c.available_dashboards() == []

    def test_dashboard_widget_dataclass(self) -> None:
        """DashboardWidget should store metadata correctly."""
        w = DashboardWidget(
            name="Overview",
            title="Test Overview",
            description="A test widget",
            requires=["orders", "customers"],
        )
        assert w.name == "Overview"
        assert w.requires == ["orders", "customers"]


# ---------------------------------------------------------------------------
# MetabaseConnector tests
# ---------------------------------------------------------------------------


class TestMetabaseConnector:
    """MetabaseConnector wraps from_metabase/to_metabase as a Connector."""

    def test_name(self) -> None:
        from dashboardmd.connectors import MetabaseConnector
        c = MetabaseConnector(_metabase_metadata())
        assert c.name() == "metabase"

    def test_entities_parsed(self) -> None:
        from dashboardmd.connectors import MetabaseConnector
        c = MetabaseConnector(_metabase_metadata())
        entities = c.entities()
        assert len(entities) == 2
        names = [e.name for e in entities]
        assert "orders" in names
        assert "customers" in names

    def test_relationships_parsed(self) -> None:
        from dashboardmd.connectors import MetabaseConnector
        c = MetabaseConnector(_metabase_metadata())
        rels = c.relationships()
        assert len(rels) == 1
        assert rels[0].from_entity == "orders"
        assert rels[0].to_entity == "customers"

    def test_export_roundtrip(self) -> None:
        from dashboardmd.connectors import MetabaseConnector
        c = MetabaseConnector(_metabase_metadata())
        exported = c.export()
        assert "tables" in exported
        assert len(exported["tables"]) == 2

    def test_register_into_analyst(self) -> None:
        from dashboardmd.analyst import Analyst
        from dashboardmd.connectors import MetabaseConnector
        c = MetabaseConnector(_metabase_metadata())
        analyst = Analyst()
        analyst.use(c)
        assert "metabase" in analyst.connectors
        assert "orders" in analyst.entities
        assert "customers" in analyst.entities


# ---------------------------------------------------------------------------
# LookMLConnector tests
# ---------------------------------------------------------------------------


class TestLookMLConnector:
    """LookMLConnector wraps from_lookml/to_lookml as a Connector."""

    def test_name(self) -> None:
        from dashboardmd.connectors import LookMLConnector
        c = LookMLConnector(_lookml_model())
        assert c.name() == "lookml"

    def test_entities_parsed(self) -> None:
        from dashboardmd.connectors import LookMLConnector
        c = LookMLConnector(_lookml_model())
        entities = c.entities()
        assert len(entities) == 1
        assert entities[0].name == "orders"
        assert len(entities[0].dimensions) == 3
        assert len(entities[0].measures) == 2

    def test_export_roundtrip(self) -> None:
        from dashboardmd.connectors import LookMLConnector
        c = LookMLConnector(_lookml_model())
        exported = c.export()
        assert "views" in exported
        assert len(exported["views"]) == 1

    def test_register_into_analyst(self) -> None:
        from dashboardmd.analyst import Analyst
        from dashboardmd.connectors import LookMLConnector
        c = LookMLConnector(_lookml_model())
        analyst = Analyst()
        analyst.use(c)
        assert "lookml" in analyst.connectors
        assert "orders" in analyst.entities


# ---------------------------------------------------------------------------
# CubeConnector tests
# ---------------------------------------------------------------------------


class TestCubeConnector:
    """CubeConnector wraps from_cube/to_cube_schema as a Connector."""

    def test_name(self) -> None:
        from dashboardmd.connectors import CubeConnector
        c = CubeConnector(_cube_schema())
        assert c.name() == "cube"

    def test_entities_parsed(self) -> None:
        from dashboardmd.connectors import CubeConnector
        c = CubeConnector(_cube_schema())
        entities = c.entities()
        assert len(entities) == 1
        assert entities[0].name == "orders"

    def test_export_roundtrip(self) -> None:
        from dashboardmd.connectors import CubeConnector
        c = CubeConnector(_cube_schema())
        exported = c.export()
        assert "cubes" in exported

    def test_register_into_analyst(self) -> None:
        from dashboardmd.analyst import Analyst
        from dashboardmd.connectors import CubeConnector
        c = CubeConnector(_cube_schema())
        analyst = Analyst()
        analyst.use(c)
        assert "cube" in analyst.connectors
        assert "orders" in analyst.entities


# ---------------------------------------------------------------------------
# PowerBIConnector tests
# ---------------------------------------------------------------------------


class TestPowerBIConnector:
    """PowerBIConnector wraps from_powerbi/to_powerbi as a Connector."""

    def test_name(self) -> None:
        from dashboardmd.connectors import PowerBIConnector
        c = PowerBIConnector(_powerbi_model())
        assert c.name() == "powerbi"

    def test_entities_parsed(self) -> None:
        from dashboardmd.connectors import PowerBIConnector
        c = PowerBIConnector(_powerbi_model())
        entities = c.entities()
        assert len(entities) == 1
        assert entities[0].name == "orders"
        assert len(entities[0].measures) == 2

    def test_export_roundtrip(self) -> None:
        from dashboardmd.connectors import PowerBIConnector
        c = PowerBIConnector(_powerbi_model())
        exported = c.export()
        assert "tables" in exported
        assert "relationships" in exported

    def test_register_into_analyst(self) -> None:
        from dashboardmd.analyst import Analyst
        from dashboardmd.connectors import PowerBIConnector
        c = PowerBIConnector(_powerbi_model())
        analyst = Analyst()
        analyst.use(c)
        assert "powerbi" in analyst.connectors
        assert "orders" in analyst.entities


# ---------------------------------------------------------------------------
# Composability tests
# ---------------------------------------------------------------------------


class TestConnectorComposability:
    """Multiple connectors should compose into one Analyst."""

    def test_multiple_connectors(self) -> None:
        """Multiple connectors can be installed into one Analyst."""
        from dashboardmd.analyst import Analyst
        from dashboardmd.connectors import CubeConnector, MetabaseConnector

        analyst = Analyst()
        analyst.use(MetabaseConnector(_metabase_metadata()))
        analyst.use(CubeConnector(_cube_schema()))

        assert len(analyst.connectors) == 2
        assert "metabase" in analyst.connectors
        assert "cube" in analyst.connectors

    def test_cross_connector_relationship(self) -> None:
        """add_relationship() should link entities from different connectors."""
        from dashboardmd.analyst import Analyst
        from dashboardmd.connectors import CubeConnector, MetabaseConnector

        analyst = Analyst()
        analyst.use(MetabaseConnector(_metabase_metadata()))
        analyst.use(CubeConnector({"cubes": [{
            "name": "events",
            "dimensions": [{"name": "order_id", "type": "number"}],
            "measures": [{"name": "count", "type": "count"}],
        }]}))

        analyst.add_relationship(Relationship(
            "orders", "events", on=("id", "order_id"), type="one_to_many"
        ))

        assert len(analyst.relationships) >= 2  # metabase FK + our cross-connector

    def test_connectors_property(self) -> None:
        """analyst.connectors should list all installed connectors."""
        from dashboardmd.analyst import Analyst
        from dashboardmd.connectors import MetabaseConnector

        analyst = Analyst()
        assert analyst.connectors == {}
        analyst.use(MetabaseConnector(_metabase_metadata()))
        assert list(analyst.connectors.keys()) == ["metabase"]
