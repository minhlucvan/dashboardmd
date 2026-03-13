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
            {
                "name": "customers",
                "sql_table_name": "public.customers",
                "dimensions": [
                    {"name": "id", "type": "number", "primary_key": True},
                    {"name": "name", "type": "string"},
                ],
                "measures": [],
            },
        ],
        "explores": [{
            "name": "orders",
            "joins": [{
                "name": "customers",
                "sql_on": "${orders.customer_id} = ${customers.id}",
                "relationship": "many_to_one",
            }],
        }],
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
                "joins": {
                    "customers": {
                        "sql": "{CUBE}.customer_id = {customers}.id",
                        "relationship": "belongsTo",
                    },
                },
            },
            {
                "name": "customers",
                "sql": "SELECT * FROM customers",
                "dimensions": [
                    {"name": "id", "type": "number", "primaryKey": True},
                    {"name": "name", "type": "string"},
                ],
                "measures": [{"name": "count", "type": "count"}],
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
                    {"name": "customer_id", "dataType": "int64"},
                ],
                "measures": [
                    {"name": "revenue", "expression": "SUM('orders'[amount])"},
                    {"name": "count", "expression": "COUNTROWS('orders')"},
                ],
            },
            {
                "name": "customers",
                "columns": [
                    {"name": "id", "dataType": "int64", "isKey": True},
                    {"name": "name", "dataType": "string"},
                ],
                "measures": [],
            },
        ],
        "relationships": [{
            "fromTable": "orders",
            "toTable": "customers",
            "fromColumn": "customer_id",
            "toColumn": "id",
            "cardinality": "manyToOne",
        }],
    }


# ---------------------------------------------------------------------------
# Connector base class tests
# ---------------------------------------------------------------------------


class TestConnectorBase:
    """Test the Connector abstract base class."""

    def test_cannot_instantiate_abstract(self) -> None:
        with pytest.raises(TypeError):
            Connector()  # type: ignore[abstract]

    def test_custom_connector_minimal(self) -> None:
        class MinimalConnector(Connector):
            def name(self) -> str:
                return "minimal"

            def sources(self) -> dict[str, SourceHandler]:
                return {}

            def entities(self) -> list[Entity]:
                return [Entity("test_table", dimensions=[
                    Dimension("id", type="number", primary_key=True),
                ])]

        c = MinimalConnector()
        assert c.name() == "minimal"
        assert len(c.entities()) == 1
        assert c.relationships() == []
        assert c.widgets() == []
        assert c.available_dashboards() == []

    def test_custom_connector_with_relationships(self) -> None:
        class RelConnector(Connector):
            def name(self) -> str:
                return "rel_test"

            def sources(self) -> dict[str, SourceHandler]:
                return {}

            def entities(self) -> list[Entity]:
                return [
                    Entity("a", dimensions=[Dimension("id", type="number", primary_key=True)]),
                    Entity("b", dimensions=[Dimension("a_id", type="number")]),
                ]

            def relationships(self) -> list[Relationship]:
                return [Relationship("b", "a", on=("a_id", "id"), type="many_to_one")]

        c = RelConnector()
        assert len(c.relationships()) == 1
        assert c.relationships()[0].from_entity == "b"

    def test_custom_connector_with_widgets(self) -> None:
        class WidgetConnector(Connector):
            def name(self) -> str:
                return "widget_test"

            def sources(self) -> dict[str, SourceHandler]:
                return {}

            def entities(self) -> list[Entity]:
                return [Entity("events")]

            def widgets(self) -> list[DashboardWidget]:
                return [DashboardWidget(
                    name="Overview",
                    title="Event Overview",
                    requires=["events"],
                )]

        c = WidgetConnector()
        assert len(c.widgets()) == 1
        assert c.widgets()[0].name == "Overview"
        assert c.widgets()[0].requires == ["events"]

    def test_dashboard_widget_dataclass(self) -> None:
        w = DashboardWidget(
            name="Overview",
            title="Test Overview",
            description="A test widget",
            requires=["orders", "customers"],
        )
        assert w.name == "Overview"
        assert w.title == "Test Overview"
        assert w.description == "A test widget"
        assert w.requires == ["orders", "customers"]

    def test_dashboard_widget_optional_fields(self) -> None:
        w = DashboardWidget(name="Simple", title="Simple Widget")
        assert w.description is None or w.description == ""
        assert w.requires == [] or w.requires is None


# ---------------------------------------------------------------------------
# MetabaseConnector tests
# ---------------------------------------------------------------------------


class TestMetabaseConnector:
    """MetabaseConnector wraps from_metabase as a Connector."""

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

    def test_entity_dimensions(self) -> None:
        from dashboardmd.connectors import MetabaseConnector
        c = MetabaseConnector(_metabase_metadata())
        orders = next(e for e in c.entities() if e.name == "orders")
        assert len(orders.dimensions) == 4
        dim_names = [d.name for d in orders.dimensions]
        assert "id" in dim_names
        assert "customer_id" in dim_names

    def test_entity_measures(self) -> None:
        from dashboardmd.connectors import MetabaseConnector
        c = MetabaseConnector(_metabase_metadata())
        orders = next(e for e in c.entities() if e.name == "orders")
        assert len(orders.measures) == 2
        measure_names = [m.name for m in orders.measures]
        assert "revenue" in measure_names
        assert "count" in measure_names

    def test_relationships_parsed(self) -> None:
        from dashboardmd.connectors import MetabaseConnector
        c = MetabaseConnector(_metabase_metadata())
        rels = c.relationships()
        assert len(rels) == 1
        assert rels[0].from_entity == "orders"
        assert rels[0].to_entity == "customers"
        assert rels[0].on == ("customer_id", "id")

    def test_sources_empty_by_default(self) -> None:
        from dashboardmd.connectors import MetabaseConnector
        c = MetabaseConnector(_metabase_metadata())
        assert c.sources() == {}

    def test_no_export_method(self) -> None:
        from dashboardmd.connectors import MetabaseConnector
        c = MetabaseConnector(_metabase_metadata())
        assert not hasattr(c, "export")

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
    """LookMLConnector wraps from_lookml as a Connector."""

    def test_name(self) -> None:
        from dashboardmd.connectors import LookMLConnector
        c = LookMLConnector(_lookml_model())
        assert c.name() == "lookml"

    def test_entities_parsed(self) -> None:
        from dashboardmd.connectors import LookMLConnector
        c = LookMLConnector(_lookml_model())
        entities = c.entities()
        assert len(entities) == 2
        names = [e.name for e in entities]
        assert "orders" in names
        assert "customers" in names

    def test_entity_dimensions(self) -> None:
        from dashboardmd.connectors import LookMLConnector
        c = LookMLConnector(_lookml_model())
        orders = next(e for e in c.entities() if e.name == "orders")
        assert len(orders.dimensions) == 3
        dims = {d.name: d for d in orders.dimensions}
        assert dims["id"].primary_key is True
        assert dims["date"].type == "time"

    def test_entity_measures(self) -> None:
        from dashboardmd.connectors import LookMLConnector
        c = LookMLConnector(_lookml_model())
        orders = next(e for e in c.entities() if e.name == "orders")
        assert len(orders.measures) == 2

    def test_relationships_from_explores(self) -> None:
        from dashboardmd.connectors import LookMLConnector
        c = LookMLConnector(_lookml_model())
        rels = c.relationships()
        assert len(rels) == 1
        assert rels[0].from_entity == "orders"
        assert rels[0].to_entity == "customers"
        assert rels[0].type == "many_to_one"

    def test_no_export_method(self) -> None:
        from dashboardmd.connectors import LookMLConnector
        c = LookMLConnector(_lookml_model())
        assert not hasattr(c, "export")

    def test_register_into_analyst(self) -> None:
        from dashboardmd.analyst import Analyst
        from dashboardmd.connectors import LookMLConnector
        c = LookMLConnector(_lookml_model())
        analyst = Analyst()
        analyst.use(c)
        assert "lookml" in analyst.connectors
        assert "orders" in analyst.entities
        assert "customers" in analyst.entities


# ---------------------------------------------------------------------------
# CubeConnector tests
# ---------------------------------------------------------------------------


class TestCubeConnector:
    """CubeConnector wraps from_cube as a Connector."""

    def test_name(self) -> None:
        from dashboardmd.connectors import CubeConnector
        c = CubeConnector(_cube_schema())
        assert c.name() == "cube"

    def test_entities_parsed(self) -> None:
        from dashboardmd.connectors import CubeConnector
        c = CubeConnector(_cube_schema())
        entities = c.entities()
        assert len(entities) == 2
        names = [e.name for e in entities]
        assert "orders" in names
        assert "customers" in names

    def test_entity_dimensions_and_measures(self) -> None:
        from dashboardmd.connectors import CubeConnector
        c = CubeConnector(_cube_schema())
        orders = next(e for e in c.entities() if e.name == "orders")
        assert len(orders.dimensions) == 2
        assert len(orders.measures) == 2

    def test_relationships_from_joins(self) -> None:
        from dashboardmd.connectors import CubeConnector
        c = CubeConnector(_cube_schema())
        rels = c.relationships()
        assert len(rels) == 1
        assert rels[0].from_entity == "orders"
        assert rels[0].to_entity == "customers"
        assert rels[0].on == ("customer_id", "id")
        assert rels[0].type == "many_to_one"

    def test_no_export_method(self) -> None:
        from dashboardmd.connectors import CubeConnector
        c = CubeConnector(_cube_schema())
        assert not hasattr(c, "export")

    def test_register_into_analyst(self) -> None:
        from dashboardmd.analyst import Analyst
        from dashboardmd.connectors import CubeConnector
        c = CubeConnector(_cube_schema())
        analyst = Analyst()
        analyst.use(c)
        assert "cube" in analyst.connectors
        assert "orders" in analyst.entities
        assert "customers" in analyst.entities


# ---------------------------------------------------------------------------
# PowerBIConnector tests
# ---------------------------------------------------------------------------


class TestPowerBIConnector:
    """PowerBIConnector wraps from_powerbi as a Connector."""

    def test_name(self) -> None:
        from dashboardmd.connectors import PowerBIConnector
        c = PowerBIConnector(_powerbi_model())
        assert c.name() == "powerbi"

    def test_entities_parsed(self) -> None:
        from dashboardmd.connectors import PowerBIConnector
        c = PowerBIConnector(_powerbi_model())
        entities = c.entities()
        assert len(entities) == 2
        names = [e.name for e in entities]
        assert "orders" in names
        assert "customers" in names

    def test_entity_dimensions_and_measures(self) -> None:
        from dashboardmd.connectors import PowerBIConnector
        c = PowerBIConnector(_powerbi_model())
        orders = next(e for e in c.entities() if e.name == "orders")
        assert len(orders.dimensions) == 3
        assert len(orders.measures) == 2

    def test_dax_measure_types(self) -> None:
        from dashboardmd.connectors import PowerBIConnector
        c = PowerBIConnector(_powerbi_model())
        orders = next(e for e in c.entities() if e.name == "orders")
        measures = {m.name: m for m in orders.measures}
        assert measures["revenue"].type == "sum"
        assert measures["count"].type == "count"

    def test_relationships_parsed(self) -> None:
        from dashboardmd.connectors import PowerBIConnector
        c = PowerBIConnector(_powerbi_model())
        rels = c.relationships()
        assert len(rels) == 1
        assert rels[0].from_entity == "orders"
        assert rels[0].to_entity == "customers"
        assert rels[0].type == "many_to_one"

    def test_no_export_method(self) -> None:
        from dashboardmd.connectors import PowerBIConnector
        c = PowerBIConnector(_powerbi_model())
        assert not hasattr(c, "export")

    def test_register_into_analyst(self) -> None:
        from dashboardmd.analyst import Analyst
        from dashboardmd.connectors import PowerBIConnector
        c = PowerBIConnector(_powerbi_model())
        analyst = Analyst()
        analyst.use(c)
        assert "powerbi" in analyst.connectors
        assert "orders" in analyst.entities
        assert "customers" in analyst.entities


# ---------------------------------------------------------------------------
# Composability tests
# ---------------------------------------------------------------------------


class TestConnectorComposability:
    """Multiple connectors should compose into one Analyst."""

    def test_multiple_connectors(self) -> None:
        from dashboardmd.analyst import Analyst
        from dashboardmd.connectors import CubeConnector, MetabaseConnector

        analyst = Analyst()
        analyst.use(MetabaseConnector(_metabase_metadata()))
        analyst.use(CubeConnector(_cube_schema()))

        assert len(analyst.connectors) == 2
        assert "metabase" in analyst.connectors
        assert "cube" in analyst.connectors

    def test_all_four_connectors(self) -> None:
        from dashboardmd.analyst import Analyst
        from dashboardmd.connectors import CubeConnector, LookMLConnector, MetabaseConnector, PowerBIConnector

        analyst = Analyst()
        analyst.use(MetabaseConnector(_metabase_metadata()))
        analyst.use(LookMLConnector(_lookml_model()))
        analyst.use(CubeConnector(_cube_schema()))
        analyst.use(PowerBIConnector(_powerbi_model()))

        assert len(analyst.connectors) == 4

    def test_cross_connector_relationship(self) -> None:
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

        # metabase FK + cube join (none here) + our cross-connector
        assert len(analyst.relationships) >= 2

    def test_connectors_property_initially_empty(self) -> None:
        from dashboardmd.analyst import Analyst

        analyst = Analyst()
        assert analyst.connectors == {}

    def test_connectors_property_after_use(self) -> None:
        from dashboardmd.analyst import Analyst
        from dashboardmd.connectors import MetabaseConnector

        analyst = Analyst()
        analyst.use(MetabaseConnector(_metabase_metadata()))
        assert list(analyst.connectors.keys()) == ["metabase"]

    def test_use_returns_analyst_for_chaining(self) -> None:
        from dashboardmd.analyst import Analyst
        from dashboardmd.connectors import MetabaseConnector

        analyst = Analyst()
        result = analyst.use(MetabaseConnector(_metabase_metadata()))
        assert result is analyst

    def test_entities_merged_from_multiple_connectors(self) -> None:
        from dashboardmd.analyst import Analyst
        from dashboardmd.connectors import CubeConnector, MetabaseConnector

        analyst = Analyst()
        analyst.use(MetabaseConnector(_metabase_metadata()))
        analyst.use(CubeConnector({"cubes": [{
            "name": "events",
            "dimensions": [{"name": "id", "type": "number"}],
            "measures": [{"name": "count", "type": "count"}],
        }]}))

        assert "orders" in analyst.entities
        assert "customers" in analyst.entities
        assert "events" in analyst.entities


# ---------------------------------------------------------------------------
# Connector imports
# ---------------------------------------------------------------------------


class TestConnectorImports:
    """All connectors should be importable from expected locations."""

    def test_import_from_connectors_package(self) -> None:
        from dashboardmd.connectors import CubeConnector, LookMLConnector, MetabaseConnector, PowerBIConnector

        assert MetabaseConnector is not None
        assert LookMLConnector is not None
        assert CubeConnector is not None
        assert PowerBIConnector is not None

    def test_import_from_top_level(self) -> None:
        from dashboardmd import Connector, DashboardWidget

        assert Connector is not None
        assert DashboardWidget is not None
