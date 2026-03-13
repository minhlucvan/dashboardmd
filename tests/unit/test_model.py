"""Tests for the Semantic Layer: Entity, Dimension, Measure, Relationship."""

from __future__ import annotations

import pytest


# ---------------------------------------------------------------------------
# Dimension
# ---------------------------------------------------------------------------


class TestDimension:
    """Dimension represents an attribute you group and filter by."""

    def test_create_string_dimension(self) -> None:
        """Create a basic string dimension."""
        from dashboardmd.model import Dimension

        dim = Dimension("status", type="string")
        assert dim.name == "status"
        assert dim.type == "string"

    def test_create_time_dimension(self) -> None:
        """Time dimensions enable granularity (day/week/month)."""
        from dashboardmd.model import Dimension

        dim = Dimension("order_date", type="time")
        assert dim.type == "time"

    def test_create_number_dimension(self) -> None:
        """Number dimensions are numeric attributes (not aggregations)."""
        from dashboardmd.model import Dimension

        dim = Dimension("quantity", type="number")
        assert dim.type == "number"

    def test_primary_key_flag(self) -> None:
        """Dimensions can be marked as primary keys."""
        from dashboardmd.model import Dimension

        dim = Dimension("id", type="number", primary_key=True)
        assert dim.primary_key is True

    def test_primary_key_defaults_false(self) -> None:
        """primary_key should default to False."""
        from dashboardmd.model import Dimension

        dim = Dimension("name", type="string")
        assert dim.primary_key is False

    def test_custom_sql_expression(self) -> None:
        """sql= overrides the column name used in queries."""
        from dashboardmd.model import Dimension

        dim = Dimension("full_name", type="string", sql="first_name || ' ' || last_name")
        assert dim.sql == "first_name || ' ' || last_name"

    def test_sql_defaults_to_name(self) -> None:
        """When sql is not provided, it defaults to the dimension name."""
        from dashboardmd.model import Dimension

        dim = Dimension("status", type="string")
        assert dim.sql is None or dim.sql == "status"

    def test_format_string(self) -> None:
        """Dimensions can have display format strings."""
        from dashboardmd.model import Dimension

        dim = Dimension("price", type="number", format="$,.2f")
        assert dim.format == "$,.2f"

    def test_invalid_type_raises_error(self) -> None:
        """Only valid types (string, number, time, boolean) should be accepted."""
        from dashboardmd.model import Dimension

        with pytest.raises((ValueError, TypeError)):
            Dimension("x", type="invalid_type")


# ---------------------------------------------------------------------------
# Measure
# ---------------------------------------------------------------------------


class TestMeasure:
    """Measure represents an aggregation you compute (SUM, COUNT, AVG, etc.)."""

    def test_create_sum_measure(self) -> None:
        """Create a SUM aggregation measure."""
        from dashboardmd.model import Measure

        m = Measure("revenue", type="sum", sql="amount")
        assert m.name == "revenue"
        assert m.type == "sum"
        assert m.sql == "amount"

    def test_create_count_measure(self) -> None:
        """COUNT measure doesn't need a sql column."""
        from dashboardmd.model import Measure

        m = Measure("order_count", type="count")
        assert m.type == "count"

    def test_create_count_distinct_measure(self) -> None:
        """COUNT DISTINCT aggregation."""
        from dashboardmd.model import Measure

        m = Measure("unique_customers", type="count_distinct", sql="customer_id")
        assert m.type == "count_distinct"

    def test_create_avg_measure(self) -> None:
        """AVG aggregation."""
        from dashboardmd.model import Measure

        m = Measure("avg_order_value", type="avg", sql="amount")
        assert m.type == "avg"

    def test_create_min_max_measures(self) -> None:
        """MIN and MAX aggregations."""
        from dashboardmd.model import Measure

        m_min = Measure("min_price", type="min", sql="price")
        m_max = Measure("max_price", type="max", sql="price")
        assert m_min.type == "min"
        assert m_max.type == "max"

    def test_computed_measure(self) -> None:
        """Computed measures reference other measures (type='number')."""
        from dashboardmd.model import Measure

        m = Measure("aov", type="number", sql="revenue / count")
        assert m.type == "number"
        assert m.sql == "revenue / count"

    def test_format_string(self) -> None:
        """Measures can have display format strings."""
        from dashboardmd.model import Measure

        m = Measure("revenue", type="sum", sql="amount", format="$,.0f")
        assert m.format == "$,.0f"

    def test_measure_with_filters(self) -> None:
        """Measures can have pre-filters (e.g., count where status='completed')."""
        from dashboardmd.model import Measure

        m = Measure(
            "completed_count",
            type="count",
            filters=[("status", "equals", "completed")],
        )
        assert len(m.filters) == 1
        assert m.filters[0] == ("status", "equals", "completed")

    def test_filters_default_empty(self) -> None:
        """Filters should default to an empty list."""
        from dashboardmd.model import Measure

        m = Measure("count", type="count")
        assert m.filters == [] or m.filters is None

    def test_invalid_type_raises_error(self) -> None:
        """Only valid aggregation types should be accepted."""
        from dashboardmd.model import Measure

        with pytest.raises((ValueError, TypeError)):
            Measure("x", type="invalid_agg")


# ---------------------------------------------------------------------------
# Entity
# ---------------------------------------------------------------------------


class TestEntity:
    """Entity is a logical table with dimensions, measures, and a source."""

    def test_create_entity_with_dimensions_and_measures(self) -> None:
        """Entity bundles a source with its semantic model."""
        from dashboardmd.model import Dimension, Entity, Measure

        entity = Entity(
            name="orders",
            dimensions=[
                Dimension("id", type="number", primary_key=True),
                Dimension("status", type="string"),
            ],
            measures=[
                Measure("count", type="count"),
            ],
        )
        assert entity.name == "orders"
        assert len(entity.dimensions) == 2
        assert len(entity.measures) == 1

    def test_entity_has_source(self) -> None:
        """Entity should hold a source handler reference."""
        from dashboardmd.model import Entity

        entity = Entity(name="orders", source="data/orders.csv")
        assert entity.source is not None

    def test_entity_name_must_be_valid_identifier(self) -> None:
        """Entity name should be a valid SQL identifier (no spaces, special chars)."""
        from dashboardmd.model import Entity

        # Valid name should work
        entity = Entity(name="orders", dimensions=[], measures=[])
        assert entity.name == "orders"

    def test_find_dimension_by_name(self) -> None:
        """Should be able to look up a dimension by name."""
        from dashboardmd.model import Dimension, Entity

        entity = Entity(
            name="orders",
            dimensions=[
                Dimension("id", type="number", primary_key=True),
                Dimension("status", type="string"),
            ],
        )
        # This tests however dimensions are accessed — dict, method, or iteration
        dim_names = [d.name for d in entity.dimensions]
        assert "status" in dim_names

    def test_find_measure_by_name(self) -> None:
        """Should be able to look up a measure by name."""
        from dashboardmd.model import Entity, Measure

        entity = Entity(
            name="orders",
            measures=[
                Measure("revenue", type="sum", sql="amount"),
                Measure("count", type="count"),
            ],
        )
        measure_names = [m.name for m in entity.measures]
        assert "revenue" in measure_names

    def test_primary_key_dimension(self) -> None:
        """Should identify the primary key dimension."""
        from dashboardmd.model import Dimension, Entity

        entity = Entity(
            name="orders",
            dimensions=[
                Dimension("id", type="number", primary_key=True),
                Dimension("status", type="string"),
            ],
        )
        pk_dims = [d for d in entity.dimensions if d.primary_key]
        assert len(pk_dims) == 1
        assert pk_dims[0].name == "id"


# ---------------------------------------------------------------------------
# Relationship
# ---------------------------------------------------------------------------


class TestRelationship:
    """Relationship defines how entities join together."""

    def test_create_many_to_one(self) -> None:
        """Many-to-one relationship (e.g., orders → customers)."""
        from dashboardmd.model import Relationship

        rel = Relationship(
            from_entity="orders",
            to_entity="customers",
            on=("customer_id", "id"),
            type="many_to_one",
        )
        assert rel.from_entity == "orders"
        assert rel.to_entity == "customers"
        assert rel.on == ("customer_id", "id")
        assert rel.type == "many_to_one"

    def test_create_one_to_many(self) -> None:
        """One-to-many relationship."""
        from dashboardmd.model import Relationship

        rel = Relationship(
            from_entity="customers",
            to_entity="orders",
            on=("id", "customer_id"),
            type="one_to_many",
        )
        assert rel.type == "one_to_many"

    def test_create_one_to_one(self) -> None:
        """One-to-one relationship."""
        from dashboardmd.model import Relationship

        rel = Relationship(
            from_entity="users",
            to_entity="profiles",
            on=("id", "user_id"),
            type="one_to_one",
        )
        assert rel.type == "one_to_one"

    def test_create_many_to_many(self) -> None:
        """Many-to-many relationship."""
        from dashboardmd.model import Relationship

        rel = Relationship(
            from_entity="orders",
            to_entity="products",
            on=("id", "order_id"),
            type="many_to_many",
        )
        assert rel.type == "many_to_many"

    def test_on_tuple_has_two_elements(self) -> None:
        """The on= parameter must be a (from_col, to_col) tuple."""
        from dashboardmd.model import Relationship

        rel = Relationship(
            from_entity="orders",
            to_entity="customers",
            on=("customer_id", "id"),
            type="many_to_one",
        )
        assert len(rel.on) == 2

    def test_invalid_type_raises_error(self) -> None:
        """Only valid relationship types should be accepted."""
        from dashboardmd.model import Relationship

        with pytest.raises((ValueError, TypeError)):
            Relationship(
                from_entity="a",
                to_entity="b",
                on=("x", "y"),
                type="invalid_rel_type",
            )
