"""Tests for the Query Layer: Query builder and SQL generation."""

from __future__ import annotations

import pytest


# ---------------------------------------------------------------------------
# Query Object
# ---------------------------------------------------------------------------


class TestQuery:
    """Query is a semantic request: measures + dimensions + filters."""

    def test_create_simple_query(self) -> None:
        """Create a query with one measure and one dimension."""
        from dashboardmd.query import Query

        q = Query(
            measures=["orders.revenue"],
            dimensions=["orders.status"],
        )
        assert q.measures == ["orders.revenue"]
        assert q.dimensions == ["orders.status"]

    def test_query_with_multiple_measures(self) -> None:
        """Query can request multiple measures."""
        from dashboardmd.query import Query

        q = Query(
            measures=["orders.revenue", "orders.count"],
            dimensions=["orders.status"],
        )
        assert len(q.measures) == 2

    def test_query_with_filters(self) -> None:
        """Queries can include filters."""
        from dashboardmd.query import Query

        q = Query(
            measures=["orders.revenue"],
            dimensions=["orders.status"],
            filters=[("orders.status", "equals", "completed")],
        )
        assert len(q.filters) == 1

    def test_query_with_time_granularity(self) -> None:
        """Queries can specify time granularity."""
        from dashboardmd.query import Query

        q = Query(
            measures=["orders.revenue"],
            dimensions=["orders.date"],
            time_granularity="month",
        )
        assert q.time_granularity == "month"

    def test_query_with_sort(self) -> None:
        """Queries can specify sort order."""
        from dashboardmd.query import Query

        q = Query(
            measures=["orders.revenue"],
            dimensions=["orders.status"],
            sort=("orders.revenue", "desc"),
        )
        assert q.sort == ("orders.revenue", "desc")

    def test_query_with_limit(self) -> None:
        """Queries can specify a row limit."""
        from dashboardmd.query import Query

        q = Query(
            measures=["orders.revenue"],
            dimensions=["orders.status"],
            limit=10,
        )
        assert q.limit == 10

    def test_query_with_compare(self) -> None:
        """Queries can request period comparison."""
        from dashboardmd.query import Query

        q = Query(
            measures=["orders.revenue"],
            compare="previous_period",
        )
        assert q.compare == "previous_period"

    def test_measures_only_query(self) -> None:
        """A query with measures but no dimensions returns a single scalar row."""
        from dashboardmd.query import Query

        q = Query(measures=["orders.revenue"])
        assert q.dimensions == [] or q.dimensions is None


# ---------------------------------------------------------------------------
# SQL Generation
# ---------------------------------------------------------------------------


class TestQueryBuilder:
    """QueryBuilder translates semantic queries into SQL strings."""

    def test_single_entity_single_measure(self) -> None:
        """Generate SQL for a simple single-measure query."""
        from dashboardmd.model import Dimension, Entity, Measure
        from dashboardmd.query import Query, QueryBuilder

        orders = Entity(
            name="orders",
            dimensions=[Dimension("id", type="number", primary_key=True)],
            measures=[Measure("revenue", type="sum", sql="amount")],
        )
        builder = QueryBuilder(entities=[orders], relationships=[])
        q = Query(measures=["orders.revenue"])
        sql = builder.build_sql(q)

        assert "SUM" in sql.upper()
        assert "amount" in sql.lower() or "AMOUNT" in sql.upper()

    def test_measure_with_dimension_groupby(self) -> None:
        """SQL should include GROUP BY when dimensions are present."""
        from dashboardmd.model import Dimension, Entity, Measure
        from dashboardmd.query import Query, QueryBuilder

        orders = Entity(
            name="orders",
            dimensions=[
                Dimension("id", type="number", primary_key=True),
                Dimension("status", type="string"),
            ],
            measures=[Measure("revenue", type="sum", sql="amount")],
        )
        builder = QueryBuilder(entities=[orders], relationships=[])
        q = Query(measures=["orders.revenue"], dimensions=["orders.status"])
        sql = builder.build_sql(q)

        assert "GROUP BY" in sql.upper()
        assert "status" in sql.lower()

    def test_count_measure_sql(self) -> None:
        """COUNT measure should generate COUNT(*) or COUNT(col)."""
        from dashboardmd.model import Dimension, Entity, Measure
        from dashboardmd.query import Query, QueryBuilder

        orders = Entity(
            name="orders",
            dimensions=[Dimension("id", type="number", primary_key=True)],
            measures=[Measure("order_count", type="count")],
        )
        builder = QueryBuilder(entities=[orders], relationships=[])
        q = Query(measures=["orders.order_count"])
        sql = builder.build_sql(q)

        assert "COUNT" in sql.upper()

    def test_count_distinct_measure_sql(self) -> None:
        """COUNT_DISTINCT should generate COUNT(DISTINCT col)."""
        from dashboardmd.model import Dimension, Entity, Measure
        from dashboardmd.query import Query, QueryBuilder

        orders = Entity(
            name="orders",
            dimensions=[Dimension("id", type="number", primary_key=True)],
            measures=[Measure("unique_customers", type="count_distinct", sql="customer_id")],
        )
        builder = QueryBuilder(entities=[orders], relationships=[])
        q = Query(measures=["orders.unique_customers"])
        sql = builder.build_sql(q)

        assert "DISTINCT" in sql.upper()

    def test_avg_measure_sql(self) -> None:
        """AVG measure should generate AVG(col)."""
        from dashboardmd.model import Dimension, Entity, Measure
        from dashboardmd.query import Query, QueryBuilder

        orders = Entity(
            name="orders",
            dimensions=[Dimension("id", type="number", primary_key=True)],
            measures=[Measure("avg_amount", type="avg", sql="amount")],
        )
        builder = QueryBuilder(entities=[orders], relationships=[])
        q = Query(measures=["orders.avg_amount"])
        sql = builder.build_sql(q)

        assert "AVG" in sql.upper()

    def test_filter_generates_where_clause(self) -> None:
        """Filters should produce a WHERE clause in the SQL."""
        from dashboardmd.model import Dimension, Entity, Measure
        from dashboardmd.query import Query, QueryBuilder

        orders = Entity(
            name="orders",
            dimensions=[
                Dimension("id", type="number", primary_key=True),
                Dimension("status", type="string"),
            ],
            measures=[Measure("count", type="count")],
        )
        builder = QueryBuilder(entities=[orders], relationships=[])
        q = Query(
            measures=["orders.count"],
            filters=[("orders.status", "equals", "completed")],
        )
        sql = builder.build_sql(q)

        assert "WHERE" in sql.upper()
        assert "completed" in sql.lower()

    def test_sort_generates_order_by(self) -> None:
        """Sort should produce an ORDER BY clause."""
        from dashboardmd.model import Dimension, Entity, Measure
        from dashboardmd.query import Query, QueryBuilder

        orders = Entity(
            name="orders",
            dimensions=[
                Dimension("id", type="number", primary_key=True),
                Dimension("status", type="string"),
            ],
            measures=[Measure("revenue", type="sum", sql="amount")],
        )
        builder = QueryBuilder(entities=[orders], relationships=[])
        q = Query(
            measures=["orders.revenue"],
            dimensions=["orders.status"],
            sort=("orders.revenue", "desc"),
        )
        sql = builder.build_sql(q)

        assert "ORDER BY" in sql.upper()

    def test_limit_generates_limit_clause(self) -> None:
        """Limit should produce a LIMIT clause."""
        from dashboardmd.model import Dimension, Entity, Measure
        from dashboardmd.query import Query, QueryBuilder

        orders = Entity(
            name="orders",
            dimensions=[Dimension("id", type="number", primary_key=True)],
            measures=[Measure("count", type="count")],
        )
        builder = QueryBuilder(entities=[orders], relationships=[])
        q = Query(measures=["orders.count"], limit=5)
        sql = builder.build_sql(q)

        assert "LIMIT" in sql.upper()
        assert "5" in sql


# ---------------------------------------------------------------------------
# Join Resolution
# ---------------------------------------------------------------------------


class TestJoinResolver:
    """JoinResolver finds join paths between entities from the relationship graph."""

    def test_direct_join(self) -> None:
        """Resolve a direct relationship between two entities."""
        from dashboardmd.model import Relationship
        from dashboardmd.query import JoinResolver

        rels = [
            Relationship("orders", "customers", on=("customer_id", "id"), type="many_to_one"),
        ]
        resolver = JoinResolver(rels)
        joins = resolver.resolve({"orders", "customers"})

        assert len(joins) >= 1

    def test_no_join_for_single_entity(self) -> None:
        """A query on a single entity requires no joins."""
        from dashboardmd.model import Relationship
        from dashboardmd.query import JoinResolver

        rels = [
            Relationship("orders", "customers", on=("customer_id", "id"), type="many_to_one"),
        ]
        resolver = JoinResolver(rels)
        joins = resolver.resolve({"orders"})

        assert len(joins) == 0

    def test_transitive_join(self) -> None:
        """Resolve a join path through intermediate entities (A → B → C)."""
        from dashboardmd.model import Relationship
        from dashboardmd.query import JoinResolver

        rels = [
            Relationship("orders", "customers", on=("customer_id", "id"), type="many_to_one"),
            Relationship("customers", "regions", on=("region_id", "id"), type="many_to_one"),
        ]
        resolver = JoinResolver(rels)
        joins = resolver.resolve({"orders", "regions"})

        # Should resolve: orders → customers → regions
        assert len(joins) >= 2

    def test_multiple_relationships(self) -> None:
        """Handle multiple relationships from the same entity."""
        from dashboardmd.model import Relationship
        from dashboardmd.query import JoinResolver

        rels = [
            Relationship("orders", "customers", on=("customer_id", "id"), type="many_to_one"),
            Relationship("orders", "products", on=("product_id", "id"), type="many_to_one"),
        ]
        resolver = JoinResolver(rels)
        joins = resolver.resolve({"orders", "customers", "products"})

        assert len(joins) >= 2

    def test_unrelated_entities_raises_error(self) -> None:
        """Requesting a join between unrelated entities should fail."""
        from dashboardmd.model import Relationship
        from dashboardmd.query import JoinResolver

        rels = [
            Relationship("orders", "customers", on=("customer_id", "id"), type="many_to_one"),
        ]
        resolver = JoinResolver(rels)

        with pytest.raises(Exception):
            resolver.resolve({"orders", "unrelated_table"})

    def test_cross_entity_sql_generation(self) -> None:
        """SQL for a cross-entity query should include JOIN clauses."""
        from dashboardmd.model import Dimension, Entity, Measure, Relationship
        from dashboardmd.query import Query, QueryBuilder

        orders = Entity(
            name="orders",
            dimensions=[
                Dimension("id", type="number", primary_key=True),
                Dimension("customer_id", type="number"),
            ],
            measures=[Measure("revenue", type="sum", sql="amount")],
        )
        customers = Entity(
            name="customers",
            dimensions=[
                Dimension("id", type="number", primary_key=True),
                Dimension("segment", type="string"),
            ],
        )
        rels = [
            Relationship("orders", "customers", on=("customer_id", "id"), type="many_to_one"),
        ]
        builder = QueryBuilder(entities=[orders, customers], relationships=rels)
        q = Query(
            measures=["orders.revenue"],
            dimensions=["customers.segment"],
        )
        sql = builder.build_sql(q)

        assert "JOIN" in sql.upper()
        assert "customer_id" in sql.lower()
