"""Tests for the Engine: DuckDB-based query execution."""

from __future__ import annotations

from pathlib import Path

import pytest


class TestEngine:
    """Engine creates a DuckDB connection, registers sources, and executes queries."""

    def test_engine_creates_connection(self) -> None:
        """Engine should initialize with a working DuckDB connection."""
        from dashboardmd.engine import Engine

        engine = Engine(entities=[], relationships=[])
        assert engine.conn is not None

    def test_register_csv_entity(self, orders_csv: Path) -> None:
        """Engine should register CSV-backed entities as queryable views."""
        from dashboardmd.engine import Engine
        from dashboardmd.model import Dimension, Entity, Measure

        orders = Entity(
            name="orders",
            source=orders_csv,  # or Source.csv(...)
            dimensions=[Dimension("id", type="number", primary_key=True)],
            measures=[Measure("count", type="count")],
        )
        engine = Engine(entities=[orders], relationships=[])
        result = engine.conn.execute("SELECT count(*) FROM orders").fetchone()
        assert result is not None
        assert result[0] == 20

    def test_execute_simple_query(self, orders_csv: Path) -> None:
        """Engine.execute() should run a Query and return results."""
        from dashboardmd.engine import Engine
        from dashboardmd.model import Dimension, Entity, Measure
        from dashboardmd.query import Query

        orders = Entity(
            name="orders",
            source=orders_csv,
            dimensions=[
                Dimension("id", type="number", primary_key=True),
                Dimension("status", type="string"),
            ],
            measures=[Measure("count", type="count")],
        )
        engine = Engine(entities=[orders], relationships=[])
        q = Query(measures=["orders.count"])
        result = engine.execute(q)

        assert result is not None
        # Result should have at least one row with a count value

    def test_execute_with_groupby(self, orders_csv: Path) -> None:
        """Execute a query with GROUP BY dimension."""
        from dashboardmd.engine import Engine
        from dashboardmd.model import Dimension, Entity, Measure
        from dashboardmd.query import Query

        orders = Entity(
            name="orders",
            source=orders_csv,
            dimensions=[
                Dimension("id", type="number", primary_key=True),
                Dimension("status", type="string"),
            ],
            measures=[Measure("revenue", type="sum", sql="amount")],
        )
        engine = Engine(entities=[orders], relationships=[])
        q = Query(
            measures=["orders.revenue"],
            dimensions=["orders.status"],
        )
        result = engine.execute(q)

        # Should have 3 rows (completed, pending, cancelled)
        assert result is not None

    def test_execute_cross_entity_join(
        self, orders_csv: Path, customers_csv: Path
    ) -> None:
        """Execute a query that joins two entities."""
        from dashboardmd.engine import Engine
        from dashboardmd.model import Dimension, Entity, Measure, Relationship
        from dashboardmd.query import Query

        orders = Entity(
            name="orders",
            source=orders_csv,
            dimensions=[
                Dimension("id", type="number", primary_key=True),
                Dimension("customer_id", type="number"),
            ],
            measures=[Measure("revenue", type="sum", sql="amount")],
        )
        customers = Entity(
            name="customers",
            source=customers_csv,
            dimensions=[
                Dimension("id", type="number", primary_key=True),
                Dimension("segment", type="string"),
            ],
        )
        rels = [
            Relationship("orders", "customers", on=("customer_id", "id"), type="many_to_one"),
        ]
        engine = Engine(entities=[orders, customers], relationships=rels)
        q = Query(
            measures=["orders.revenue"],
            dimensions=["customers.segment"],
        )
        result = engine.execute(q)
        assert result is not None

    def test_execute_with_filter(self, orders_csv: Path) -> None:
        """Execute a query with a filter applied."""
        from dashboardmd.engine import Engine
        from dashboardmd.model import Dimension, Entity, Measure
        from dashboardmd.query import Query

        orders = Entity(
            name="orders",
            source=orders_csv,
            dimensions=[
                Dimension("id", type="number", primary_key=True),
                Dimension("status", type="string"),
            ],
            measures=[Measure("count", type="count")],
        )
        engine = Engine(entities=[orders], relationships=[])
        q = Query(
            measures=["orders.count"],
            filters=[("orders.status", "equals", "completed")],
        )
        result = engine.execute(q)
        assert result is not None

    def test_multiple_entities_registered(
        self, orders_csv: Path, customers_csv: Path, products_csv: Path
    ) -> None:
        """Engine should register all provided entities."""
        from dashboardmd.engine import Engine
        from dashboardmd.model import Dimension, Entity

        entities = [
            Entity(
                name="orders",
                source=orders_csv,
                dimensions=[Dimension("id", type="number", primary_key=True)],
            ),
            Entity(
                name="customers",
                source=customers_csv,
                dimensions=[Dimension("id", type="number", primary_key=True)],
            ),
            Entity(
                name="products",
                source=products_csv,
                dimensions=[Dimension("id", type="number", primary_key=True)],
            ),
        ]
        engine = Engine(entities=entities, relationships=[])

        for name, expected_count in [("orders", 20), ("customers", 8), ("products", 5)]:
            result = engine.conn.execute(f"SELECT count(*) FROM {name}").fetchone()
            assert result is not None
            assert result[0] == expected_count

    def test_engine_cleanup(self) -> None:
        """Engine should close the DuckDB connection when done."""
        from dashboardmd.engine import Engine

        engine = Engine(entities=[], relationships=[])
        engine.close()
        # After close, executing should raise an error
        with pytest.raises(Exception):
            engine.conn.execute("SELECT 1")
