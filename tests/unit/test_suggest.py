"""Tests for Auto-Discovery: discover(), suggest_measures(), auto_join()."""

from __future__ import annotations

from pathlib import Path

import pytest


class TestDiscover:
    """discover() scans a directory and infers entities from data files."""

    def test_discover_csv_files(self, samples_dir: Path) -> None:
        """discover() should find all CSV files and create entities."""
        from dashboardmd.suggest import discover

        entities = discover(str(samples_dir))
        entity_names = [e.name for e in entities]

        assert "orders" in entity_names
        assert "customers" in entity_names
        assert "products" in entity_names

    def test_discover_infers_dimensions(self, samples_dir: Path) -> None:
        """Discovered entities should have dimensions from column names."""
        from dashboardmd.suggest import discover

        entities = discover(str(samples_dir))
        orders = next(e for e in entities if e.name == "orders")
        dim_names = [d.name for d in orders.dimensions]

        assert "id" in dim_names
        assert "status" in dim_names
        assert "date" in dim_names

    def test_discover_infers_types(self, samples_dir: Path) -> None:
        """Discovered dimensions should have types inferred from data."""
        from dashboardmd.suggest import discover

        entities = discover(str(samples_dir))
        orders = next(e for e in entities if e.name == "orders")
        dims = {d.name: d for d in orders.dimensions}

        # 'id' should be number, 'status' should be string
        assert dims["id"].type == "number"
        assert dims["status"].type == "string"

    def test_discover_empty_directory(self, tmp_path: Path) -> None:
        """discover() on an empty directory should return an empty list."""
        from dashboardmd.suggest import discover

        entities = discover(str(tmp_path))
        assert entities == []

    def test_discover_ignores_non_data_files(self, tmp_path: Path) -> None:
        """discover() should skip non-data files (.py, .md, etc.)."""
        from dashboardmd.suggest import discover

        (tmp_path / "readme.md").write_text("# Hello")
        (tmp_path / "script.py").write_text("print('hello')")

        entities = discover(str(tmp_path))
        assert entities == []


class TestSuggestMeasures:
    """suggest_measures() detects numeric columns and proposes aggregations."""

    def test_suggest_from_numeric_columns(self) -> None:
        """Numeric columns should get SUM/AVG/MIN/MAX suggestions."""
        from dashboardmd.model import Dimension, Entity
        from dashboardmd.suggest import suggest_measures

        entity = Entity(
            name="orders",
            dimensions=[
                Dimension("id", type="number", primary_key=True),
                Dimension("amount", type="number"),
                Dimension("status", type="string"),
            ],
        )
        suggestions = suggest_measures(entity)
        measure_names = [m.name for m in suggestions]

        # Should suggest aggregation on 'amount' but not on 'id' (PK) or 'status' (string)
        assert any("amount" in name for name in measure_names)

    def test_suggest_count(self) -> None:
        """Should always suggest a COUNT measure."""
        from dashboardmd.model import Dimension, Entity
        from dashboardmd.suggest import suggest_measures

        entity = Entity(
            name="orders",
            dimensions=[Dimension("id", type="number", primary_key=True)],
        )
        suggestions = suggest_measures(entity)
        types = [m.type for m in suggestions]

        assert "count" in types

    def test_skip_primary_key_aggregation(self) -> None:
        """Should not suggest SUM/AVG on primary key columns."""
        from dashboardmd.model import Dimension, Entity
        from dashboardmd.suggest import suggest_measures

        entity = Entity(
            name="orders",
            dimensions=[
                Dimension("id", type="number", primary_key=True),
            ],
        )
        suggestions = suggest_measures(entity)
        # Should not suggest SUM(id) or AVG(id)
        sum_measures = [m for m in suggestions if m.type == "sum" and m.sql == "id"]
        assert len(sum_measures) == 0


class TestAutoJoin:
    """auto_join() detects foreign key relationships from naming patterns."""

    def test_detect_fk_by_naming_pattern(self) -> None:
        """Columns like 'customer_id' should link to entity 'customers'."""
        from dashboardmd.model import Dimension, Entity
        from dashboardmd.suggest import auto_join

        orders = Entity(
            name="orders",
            dimensions=[
                Dimension("id", type="number", primary_key=True),
                Dimension("customer_id", type="number"),
            ],
        )
        customers = Entity(
            name="customers",
            dimensions=[
                Dimension("id", type="number", primary_key=True),
            ],
        )
        relationships = auto_join([orders, customers])

        assert len(relationships) >= 1
        rel = relationships[0]
        assert rel.from_entity == "orders"
        assert rel.to_entity == "customers"

    def test_detect_multiple_fks(self) -> None:
        """Should detect multiple foreign keys from one entity."""
        from dashboardmd.model import Dimension, Entity
        from dashboardmd.suggest import auto_join

        orders = Entity(
            name="orders",
            dimensions=[
                Dimension("id", type="number", primary_key=True),
                Dimension("customer_id", type="number"),
                Dimension("product_id", type="number"),
            ],
        )
        customers = Entity(
            name="customers",
            dimensions=[Dimension("id", type="number", primary_key=True)],
        )
        products = Entity(
            name="products",
            dimensions=[Dimension("id", type="number", primary_key=True)],
        )
        relationships = auto_join([orders, customers, products])

        assert len(relationships) >= 2

    def test_no_false_positives(self) -> None:
        """Should not create relationships for non-FK numeric columns."""
        from dashboardmd.model import Dimension, Entity
        from dashboardmd.suggest import auto_join

        orders = Entity(
            name="orders",
            dimensions=[
                Dimension("id", type="number", primary_key=True),
                Dimension("amount", type="number"),
            ],
        )
        products = Entity(
            name="products",
            dimensions=[Dimension("id", type="number", primary_key=True)],
        )
        relationships = auto_join([orders, products])

        # 'amount' should NOT create a relationship to 'products'
        assert len(relationships) == 0
