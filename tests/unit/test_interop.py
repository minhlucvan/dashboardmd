"""Tests for BI platform interop modules (import-only)."""

from __future__ import annotations

import pytest

from dashboardmd.model import Dimension, Entity, Measure, Relationship


# ---------------------------------------------------------------------------
# Metabase — from_metabase()
# ---------------------------------------------------------------------------


class TestFromMetabase:
    """from_metabase() converts Metabase metadata to entities + relationships."""

    def test_basic_table_import(self) -> None:
        from dashboardmd.interop.metabase import from_metabase

        metadata = {
            "tables": [{
                "name": "orders",
                "fields": [
                    {"id": 1, "name": "id", "base_type": "type/Integer", "semantic_type": "type/PK"},
                    {"id": 2, "name": "amount", "base_type": "type/Float"},
                ],
                "metrics": [],
            }]
        }
        entities, rels = from_metabase(metadata)
        assert len(entities) == 1
        assert entities[0].name == "orders"
        assert len(entities[0].dimensions) == 2
        assert rels == []

    def test_multiple_tables(self) -> None:
        from dashboardmd.interop.metabase import from_metabase

        metadata = {
            "tables": [
                {"name": "orders", "fields": [{"id": 1, "name": "id", "base_type": "type/Integer"}], "metrics": []},
                {"name": "customers", "fields": [{"id": 2, "name": "id", "base_type": "type/Integer"}], "metrics": []},
                {"name": "products", "fields": [{"id": 3, "name": "id", "base_type": "type/Integer"}], "metrics": []},
            ]
        }
        entities, _ = from_metabase(metadata)
        assert len(entities) == 3
        names = [e.name for e in entities]
        assert "orders" in names
        assert "customers" in names
        assert "products" in names

    def test_field_type_mapping(self) -> None:
        from dashboardmd.interop.metabase import from_metabase

        metadata = {
            "tables": [{
                "name": "test",
                "fields": [
                    {"id": 1, "name": "id", "base_type": "type/Integer", "semantic_type": "type/PK"},
                    {"id": 2, "name": "price", "base_type": "type/Float"},
                    {"id": 3, "name": "big_id", "base_type": "type/BigInteger"},
                    {"id": 4, "name": "amount", "base_type": "type/Decimal"},
                    {"id": 5, "name": "created_at", "base_type": "type/DateTime"},
                    {"id": 6, "name": "date", "base_type": "type/Date"},
                    {"id": 7, "name": "time", "base_type": "type/Time"},
                    {"id": 8, "name": "tz_time", "base_type": "type/DateTimeWithLocalTZ"},
                    {"id": 9, "name": "active", "base_type": "type/Boolean"},
                    {"id": 10, "name": "name", "base_type": "type/Name"},
                    {"id": 11, "name": "category", "base_type": "type/Category"},
                    {"id": 12, "name": "city", "base_type": "type/City"},
                    {"id": 13, "name": "state", "base_type": "type/State"},
                    {"id": 14, "name": "country", "base_type": "type/Country"},
                    {"id": 15, "name": "zipcode", "base_type": "type/ZipCode"},
                    {"id": 16, "name": "email", "base_type": "type/Email"},
                    {"id": 17, "name": "url", "base_type": "type/URL"},
                    {"id": 18, "name": "description", "base_type": "type/Text"},
                ],
                "metrics": [],
            }]
        }
        entities, _ = from_metabase(metadata)
        dims = {d.name: d for d in entities[0].dimensions}
        # Number types
        assert dims["id"].type == "number"
        assert dims["price"].type == "number"
        assert dims["big_id"].type == "number"
        assert dims["amount"].type == "number"
        # Time types
        assert dims["created_at"].type == "time"
        assert dims["date"].type == "time"
        assert dims["time"].type == "time"
        assert dims["tz_time"].type == "time"
        # Boolean
        assert dims["active"].type == "boolean"
        # String types
        assert dims["name"].type == "string"
        assert dims["category"].type == "string"
        assert dims["city"].type == "string"
        assert dims["state"].type == "string"
        assert dims["country"].type == "string"
        assert dims["zipcode"].type == "string"
        assert dims["email"].type == "string"
        assert dims["url"].type == "string"
        assert dims["description"].type == "string"

    def test_primary_key_detection(self) -> None:
        from dashboardmd.interop.metabase import from_metabase

        metadata = {
            "tables": [{
                "name": "test",
                "fields": [
                    {"id": 1, "name": "id", "base_type": "type/Integer", "semantic_type": "type/PK"},
                    {"id": 2, "name": "name", "base_type": "type/Text"},
                ],
                "metrics": [],
            }]
        }
        entities, _ = from_metabase(metadata)
        dims = {d.name: d for d in entities[0].dimensions}
        assert dims["id"].primary_key is True
        assert dims["name"].primary_key is False

    def test_metrics_import(self) -> None:
        from dashboardmd.interop.metabase import from_metabase

        metadata = {
            "tables": [{
                "name": "orders",
                "fields": [{"id": 1, "name": "id", "base_type": "type/Integer"}],
                "metrics": [
                    {"name": "revenue", "aggregation": "sum", "field": "amount"},
                    {"name": "order_count", "aggregation": "count"},
                    {"name": "avg_price", "aggregation": "avg", "field": "price"},
                    {"name": "unique_customers", "aggregation": "distinct", "field": "customer_id"},
                    {"name": "min_price", "aggregation": "min", "field": "price"},
                    {"name": "max_price", "aggregation": "max", "field": "price"},
                ],
            }]
        }
        entities, _ = from_metabase(metadata)
        measures = {m.name: m for m in entities[0].measures}
        assert measures["revenue"].type == "sum"
        assert measures["revenue"].sql == "amount"
        assert measures["order_count"].type == "count"
        assert measures["avg_price"].type == "avg"
        assert measures["unique_customers"].type == "count_distinct"
        assert measures["min_price"].type == "min"
        assert measures["max_price"].type == "max"

    def test_foreign_key_relationships(self) -> None:
        from dashboardmd.interop.metabase import from_metabase

        metadata = {
            "tables": [
                {
                    "name": "orders",
                    "fields": [
                        {"id": 1, "name": "id", "base_type": "type/Integer", "semantic_type": "type/PK"},
                        {"id": 2, "name": "customer_id", "base_type": "type/Integer", "fk_target_field_id": 3},
                    ],
                    "metrics": [],
                },
                {
                    "name": "customers",
                    "fields": [
                        {"id": 3, "name": "id", "base_type": "type/Integer", "semantic_type": "type/PK"},
                        {"id": 4, "name": "name", "base_type": "type/Text"},
                    ],
                    "metrics": [],
                },
            ]
        }
        entities, rels = from_metabase(metadata)
        assert len(rels) == 1
        assert rels[0].from_entity == "orders"
        assert rels[0].to_entity == "customers"
        assert rels[0].on == ("customer_id", "id")
        assert rels[0].type == "many_to_one"

    def test_multiple_foreign_keys(self) -> None:
        from dashboardmd.interop.metabase import from_metabase

        metadata = {
            "tables": [
                {
                    "name": "orders",
                    "fields": [
                        {"id": 1, "name": "id", "base_type": "type/Integer", "semantic_type": "type/PK"},
                        {"id": 2, "name": "customer_id", "base_type": "type/Integer", "fk_target_field_id": 4},
                        {"id": 3, "name": "product_id", "base_type": "type/Integer", "fk_target_field_id": 5},
                    ],
                    "metrics": [],
                },
                {
                    "name": "customers",
                    "fields": [{"id": 4, "name": "id", "base_type": "type/Integer", "semantic_type": "type/PK"}],
                    "metrics": [],
                },
                {
                    "name": "products",
                    "fields": [{"id": 5, "name": "id", "base_type": "type/Integer", "semantic_type": "type/PK"}],
                    "metrics": [],
                },
            ]
        }
        _, rels = from_metabase(metadata)
        assert len(rels) == 2
        rel_targets = {r.to_entity for r in rels}
        assert "customers" in rel_targets
        assert "products" in rel_targets

    def test_unknown_field_type_defaults_to_string(self) -> None:
        from dashboardmd.interop.metabase import from_metabase

        metadata = {
            "tables": [{
                "name": "test",
                "fields": [{"id": 1, "name": "weird", "base_type": "type/Unknown"}],
                "metrics": [],
            }]
        }
        entities, _ = from_metabase(metadata)
        assert entities[0].dimensions[0].type == "string"

    def test_unknown_aggregation_defaults_to_count(self) -> None:
        from dashboardmd.interop.metabase import from_metabase

        metadata = {
            "tables": [{
                "name": "test",
                "fields": [],
                "metrics": [{"name": "custom", "aggregation": "weird_agg"}],
            }]
        }
        entities, _ = from_metabase(metadata)
        assert entities[0].measures[0].type == "count"

    def test_empty_metadata(self) -> None:
        from dashboardmd.interop.metabase import from_metabase

        entities, rels = from_metabase({})
        assert entities == []
        assert rels == []

    def test_table_with_no_fields_or_metrics(self) -> None:
        from dashboardmd.interop.metabase import from_metabase

        metadata = {"tables": [{"name": "empty"}]}
        entities, _ = from_metabase(metadata)
        assert len(entities) == 1
        assert entities[0].dimensions == []
        assert entities[0].measures == []


# ---------------------------------------------------------------------------
# LookML — from_lookml()
# ---------------------------------------------------------------------------


class TestFromLookML:
    """from_lookml() converts LookML models to entities + relationships."""

    def test_basic_view_import(self) -> None:
        from dashboardmd.interop.lookml import from_lookml

        model = {
            "views": [{
                "name": "orders",
                "sql_table_name": "public.orders",
                "dimensions": [
                    {"name": "id", "type": "number", "primary_key": True},
                    {"name": "status", "type": "string"},
                ],
                "measures": [{"name": "count", "type": "count"}],
            }],
            "explores": [],
        }
        entities, rels = from_lookml(model)
        assert len(entities) == 1
        assert entities[0].name == "orders"
        assert entities[0].source == "public.orders"
        assert len(entities[0].dimensions) == 2
        assert len(entities[0].measures) == 1

    def test_dimension_type_mapping(self) -> None:
        from dashboardmd.interop.lookml import from_lookml

        model = {
            "views": [{
                "name": "test",
                "dimensions": [
                    {"name": "id", "type": "number", "primary_key": True},
                    {"name": "name", "type": "string"},
                    {"name": "active", "type": "yesno"},
                    {"name": "created_date", "type": "date"},
                    {"name": "created_time", "type": "time"},
                    {"name": "created_dt", "type": "date_time"},
                    {"name": "raw", "type": "date_raw"},
                    {"name": "by_date", "type": "date_date"},
                    {"name": "by_week", "type": "date_week"},
                    {"name": "by_month", "type": "date_month"},
                    {"name": "by_quarter", "type": "date_quarter"},
                    {"name": "by_year", "type": "date_year"},
                    {"name": "price_tier", "type": "tier"},
                    {"name": "zip", "type": "zipcode"},
                    {"name": "loc", "type": "location"},
                ],
                "measures": [],
            }],
            "explores": [],
        }
        entities, _ = from_lookml(model)
        dims = {d.name: d for d in entities[0].dimensions}
        assert dims["id"].type == "number"
        assert dims["id"].primary_key is True
        assert dims["name"].type == "string"
        assert dims["active"].type == "boolean"
        assert dims["created_date"].type == "time"
        assert dims["created_time"].type == "time"
        assert dims["created_dt"].type == "time"
        assert dims["raw"].type == "time"
        assert dims["by_date"].type == "time"
        assert dims["by_week"].type == "time"
        assert dims["by_month"].type == "time"
        assert dims["by_quarter"].type == "time"
        assert dims["by_year"].type == "time"
        assert dims["price_tier"].type == "string"
        assert dims["zip"].type == "string"
        assert dims["loc"].type == "string"

    def test_measure_type_mapping(self) -> None:
        from dashboardmd.interop.lookml import from_lookml

        model = {
            "views": [{
                "name": "test",
                "dimensions": [],
                "measures": [
                    {"name": "total", "type": "sum", "sql": "${amount}"},
                    {"name": "count", "type": "count"},
                    {"name": "unique", "type": "count_distinct", "sql": "${user_id}"},
                    {"name": "avg_price", "type": "average", "sql": "${price}"},
                    {"name": "min_val", "type": "min", "sql": "${value}"},
                    {"name": "max_val", "type": "max", "sql": "${value}"},
                    {"name": "calc", "type": "number", "sql": "${total} / ${count}"},
                    {"name": "sum_d", "type": "sum_distinct", "sql": "${amount}"},
                ],
            }],
            "explores": [],
        }
        entities, _ = from_lookml(model)
        measures = {m.name: m for m in entities[0].measures}
        assert measures["total"].type == "sum"
        assert measures["total"].sql == "${amount}"
        assert measures["count"].type == "count"
        assert measures["unique"].type == "count_distinct"
        assert measures["avg_price"].type == "avg"
        assert measures["min_val"].type == "min"
        assert measures["max_val"].type == "max"
        assert measures["calc"].type == "number"
        assert measures["sum_d"].type == "sum"

    def test_explore_joins_create_relationships(self) -> None:
        from dashboardmd.interop.lookml import from_lookml

        model = {
            "views": [
                {"name": "orders", "dimensions": [{"name": "id", "type": "number"}], "measures": []},
                {"name": "customers", "dimensions": [{"name": "id", "type": "number"}], "measures": []},
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
        _, rels = from_lookml(model)
        assert len(rels) == 1
        assert rels[0].from_entity == "orders"
        assert rels[0].to_entity == "customers"
        assert rels[0].on == ("customer_id", "id")
        assert rels[0].type == "many_to_one"

    def test_explore_from_override(self) -> None:
        from dashboardmd.interop.lookml import from_lookml

        model = {
            "views": [
                {"name": "orders_v2", "dimensions": [], "measures": []},
                {"name": "users", "dimensions": [], "measures": []},
            ],
            "explores": [{
                "name": "orders_explore",
                "from": "orders_v2",
                "joins": [{
                    "name": "users_join",
                    "from": "users",
                    "sql_on": "${orders_v2.user_id} = ${users.id}",
                    "relationship": "many_to_one",
                }],
            }],
        }
        _, rels = from_lookml(model)
        assert rels[0].from_entity == "orders_v2"
        assert rels[0].to_entity == "users"

    def test_sql_on_without_dollar_braces(self) -> None:
        from dashboardmd.interop.lookml import from_lookml

        model = {
            "views": [],
            "explores": [{
                "name": "orders",
                "joins": [{
                    "name": "customers",
                    "sql_on": "orders.customer_id = customers.id",
                    "relationship": "many_to_one",
                }],
            }],
        }
        _, rels = from_lookml(model)
        assert rels[0].on == ("customer_id", "id")

    def test_unparseable_sql_on_defaults_to_id(self) -> None:
        from dashboardmd.interop.lookml import from_lookml

        model = {
            "views": [],
            "explores": [{
                "name": "orders",
                "joins": [{
                    "name": "customers",
                    "sql_on": "COMPLEX EXPRESSION",
                    "relationship": "many_to_one",
                }],
            }],
        }
        _, rels = from_lookml(model)
        assert rels[0].on == ("id", "id")

    def test_dimension_sql_preserved(self) -> None:
        from dashboardmd.interop.lookml import from_lookml

        model = {
            "views": [{
                "name": "test",
                "dimensions": [{"name": "full_name", "type": "string", "sql": "CONCAT(first, ' ', last)"}],
                "measures": [],
            }],
            "explores": [],
        }
        entities, _ = from_lookml(model)
        assert entities[0].dimensions[0].sql == "CONCAT(first, ' ', last)"

    def test_empty_model(self) -> None:
        from dashboardmd.interop.lookml import from_lookml

        entities, rels = from_lookml({})
        assert entities == []
        assert rels == []

    def test_multiple_views(self) -> None:
        from dashboardmd.interop.lookml import from_lookml

        model = {
            "views": [
                {"name": "v1", "dimensions": [{"name": "a", "type": "string"}], "measures": []},
                {"name": "v2", "dimensions": [{"name": "b", "type": "number"}], "measures": []},
            ],
            "explores": [],
        }
        entities, _ = from_lookml(model)
        assert len(entities) == 2


# ---------------------------------------------------------------------------
# Cube — from_cube()
# ---------------------------------------------------------------------------


class TestFromCube:
    """from_cube() converts Cube.js schemas to entities + relationships."""

    def test_basic_cube_import(self) -> None:
        from dashboardmd.interop.cube import from_cube

        schema = {
            "cubes": [{
                "name": "orders",
                "sql": "SELECT * FROM orders",
                "dimensions": [
                    {"name": "id", "type": "number", "primaryKey": True},
                    {"name": "status", "type": "string"},
                ],
                "measures": [
                    {"name": "count", "type": "count"},
                    {"name": "total", "type": "sum", "sql": "amount"},
                ],
            }]
        }
        entities, rels = from_cube(schema)
        assert len(entities) == 1
        assert entities[0].name == "orders"
        assert entities[0].source == "SELECT * FROM orders"
        assert len(entities[0].dimensions) == 2
        assert len(entities[0].measures) == 2

    def test_dimension_type_mapping(self) -> None:
        from dashboardmd.interop.cube import from_cube

        schema = {
            "cubes": [{
                "name": "test",
                "dimensions": [
                    {"name": "name", "type": "string"},
                    {"name": "amount", "type": "number"},
                    {"name": "created_at", "type": "time"},
                    {"name": "active", "type": "boolean"},
                    {"name": "location", "type": "geo"},
                ],
                "measures": [],
            }]
        }
        entities, _ = from_cube(schema)
        dims = {d.name: d for d in entities[0].dimensions}
        assert dims["name"].type == "string"
        assert dims["amount"].type == "number"
        assert dims["created_at"].type == "time"
        assert dims["active"].type == "boolean"
        assert dims["location"].type == "string"  # geo → string

    def test_measure_type_mapping(self) -> None:
        from dashboardmd.interop.cube import from_cube

        schema = {
            "cubes": [{
                "name": "test",
                "dimensions": [],
                "measures": [
                    {"name": "total", "type": "sum", "sql": "amount"},
                    {"name": "n", "type": "count"},
                    {"name": "unique", "type": "countDistinct", "sql": "user_id"},
                    {"name": "approx", "type": "countDistinctApprox", "sql": "user_id"},
                    {"name": "avg", "type": "avg", "sql": "price"},
                    {"name": "low", "type": "min", "sql": "price"},
                    {"name": "high", "type": "max", "sql": "price"},
                    {"name": "calc", "type": "number", "sql": "total / count"},
                    {"name": "running", "type": "runningTotal", "sql": "amount"},
                ],
            }]
        }
        entities, _ = from_cube(schema)
        measures = {m.name: m for m in entities[0].measures}
        assert measures["total"].type == "sum"
        assert measures["n"].type == "count"
        assert measures["unique"].type == "count_distinct"
        assert measures["approx"].type == "count_distinct"
        assert measures["avg"].type == "avg"
        assert measures["low"].type == "min"
        assert measures["high"].type == "max"
        assert measures["calc"].type == "number"
        assert measures["running"].type == "sum"

    def test_primary_key(self) -> None:
        from dashboardmd.interop.cube import from_cube

        schema = {
            "cubes": [{
                "name": "test",
                "dimensions": [
                    {"name": "id", "type": "number", "primaryKey": True},
                    {"name": "name", "type": "string"},
                ],
                "measures": [],
            }]
        }
        entities, _ = from_cube(schema)
        dims = {d.name: d for d in entities[0].dimensions}
        assert dims["id"].primary_key is True
        assert dims["name"].primary_key is False

    def test_joins_create_relationships(self) -> None:
        from dashboardmd.interop.cube import from_cube

        schema = {
            "cubes": [{
                "name": "orders",
                "sql": "SELECT * FROM orders",
                "dimensions": [{"name": "id", "type": "number", "primaryKey": True}],
                "measures": [],
                "joins": {
                    "customers": {
                        "sql": "{CUBE}.customer_id = {customers}.id",
                        "relationship": "belongsTo",
                    },
                },
            }]
        }
        _, rels = from_cube(schema)
        assert len(rels) == 1
        assert rels[0].from_entity == "orders"
        assert rels[0].to_entity == "customers"
        assert rels[0].on == ("customer_id", "id")
        assert rels[0].type == "many_to_one"

    def test_join_type_mapping(self) -> None:
        from dashboardmd.interop.cube import from_cube

        schema = {
            "cubes": [{
                "name": "orders",
                "dimensions": [],
                "measures": [],
                "joins": {
                    "items": {"sql": "{CUBE}.id = {items}.order_id", "relationship": "hasMany"},
                    "customer": {"sql": "{CUBE}.customer_id = {customer}.id", "relationship": "hasOne"},
                    "vendor": {"sql": "{CUBE}.vendor_id = {vendor}.id", "relationship": "belongsTo"},
                },
            }]
        }
        _, rels = from_cube(schema)
        rel_map = {r.to_entity: r for r in rels}
        assert rel_map["items"].type == "one_to_many"
        assert rel_map["customer"].type == "many_to_one"
        assert rel_map["vendor"].type == "many_to_one"

    def test_unparseable_join_sql_defaults_to_id(self) -> None:
        from dashboardmd.interop.cube import from_cube

        schema = {
            "cubes": [{
                "name": "orders",
                "dimensions": [],
                "measures": [],
                "joins": {
                    "other": {"sql": "COMPLEX EXPRESSION", "relationship": "belongsTo"},
                },
            }]
        }
        _, rels = from_cube(schema)
        assert rels[0].on == ("id", "id")

    def test_dimension_sql_preserved(self) -> None:
        from dashboardmd.interop.cube import from_cube

        schema = {
            "cubes": [{
                "name": "test",
                "dimensions": [{"name": "full_name", "type": "string", "sql": "CONCAT(first, last)"}],
                "measures": [],
            }]
        }
        entities, _ = from_cube(schema)
        assert entities[0].dimensions[0].sql == "CONCAT(first, last)"

    def test_empty_schema(self) -> None:
        from dashboardmd.interop.cube import from_cube

        entities, rels = from_cube({})
        assert entities == []
        assert rels == []

    def test_multiple_cubes(self) -> None:
        from dashboardmd.interop.cube import from_cube

        schema = {
            "cubes": [
                {"name": "orders", "dimensions": [{"name": "id", "type": "number"}], "measures": []},
                {"name": "users", "dimensions": [{"name": "id", "type": "number"}], "measures": []},
            ]
        }
        entities, _ = from_cube(schema)
        assert len(entities) == 2


# ---------------------------------------------------------------------------
# PowerBI — from_powerbi()
# ---------------------------------------------------------------------------


class TestFromPowerBI:
    """from_powerbi() converts PowerBI tabular models to entities + relationships."""

    def test_basic_table_import(self) -> None:
        from dashboardmd.interop.powerbi import from_powerbi

        model = {
            "tables": [{
                "name": "orders",
                "columns": [
                    {"name": "id", "dataType": "int64", "isKey": True},
                    {"name": "status", "dataType": "string"},
                ],
                "measures": [
                    {"name": "revenue", "expression": "SUM('orders'[amount])"},
                ],
            }],
            "relationships": [],
        }
        entities, rels = from_powerbi(model)
        assert len(entities) == 1
        assert entities[0].name == "orders"
        assert len(entities[0].dimensions) == 2
        assert len(entities[0].measures) == 1
        assert rels == []

    def test_column_type_mapping(self) -> None:
        from dashboardmd.interop.powerbi import from_powerbi

        model = {
            "tables": [{
                "name": "test",
                "columns": [
                    {"name": "int_col", "dataType": "int64"},
                    {"name": "dbl_col", "dataType": "double"},
                    {"name": "dec_col", "dataType": "decimal"},
                    {"name": "cur_col", "dataType": "currency"},
                    {"name": "str_col", "dataType": "string"},
                    {"name": "bool_col", "dataType": "boolean"},
                    {"name": "dt_col", "dataType": "dateTime"},
                    {"name": "date_col", "dataType": "date"},
                    {"name": "time_col", "dataType": "time"},
                ],
                "measures": [],
            }],
            "relationships": [],
        }
        entities, _ = from_powerbi(model)
        dims = {d.name: d for d in entities[0].dimensions}
        assert dims["int_col"].type == "number"
        assert dims["dbl_col"].type == "number"
        assert dims["dec_col"].type == "number"
        assert dims["cur_col"].type == "number"
        assert dims["str_col"].type == "string"
        assert dims["bool_col"].type == "boolean"
        assert dims["dt_col"].type == "time"
        assert dims["date_col"].type == "time"
        assert dims["time_col"].type == "time"

    def test_primary_key_detection(self) -> None:
        from dashboardmd.interop.powerbi import from_powerbi

        model = {
            "tables": [{
                "name": "test",
                "columns": [
                    {"name": "id", "dataType": "int64", "isKey": True},
                    {"name": "name", "dataType": "string"},
                ],
                "measures": [],
            }],
            "relationships": [],
        }
        entities, _ = from_powerbi(model)
        dims = {d.name: d for d in entities[0].dimensions}
        assert dims["id"].primary_key is True
        assert dims["name"].primary_key is False

    def test_dax_measure_type_inference(self) -> None:
        from dashboardmd.interop.powerbi import from_powerbi

        model = {
            "tables": [{
                "name": "test",
                "columns": [],
                "measures": [
                    {"name": "total", "expression": "SUM('test'[amount])"},
                    {"name": "n", "expression": "COUNT('test'[id])"},
                    {"name": "rows", "expression": "COUNTROWS('test')"},
                    {"name": "unique", "expression": "DISTINCTCOUNT('test'[customer_id])"},
                    {"name": "avg_price", "expression": "AVERAGE('test'[price])"},
                    {"name": "min_price", "expression": "MIN('test'[price])"},
                    {"name": "max_price", "expression": "MAX('test'[price])"},
                ],
            }],
            "relationships": [],
        }
        entities, _ = from_powerbi(model)
        measures = {m.name: m for m in entities[0].measures}
        assert measures["total"].type == "sum"
        assert measures["n"].type == "count"
        assert measures["rows"].type == "count"
        assert measures["unique"].type == "count_distinct"
        assert measures["avg_price"].type == "avg"
        assert measures["min_price"].type == "min"
        assert measures["max_price"].type == "max"

    def test_dax_column_extraction(self) -> None:
        from dashboardmd.interop.powerbi import from_powerbi

        model = {
            "tables": [{
                "name": "test",
                "columns": [],
                "measures": [
                    {"name": "total", "expression": "SUM('test'[amount])"},
                    {"name": "avg", "expression": "AVERAGE([price])"},
                ],
            }],
            "relationships": [],
        }
        entities, _ = from_powerbi(model)
        measures = {m.name: m for m in entities[0].measures}
        assert measures["total"].sql == "amount"
        assert measures["avg"].sql == "price"

    def test_complex_dax_defaults_to_number(self) -> None:
        from dashboardmd.interop.powerbi import from_powerbi

        model = {
            "tables": [{
                "name": "test",
                "columns": [],
                "measures": [
                    {"name": "custom", "expression": "CALCULATE(SUM(test[a]), FILTER(test, test[b] > 0))"},
                ],
            }],
            "relationships": [],
        }
        entities, _ = from_powerbi(model)
        # CALCULATE doesn't match any simple pattern
        assert entities[0].measures[0].type == "number"

    def test_relationships_import(self) -> None:
        from dashboardmd.interop.powerbi import from_powerbi

        model = {
            "tables": [
                {"name": "orders", "columns": [{"name": "id", "dataType": "int64"}], "measures": []},
                {"name": "customers", "columns": [{"name": "id", "dataType": "int64"}], "measures": []},
            ],
            "relationships": [{
                "fromTable": "orders",
                "toTable": "customers",
                "fromColumn": "customer_id",
                "toColumn": "id",
                "cardinality": "manyToOne",
            }],
        }
        _, rels = from_powerbi(model)
        assert len(rels) == 1
        assert rels[0].from_entity == "orders"
        assert rels[0].to_entity == "customers"
        assert rels[0].on == ("customer_id", "id")
        assert rels[0].type == "many_to_one"

    def test_relationship_cardinality_mapping(self) -> None:
        from dashboardmd.interop.powerbi import from_powerbi

        model = {
            "tables": [],
            "relationships": [
                {"fromTable": "a", "toTable": "b", "fromColumn": "id", "toColumn": "id", "cardinality": "oneToMany"},
                {"fromTable": "c", "toTable": "d", "fromColumn": "id", "toColumn": "id", "cardinality": "manyToOne"},
                {"fromTable": "e", "toTable": "f", "fromColumn": "id", "toColumn": "id", "cardinality": "oneToOne"},
                {"fromTable": "g", "toTable": "h", "fromColumn": "id", "toColumn": "id", "cardinality": "manyToMany"},
            ],
        }
        _, rels = from_powerbi(model)
        rel_map = {r.from_entity: r for r in rels}
        assert rel_map["a"].type == "one_to_many"
        assert rel_map["c"].type == "many_to_one"
        assert rel_map["e"].type == "one_to_one"
        assert rel_map["g"].type == "many_to_many"

    def test_cross_filtering_behavior_used_for_cardinality(self) -> None:
        from dashboardmd.interop.powerbi import from_powerbi

        model = {
            "tables": [],
            "relationships": [{
                "fromTable": "a",
                "toTable": "b",
                "fromColumn": "id",
                "toColumn": "id",
                "crossFilteringBehavior": "oneToMany",
                "cardinality": "manyToOne",
            }],
        }
        _, rels = from_powerbi(model)
        # crossFilteringBehavior takes precedence
        assert rels[0].type == "one_to_many"

    def test_unknown_data_type_defaults_to_string(self) -> None:
        from dashboardmd.interop.powerbi import from_powerbi

        model = {
            "tables": [{
                "name": "test",
                "columns": [{"name": "weird", "dataType": "unknownType"}],
                "measures": [],
            }],
            "relationships": [],
        }
        entities, _ = from_powerbi(model)
        assert entities[0].dimensions[0].type == "string"

    def test_empty_model(self) -> None:
        from dashboardmd.interop.powerbi import from_powerbi

        entities, rels = from_powerbi({})
        assert entities == []
        assert rels == []

    def test_multiple_tables(self) -> None:
        from dashboardmd.interop.powerbi import from_powerbi

        model = {
            "tables": [
                {"name": "t1", "columns": [{"name": "a", "dataType": "string"}], "measures": []},
                {"name": "t2", "columns": [{"name": "b", "dataType": "int64"}], "measures": []},
                {"name": "t3", "columns": [], "measures": []},
            ],
            "relationships": [],
        }
        entities, _ = from_powerbi(model)
        assert len(entities) == 3


# ---------------------------------------------------------------------------
# Interop package imports
# ---------------------------------------------------------------------------


class TestInteropImports:
    """All interop functions should be importable from the interop package."""

    def test_import_from_functions(self) -> None:
        from dashboardmd.interop import from_cube, from_lookml, from_metabase, from_powerbi

        assert callable(from_metabase)
        assert callable(from_lookml)
        assert callable(from_cube)
        assert callable(from_powerbi)

    def test_all_exports(self) -> None:
        import dashboardmd.interop as interop

        assert set(interop.__all__) == {"from_cube", "from_lookml", "from_metabase", "from_powerbi"}
