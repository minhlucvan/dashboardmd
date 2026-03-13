# dashboardmd Test Suite

## Architecture

Tests are organized by the 4-layer architecture, with each layer tested independently
(unit tests) and layers tested together (integration tests).

```
tests/
├── README.md                    # This file
├── conftest.py                  # Shared fixtures: sample paths, DuckDB connections
├── samples/                     # Sample CSV data for all tests
│   ├── orders.csv               # 20 orders with amounts, dates, statuses
│   ├── customers.csv            # 8 customers with segments, regions
│   └── products.csv             # 5 products with categories, prices
│
├── unit/                        # Unit tests — one layer at a time
│   ├── test_import.py           # Package import and version check
│   │
│   ├── sources/                 # Layer 1: Source Layer
│   │   ├── README.md            # Source layer test scenarios
│   │   ├── test_base.py         # SourceHandler ABC contract
│   │   ├── test_file.py         # CSV, Parquet, JSON handlers
│   │   ├── test_dataframe.py    # pandas/polars DataFrame handler
│   │   ├── test_database.py     # DuckDB, SQLite, Postgres, MySQL handlers
│   │   └── test_sql.py          # Raw SQL handler
│   │
│   ├── test_model.py            # Layer 2: Semantic Layer
│   │                            #   Dimension, Measure, Entity, Relationship
│   │
│   ├── test_query.py            # Layer 3: Query Layer
│   │                            #   Query object, SQL generation, join resolution
│   │
│   ├── test_engine.py           # Layer 3: Engine (DuckDB execution)
│   │                            #   Source registration, query execution
│   │
│   ├── test_time.py             # Layer 3: Time Intelligence
│   │                            #   Granularity, period comparison, presets
│   │
│   ├── test_dashboard.py        # Layer 4: Render Layer
│   │                            #   Dashboard construction, tiles, filters,
│   │                            #   smart viz selection, save/render
│   │
│   ├── test_suggest.py          # Auto-Discovery
│   │                            #   discover(), suggest_measures(), auto_join()
│   │
│   └── test_refresh.py          # Refresh & Diff Tracking
│                                #   MetricDiff, refresh()
│
└── integration/                 # Integration tests — layers working together
    ├── README.md                # Integration test scenarios
    ├── test_csv_to_dashboard.py # CSV → Entity → Dashboard → Markdown
    ├── test_multi_source.py     # Multiple source types in one dashboard
    └── test_end_to_end.py       # README examples, agent workflows, Source factory
```

## Test Coverage by Layer

### Layer 1: Source Layer (26 tests)

| Handler | Tests | Key Scenarios |
|---------|-------|--------------|
| `SourceHandler` ABC | 6 | Cannot instantiate, must implement register/describe |
| `CSVSource` | 6 | Register, query, schema, missing file, empty, aggregation |
| `ParquetSource` | 4 | Register, values, schema, missing file |
| `JSONSource` | 3 | Register, values, missing file |
| `DataFrameSource` | 6 | Register, types, empty, mixed types, aggregation, schema |
| `DuckDBSource` | 5 | Register, values, missing file, missing table, schema |
| `SQLiteSource` | 1 | Register from SQLite file |
| `RawSQLSource` | 5 | Simple query, WHERE, aggregation, invalid SQL, schema |

### Layer 2: Semantic Layer (22 tests)

| Class | Tests | Key Scenarios |
|-------|-------|--------------|
| `Dimension` | 9 | String/time/number types, PK flag, SQL override, format, invalid type |
| `Measure` | 10 | sum/count/count_distinct/avg/min/max, computed, format, filters, invalid |
| `Entity` | 6 | Create with dims/measures, source, find by name, PK dimension |
| `Relationship` | 6 | many_to_one/one_to_many/one_to_one/many_to_many, tuple validation, invalid |

### Layer 3: Query Layer (25 tests)

| Component | Tests | Key Scenarios |
|-----------|-------|--------------|
| `Query` object | 8 | Create, measures, filters, time granularity, sort, limit, compare |
| `QueryBuilder` SQL | 8 | SUM/COUNT/COUNT_DISTINCT/AVG, GROUP BY, WHERE, ORDER BY, LIMIT, JOIN |
| `JoinResolver` | 6 | Direct join, single entity, transitive, multiple rels, unrelated error |
| `Engine` | 8 | Create connection, register CSV, execute, GROUP BY, JOIN, filter, cleanup |
| Time Intelligence | 12 | Granularity (5), period comparison (5), presets (5) |

### Layer 4: Render Layer (16 tests)

| Component | Tests | Key Scenarios |
|-----------|-------|--------------|
| Dashboard construction | 3 | Create, with entities, with relationships |
| Sections | 2 | Add section, multiple sections |
| Tiles | 7 | Metric, dimension, explicit type, compare, top N, multi-measure, granularity |
| Filters | 3 | Date filter, categorical filter, multiple |
| Smart viz selection | 5 | Metric, line, bar, table, metric with delta |
| Save/Render | 4 | Creates file, includes title, section headings, metric values |

### Other (9 tests)

| Component | Tests | Key Scenarios |
|-----------|-------|--------------|
| Auto-discovery | 8 | Discover CSVs, infer dims/types, empty dir, non-data files, suggest measures, auto join |
| Refresh | 6 | MetricDiff (positive, negative, zero, from-zero change), string repr |

### Integration Tests (10 tests)

| Test | Layers Covered | Scenario |
|------|---------------|----------|
| Single entity dashboard | All 4 | CSV → tiles → Markdown |
| Multi-entity joined dashboard | All 4 | 3 CSVs → joins → filtered → Markdown |
| Filters applied | All 4 | Filter restricts tile data |
| CSV + Parquet together | Source + Engine | Cross-format queries |
| CSV + DuckDB file | Source + Engine | Cross-source joins |
| CSV + DataFrame | Source + Engine | DataFrame + file hybrid |
| Three source types | Source + Engine | Triple-source queries |
| README Quick Start | All 4 | Exact README example |
| Agent discover workflow | All 4 | discover() → auto_dashboard() → save() |
| Source factory methods | Source | Source.csv(), .parquet(), .duckdb(), .dataframe(), .sql() |

## Running Tests

```bash
# All tests
pytest tests/ -v

# Unit tests only
pytest tests/unit/ -v

# Integration tests only
pytest tests/integration/ -v -m integration

# Source layer only
pytest tests/unit/sources/ -v

# Specific layer
pytest tests/unit/test_model.py -v          # Semantic layer
pytest tests/unit/test_query.py -v          # Query layer
pytest tests/unit/test_dashboard.py -v      # Render layer

# Tests that require pandas
pytest tests/ -v -m requires_pandas

# Coverage report
pytest tests/ --cov=dashboardmd --cov-report=term-missing
```

## Test Markers

| Marker | Description |
|--------|------------|
| `integration` | Tests that span multiple layers |
| `requires_pandas` | Tests that need pandas installed |
| `requires_matplotlib` | Tests that need matplotlib installed |
| `requires_duckdb` | Tests that need duckdb installed |
| `slow` | Tests that take more than 1 second |

## Sample Data

All tests use the same 3 CSV files in `tests/samples/`:

- **orders.csv**: 20 orders (Jan–Mar 2026) with amounts ($89.99–$320), statuses (completed/pending/cancelled), FK to customers and products
- **customers.csv**: 8 customers across 4 regions and 3 segments (enterprise/smb/mid_market)
- **products.csv**: 5 products in 3 categories (widgets/gadgets/gizmos)

These files are small enough for fast tests but rich enough to verify joins,
aggregations, filters, and groupings.

## Fixtures

Key fixtures from `conftest.py`:

| Fixture | Type | Description |
|---------|------|-------------|
| `samples_dir` | Path | Path to `tests/samples/` |
| `orders_csv` | Path | Path to `orders.csv` |
| `customers_csv` | Path | Path to `customers.csv` |
| `products_csv` | Path | Path to `products.csv` |
| `tmp_output_dir` | Path | Temp directory for dashboard output |
| `tmp_parquet` | Path | Orders data as Parquet (requires duckdb) |
| `tmp_json` | Path | Orders data as JSON (requires duckdb) |
| `tmp_duckdb` | Path | Orders data in a .duckdb file (requires duckdb) |
| `duckdb_conn` | Connection | Fresh in-memory DuckDB connection |
| `sample_dataframe` | DataFrame | Orders data as pandas DataFrame (requires pandas) |

## Total Test Count

**~108 test scenarios** across all files, covering every layer of the architecture.
