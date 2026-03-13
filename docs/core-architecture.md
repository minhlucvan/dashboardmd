# dashboardmd — Core Architecture Proposal

**DuckDB-powered local analytics. MindsDB-inspired universal data connectivity. Markdown output.**

---

## Design Philosophy

Two projects provide the key architectural inspiration:

- **DuckDB** — an in-process analytical database that can query CSV, Parquet, JSON,
  PostgreSQL, MySQL, and remote files (S3/HTTP) without a server. It becomes our
  **universal query engine**: every data source is registered into DuckDB, and every
  analytical query is SQL executed locally.

- **MindsDB** — a platform with 300+ pluggable "handlers" that each know how to connect
  to a specific data source. We borrow this **handler/source plugin pattern** so that
  adding a new data source is just implementing a small Python class.

The result: **connect anything → model semantically → query locally via DuckDB → render to Markdown**.

---

## Architecture Overview

```
┌──────────────────────────────────────────────────────────────────────┐
│                        USER CODE (Python)                           │
│                                                                     │
│  orders = Entity("orders", source=Source.csv("data/orders.csv"))    │
│  dash = Dashboard(entities=[orders, ...])                           │
│  dash.tile("orders.revenue", by="customers.segment")                │
│  dash.save()                                                        │
└──────────────────────────┬───────────────────────────────────────────┘
                           │
          ┌────────────────┼────────────────┐
          ▼                ▼                ▼
   ┌─────────────┐  ┌───────────┐  ┌──────────────┐
   │ Source Layer │  │ Semantic  │  │ Render Layer │
   │ (handlers)  │  │   Layer   │  │ (notebookmd) │
   └──────┬──────┘  └─────┬─────┘  └──────┬───────┘
          │               │               │
          ▼               ▼               ▼
   ┌─────────────┐  ┌───────────┐  ┌──────────────┐
   │   DuckDB    │  │  Query    │  │  Markdown    │
   │  (engine)   │◄─┤  Builder  │  │  + assets/   │
   │  in-process │  │  (SQL)    │  │  (.md files) │
   └─────────────┘  └───────────┘  └──────────────┘
```

### The Four Layers

| Layer | Responsibility | Key Module |
|-------|---------------|------------|
| **Source Layer** | Connect to data, register tables in DuckDB | `sources/` |
| **Semantic Layer** | Entity, Dimension, Measure, Relationship definitions | `model.py` |
| **Query Layer** | Translate semantic queries → SQL, execute via DuckDB | `query.py`, `engine.py` |
| **Render Layer** | Turn query results → Markdown tiles, charts, tables | `dashboard.py` via notebookmd |

---

## Layer 1: Source Layer (MindsDB-Inspired Handlers)

### The Problem

BI tools need to connect to many data sources. The naive approach (if/elif chains) doesn't scale.
MindsDB solved this with pluggable handlers — each source type is a self-contained class.

### The Solution: `Source` Handlers

Each handler knows how to register its data into DuckDB. DuckDB already supports many
formats natively, so most handlers are thin wrappers.

```python
from dashboardmd import Source

# Files — DuckDB reads these natively
Source.csv("data/orders.csv")
Source.parquet("data/events.parquet")
Source.json("data/logs.json")

# Remote files — DuckDB httpfs extension
Source.csv("s3://bucket/orders.csv")
Source.parquet("https://data.example.com/events.parquet")

# Databases — DuckDB scanner extensions
Source.postgres("postgresql://user:pass@host/db", table="orders")
Source.mysql("mysql://user:pass@host/db", table="orders")
Source.duckdb("analytics.duckdb", table="orders")

# Pandas/Polars — DuckDB queries DataFrames directly
Source.dataframe(df)

# SQL query — any DuckDB-supported source via raw SQL
Source.sql("SELECT * FROM read_csv('data/orders.csv') WHERE status = 'active'")
```

### Handler Interface

```python
from dataclasses import dataclass
from abc import ABC, abstractmethod
import duckdb

class SourceHandler(ABC):
    """Base class for all data source handlers.

    Each handler knows how to register its data as a DuckDB table/view.
    Inspired by MindsDB's handler pattern but much simpler — we just
    need to get data into DuckDB, not build a full middleware.
    """

    @abstractmethod
    def register(self, conn: duckdb.DuckDBPyConnection, table_name: str) -> None:
        """Register this source as a table/view in the DuckDB connection."""
        ...

    @abstractmethod
    def describe(self) -> dict:
        """Return schema metadata: column names, types, row count estimate."""
        ...


@dataclass
class CSVSource(SourceHandler):
    path: str

    def register(self, conn, table_name):
        conn.execute(f"CREATE VIEW {table_name} AS SELECT * FROM read_csv_auto('{self.path}')")

    def describe(self):
        # Use DuckDB to sniff the schema
        conn = duckdb.connect()
        return conn.execute(f"DESCRIBE SELECT * FROM read_csv_auto('{self.path}')").fetchall()


@dataclass
class PostgresSource(SourceHandler):
    dsn: str
    table: str
    schema: str = "public"

    def register(self, conn, table_name):
        conn.execute(f"ATTACH '{self.dsn}' AS pg_db (TYPE postgres, READ_ONLY)")
        conn.execute(f"CREATE VIEW {table_name} AS SELECT * FROM pg_db.{self.schema}.{self.table}")


@dataclass
class DataFrameSource(SourceHandler):
    df: "pd.DataFrame"

    def register(self, conn, table_name):
        # DuckDB can query pandas DataFrames directly by name
        conn.register(table_name, self.df)
```

### Why DuckDB as the Universal Engine

Instead of implementing separate query execution for each source type (pandas for CSV,
psycopg2 for PostgreSQL, pymongo for MongoDB...), we funnel everything into DuckDB:

| Source | How DuckDB Accesses It |
|--------|----------------------|
| CSV / TSV | `read_csv_auto()` — native, zero-copy |
| Parquet | `read_parquet()` — native, columnar, fast |
| JSON | `read_json_auto()` — native |
| S3 / HTTP files | `httpfs` extension — streams remote Parquet/CSV |
| PostgreSQL | `postgres_scanner` extension — federated queries |
| MySQL | `mysql_scanner` extension — federated queries |
| SQLite | `sqlite_scanner` extension — attach directly |
| pandas DataFrame | `conn.register()` — zero-copy via Arrow |
| Polars DataFrame | Arrow interchange — zero-copy |
| DuckDB file | `ATTACH` — native |

**One SQL dialect. One query planner. One execution engine.** No matter where the data lives.

---

## Layer 2: Semantic Layer (The BI Data Model)

This layer is unchanged from the existing design — it's the universal BI model that maps
1:1 to Metabase, Looker, PowerBI, and Cube.

```python
@dataclass
class Dimension:
    name: str
    type: str            # "string" | "number" | "time" | "boolean"
    sql: str | None      # Column name or SQL expression (defaults to name)
    primary_key: bool
    format: str | None   # Display format string

@dataclass
class Measure:
    name: str
    type: str            # "sum" | "count" | "count_distinct" | "avg" | "min" | "max" | "number"
    sql: str | None      # Column or expression to aggregate
    format: str | None
    filters: list        # Pre-filters on the measure (e.g., count where status = 'completed')

@dataclass
class Relationship:
    from_entity: str
    to_entity: str
    on: tuple[str, str]  # (from_column, to_column)
    type: str            # "one_to_one" | "one_to_many" | "many_to_one" | "many_to_many"

@dataclass
class Entity:
    name: str
    source: SourceHandler
    dimensions: list[Dimension]
    measures: list[Measure]
```

### How Source + Semantic Connect

The `Entity` holds a `SourceHandler`. When the engine initializes, it calls
`source.register(conn, entity.name)` — which creates a DuckDB view for that entity.
From that point on, all queries reference the entity by name in SQL.

```python
# User writes:
orders = Entity("orders", source=Source.csv("data/orders.csv"), dimensions=[...], measures=[...])

# Engine does:
# 1. conn = duckdb.connect()                              → in-memory DuckDB
# 2. orders.source.register(conn, "orders")               → CREATE VIEW orders AS SELECT * FROM read_csv_auto(...)
# 3. Query("orders.revenue by customers.segment")         → SELECT segment, SUM(amount) FROM orders JOIN customers ...
# 4. conn.execute(sql)                                    → DuckDB runs it locally
```

---

## Layer 3: Query Layer (Semantic → SQL → DuckDB)

### Query Object

```python
@dataclass
class Query:
    measures: list[str]        # ["orders.revenue", "orders.count"]
    dimensions: list[str]      # ["customers.segment", "orders.date"]
    filters: list[tuple]       # [("orders.status", "equals", "completed")]
    time_granularity: str      # "day" | "week" | "month" | "quarter" | "year"
    sort: tuple | None
    limit: int | None
    compare: str | None        # "previous_period" | "previous_year"
```

### SQL Generation

The query builder translates semantic queries into SQL:

```python
# Input:  Query(measures=["orders.revenue"], dimensions=["customers.segment"])
#
# The builder:
# 1. Resolves "orders.revenue" → Measure(type="sum", sql="amount") on entity "orders"
# 2. Resolves "customers.segment" → Dimension on entity "customers"
# 3. Finds join path: orders → customers via Relationship(on=("customer_id", "id"))
# 4. Generates SQL:

SELECT
    customers.segment,
    SUM(orders.amount) AS revenue
FROM orders
JOIN customers ON orders.customer_id = customers.id
GROUP BY customers.segment
ORDER BY revenue DESC
```

### Automatic Join Resolution

The relationship graph is used to find the shortest join path between entities
referenced in a query. This mirrors how Looker resolves explores and how Cube
resolves join paths.

```python
class JoinResolver:
    """Build a graph from Relationships, find shortest path between entities."""

    def __init__(self, relationships: list[Relationship]):
        self.graph = self._build_graph(relationships)

    def resolve(self, entities: set[str]) -> list[JoinClause]:
        """Given a set of entities needed, return the JOIN clauses."""
        # BFS/shortest-path through the relationship graph
        ...
```

### Engine: Execute Against DuckDB

```python
class Engine:
    def __init__(self, entities: list[Entity], relationships: list[Relationship]):
        self.conn = duckdb.connect()  # In-memory by default
        self.resolver = JoinResolver(relationships)

        # Register all sources
        for entity in entities:
            entity.source.register(self.conn, entity.name)

    def execute(self, query: Query) -> "pd.DataFrame":
        sql = self._build_sql(query)
        return self.conn.execute(sql).fetchdf()
```

---

## Layer 4: Render Layer (Results → Markdown)

Uses `notebookmd` to render query results into Markdown:

```python
class Dashboard:
    def save(self):
        nb = notebookmd.Notebook()

        for section in self.sections:
            nb.heading(section.title, level=2)

            for tile in section.tiles:
                result = self.engine.execute(tile.query)

                match tile.viz_type:
                    case "metric":
                        nb.metric(tile.label, result.iloc[0, 0], format=tile.format)
                    case "table":
                        nb.table(result)
                    case "bar_chart":
                        nb.bar_chart(result, x=tile.dimension, y=tile.measure)
                    case "line_chart":
                        nb.line_chart(result, x=tile.dimension, y=tile.measure)

        nb.save(self.output)
```

---

## Revised File Structure

```
dashboardmd/
├── __init__.py              # Public API
├── model.py                 # Entity, Dimension, Measure, Relationship
├── dashboard.py             # Dashboard: tiles, filters, sections, save()
├── query.py                 # Query builder: semantic → SQL
├── engine.py                # DuckDB execution engine
├── time.py                  # Time intelligence: granularity, period comparison
├── suggest.py               # Auto-discovery: detect dimensions, suggest measures
├── refresh.py               # Re-run + metric diff tracking
├── cli.py                   # CLI entry point
├── py.typed                 # PEP 561 marker
├── sources/                 # Data source handlers (NEW)
│   ├── __init__.py          # Source factory: Source.csv(), Source.postgres(), etc.
│   ├── base.py              # SourceHandler ABC
│   ├── file.py              # CSVSource, ParquetSource, JSONSource
│   ├── database.py          # PostgresSource, MySQLSource, SQLiteSource, DuckDBSource
│   ├── dataframe.py         # DataFrameSource (pandas/polars)
│   └── sql.py               # RawSQLSource
└── interop/                 # BI platform connectors
    ├── __init__.py
    ├── metabase.py
    ├── lookml.py
    ├── cube.py
    └── powerbi.py
```

### Dependency Strategy

```
dashboardmd (core)
├── notebookmd          # Required — Markdown rendering
└── duckdb              # Required — the query engine

dashboardmd[pandas]
└── pandas              # Optional — DataFrame interchange

dashboardmd[postgres]
└── (nothing extra)     # DuckDB postgres_scanner extension auto-loads

dashboardmd[mysql]
└── (nothing extra)     # DuckDB mysql_scanner extension auto-loads

dashboardmd[plotting]
└── matplotlib          # Optional — chart images

dashboardmd[all]
└── everything above
```

DuckDB is a single pip package (`duckdb`) with no system dependencies. Its extensions
(httpfs, postgres_scanner, mysql_scanner) auto-install on first use.

---

## End-to-End Example

```python
from dashboardmd import Dashboard, Entity, Dimension, Measure, Relationship, Source

# === Connect to anything ===

# CSV file on disk
orders = Entity("orders", source=Source.csv("data/orders.csv"), dimensions=[
    Dimension("id", type="number", primary_key=True),
    Dimension("date", type="time"),
    Dimension("customer_id", type="number"),
    Dimension("status", type="string"),
], measures=[
    Measure("revenue", type="sum", sql="amount", format="$,.0f"),
    Measure("count", type="count"),
])

# PostgreSQL database
customers = Entity("customers",
    source=Source.postgres("postgresql://user:pass@host/db", table="customers"),
    dimensions=[
        Dimension("id", type="number", primary_key=True),
        Dimension("name", type="string"),
        Dimension("segment", type="string"),
        Dimension("region", type="string"),
    ],
)

# Parquet on S3
events = Entity("events",
    source=Source.parquet("s3://analytics-bucket/events/*.parquet"),
    dimensions=[
        Dimension("event_id", type="number", primary_key=True),
        Dimension("event_type", type="string"),
        Dimension("timestamp", type="time"),
    ],
    measures=[
        Measure("count", type="count"),
    ],
)

# === Define relationships ===
rels = [
    Relationship("orders", "customers", on=("customer_id", "id"), type="many_to_one"),
]

# === Build dashboard ===
dash = Dashboard(
    title="Cross-Source Business Review",
    entities=[orders, customers, events],
    relationships=rels,
    output="output/review.md",
)

dash.filter("date_range", dimension="orders.date", default="last_30_days")

dash.section("Key Metrics")
dash.tile("orders.revenue", compare="previous_period")
dash.tile("orders.count", compare="previous_period")

dash.section("Revenue by Segment")
dash.tile("orders.revenue", by="customers.segment", type="bar_chart")

dash.save()  # → DuckDB joins CSV + PostgreSQL, renders Markdown
```

**What happens when `dash.save()` is called:**

1. `Engine.__init__()` creates an in-memory DuckDB connection
2. `CSVSource.register()` → `CREATE VIEW orders AS SELECT * FROM read_csv_auto('data/orders.csv')`
3. `PostgresSource.register()` → `ATTACH 'postgresql://...' AS pg; CREATE VIEW customers AS SELECT * FROM pg.public.customers`
4. `ParquetSource.register()` → `CREATE VIEW events AS SELECT * FROM read_parquet('s3://...')`
5. For each tile, `Query` is built from the tile definition
6. `QueryBuilder` resolves joins via the relationship graph and generates SQL
7. `Engine.execute(sql)` → DuckDB runs the query across all sources locally
8. Results are passed to `notebookmd` for Markdown rendering
9. `output/review.md` is written to disk

---

## Why This Architecture

### DuckDB over raw pandas

| Concern | pandas only | DuckDB |
|---------|-------------|--------|
| Query language | Custom Python code per source | Standard SQL for everything |
| Multi-source joins | Manual merge/concat | Native federated JOIN |
| Performance on large data | Slow, memory-bound | Vectorized, columnar, out-of-core |
| Remote files (S3/HTTP) | Needs boto3 + custom code | Built-in httpfs extension |
| Database connectivity | Needs psycopg2, pymysql, etc. | Built-in scanner extensions |
| Aggregations + GROUP BY | `.groupby().agg()` per query | SQL GROUP BY, optimized |

### MindsDB-inspired handlers over hardcoded sources

| Concern | Hardcoded if/elif | Handler pattern |
|---------|-------------------|-----------------|
| Adding new sources | Modify engine.py | Add a new file in `sources/` |
| Source-specific logic | Scattered in engine | Encapsulated in handler |
| Testing | Hard to isolate | Test each handler independently |
| Community contributions | High barrier | Low barrier — implement one class |

### Compared to full MindsDB

dashboardmd does NOT try to be MindsDB. MindsDB is a server with 300+ handlers, AI model
management, and agent orchestration. dashboardmd is a **library** — no server, no daemon,
no infrastructure. We take only the **handler abstraction pattern** from MindsDB and use
DuckDB (not MindsDB's own query engine) for execution.

---

## Auto-Discovery (Enhanced with DuckDB)

DuckDB can introspect schemas, making auto-discovery more powerful:

```python
from dashboardmd import discover

# Scan a directory — DuckDB sniffs all CSV/Parquet/JSON files
entities = discover("data/")

# Scan a database — DuckDB reads the catalog
entities = discover(Source.postgres("postgresql://user:pass@host/db"))

# Auto-build a dashboard
dash = Dashboard.from_entities(entities, auto_join=True)
dash.auto_dashboard()
dash.save()
```

Under the hood, `discover()` uses DuckDB's `DESCRIBE` and column statistics to:
- Detect dimension types (string, number, time) from column types
- Suggest measures from numeric columns (SUM, AVG, COUNT)
- Detect foreign keys from column naming patterns (`*_id` → likely FK)

---

## Implementation Phases (Revised)

### Phase 1: Foundation
- `sources/base.py` — `SourceHandler` ABC
- `sources/file.py` — CSV, Parquet, JSON handlers
- `sources/dataframe.py` — pandas DataFrame handler
- `model.py` — Entity, Dimension, Measure, Relationship dataclasses
- `engine.py` — DuckDB-based engine (create connection, register sources)
- `query.py` — Basic SQL generation (single entity, no joins)
- `dashboard.py` — Dashboard with section/tile/filter/save via notebookmd

### Phase 2: Query Engine
- Join resolution from relationship graph
- Multi-entity SQL generation
- Time granularity (DATE_TRUNC in SQL)
- Period comparison queries
- Smart viz type selection

### Phase 3: Database Sources + Remote
- `sources/database.py` — PostgreSQL, MySQL, SQLite handlers
- Remote file support via httpfs (S3, HTTP)
- DuckDB extension auto-loading

### Phase 4: Auto-Discovery
- `suggest.py` — `discover()`, `suggest_measures()`, `auto_join()`
- Schema introspection via DuckDB

### Phase 5: Interop + Operations
- BI platform import/export (Metabase, Looker, Cube, PowerBI)
- `refresh()` with metric diff
- CLI interface

---

## Research References

- [MindsDB 2025: Universal AI Data Hub](https://mindsdb.com/blog/mindsdb-in-2025-from-sql-to-the-universal-ai-data-hub) — handler pattern, federated query design
- [MindsDB Data Integrations](https://docs.mindsdb.com/integrations/data-overview) — pluggable handler architecture
- [DuckDB PostgreSQL Extension](https://duckdb.org/docs/stable/core_extensions/postgres) — federated database queries
- [DuckDB httpfs Extension](https://duckdb.org/docs/stable/core_extensions/httpfs/overview) — remote file access (S3/HTTP)
- [DuckDB Python Analytics Guide](https://www.kdnuggets.com/integrating-duckdb-python-an-analytics-guide) — Python integration patterns
- [DuckDB Enterprise Integration Patterns](https://endjin.com/blog/2025/04/duckdb-in-practice-enterprise-integration-architectural-patterns) — architectural patterns
- [MindsDB GitHub](https://github.com/mindsdb/mindsdb) — handler implementation reference
