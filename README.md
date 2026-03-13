# dashboardmd

**Code-first analytics dashboard platform. Markdown output. Mirrors BI platform concepts.**

> Same data model as Metabase, Looker, PowerBI, Cube — but in Python, for agents,
> outputting Markdown. Import from them. Export to them. Or use standalone.

## The Core Idea

Every BI platform — Metabase, Looker, PowerBI, Cube — uses the same foundational concepts:

1. **Entities** (tables/views with semantic meaning)
2. **Dimensions** (attributes you group and filter by)
3. **Measures** (aggregations you compute)
4. **Relationships** (how entities join together)
5. **Queries** (select measures + dimensions → get results)
6. **Dashboards** (tiles bound to queries + global filters)

dashboardmd mirrors these concepts exactly in Python. The result is a data model that agents can
build programmatically, that maps 1:1 to existing BI platforms, and that renders to Markdown.

It connects to data sources you already use — **CSV**, **Parquet**, **JSON**, **DuckDB**,
**PostgreSQL**, **MySQL**, **SQLite**, **pandas DataFrames** — and interops with BI platforms.

```
     Data Sources                      BI Platforms
  ┌─────────────────────┐        ┌─────────────────────┐
  │ CSV / Parquet / JSON │        │ Metabase            │
  │ PostgreSQL / MySQL  │        │ Looker / LookML     │
  │ SQLite / DuckDB     │        │ Power BI            │
  │ pandas DataFrames   │        │ Cube.js             │
  └───────────┬─────────┘        └───────────┬─────────┘
              │                              │
              └──────────────┬───────────────┘
                             ▼
                    ┌─────────────────┐
                    │   dashboardmd   │
                    │    Analyst      │  ← DuckDB engine
                    │  (core SQL)    │
                    └─────────┬───────┘
                              │
                              ▼
                ┌──────────────────────────┐
                │      Markdown Report     │
                │        (.md files)       │
                └──────────────────────────┘
```

## Installation

```bash
pip install dashboardmd
```

With optional dependencies:

```bash
pip install "dashboardmd[pandas]"      # DataFrame support
pip install "dashboardmd[plotting]"    # Chart images via matplotlib
pip install "dashboardmd[all]"         # Everything
```

## Quick Start

### Direct SQL with Analyst (maximum agent power)

The `Analyst` is the core of dashboardmd — a DuckDB-powered query engine that gives
AI agents full analytical SQL capabilities over any data source.

```python
from dashboardmd import Analyst

# Create an analyst and register data sources
analyst = Analyst()
analyst.add("orders", "data/orders.csv")
analyst.add("customers", "data/customers.csv")

# Full DuckDB SQL — aggregations, window functions, CTEs, JOINs, pivots, regex...
result = analyst.sql("""
    SELECT
        c.segment,
        COUNT(*) AS order_count,
        SUM(o.amount) AS revenue,
        AVG(o.amount) AS avg_order_value
    FROM orders o
    JOIN customers c ON o.customer_id = c.id
    WHERE o.status = 'completed'
    GROUP BY 1
    ORDER BY revenue DESC
""")

# Multiple output formats
result.show()                  # Print to stdout
result.to_markdown_table()     # Render as Markdown table
result.df()                    # Return as pandas DataFrame
result.fetchall()              # Raw tuples
result.scalar()                # Single value

# Schema exploration (great for agents)
analyst.tables()               # List all tables
analyst.schema("orders")       # Column names and types
analyst.sample("orders", 5)    # Preview rows
analyst.count("orders")        # Row count

# Write results to Markdown
analyst.to_md("output/analysis.md", title="Revenue Analysis", queries=[
    ("By Segment", "SELECT segment, SUM(amount) FROM orders GROUP BY 1"),
    ("Top 10", "SELECT * FROM orders ORDER BY amount DESC LIMIT 10"),
])
```

### Semantic Queries (BI-style)

Build on top of Analyst with entities, dimensions, and measures — same concepts as
Looker, PowerBI, Cube, and Metabase.

```python
from dashboardmd import Analyst, Entity, Dimension, Measure, Relationship

# Define entities (like Looker views, PowerBI tables, Cube cubes)
orders = Entity("orders", source="data/orders.csv", dimensions=[
    Dimension("id", type="number", primary_key=True),
    Dimension("date", type="time"),
    Dimension("customer_id", type="number"),
    Dimension("status", type="string"),
], measures=[
    Measure("revenue", type="sum", sql="amount"),
    Measure("count", type="count"),
])

# Register entities and query semantically
analyst = Analyst()
analyst.add_entity(orders)
result = analyst.query(measures=["orders.revenue"], dimensions=["orders.status"])
print(result.to_markdown_table())
```

### Dashboards (structured reports)

```python
from dashboardmd import Dashboard, Entity, Dimension, Measure, Relationship

orders = Entity("orders", source="data/orders.csv", dimensions=[
    Dimension("id", type="number", primary_key=True),
    Dimension("date", type="time"),
    Dimension("customer_id", type="number"),
    Dimension("status", type="string"),
], measures=[
    Measure("revenue", type="sum", sql="amount", format="$,.0f"),
    Measure("count", type="count"),
])

customers = Entity("customers", source="data/customers.csv", dimensions=[
    Dimension("id", type="number", primary_key=True),
    Dimension("name", type="string"),
    Dimension("segment", type="string"),
])

rels = [
    Relationship("orders", "customers", on=("customer_id", "id"), type="many_to_one"),
]

# Build the dashboard
dash = Dashboard(
    title="Weekly Business Review",
    entities=[orders, customers],
    relationships=rels,
    output="output/weekly.md",
)

dash.filter("date_range", dimension="orders.date", default="last_30_days")

dash.section("Key Metrics")
dash.tile("orders.revenue", compare="previous_period")
dash.tile("orders.count", compare="previous_period")

dash.section("Revenue by Segment")
dash.tile("orders.revenue", by="customers.segment", type="bar_chart")

# Raw SQL tiles — full DuckDB power inside dashboards
dash.section("Custom Analysis")
dash.tile_sql("Top Customers", """
    SELECT c.name, SUM(o.amount) AS total
    FROM orders o JOIN customers c ON o.customer_id = c.id
    GROUP BY 1 ORDER BY 2 DESC LIMIT 5
""")

dash.save()  # → output/weekly.md
```

## Concept Mapping

| Concept | Looker | PowerBI | Cube | Metabase | **dashboardmd** |
|---------|--------|---------|------|----------|:--:|
| Logical table | View | Table | Cube | Table | **Entity** |
| Attribute | dimension | Column | dimension | Column | **Dimension** |
| Aggregation | measure | DAX Measure | measure | Metric | **Measure** |
| Table link | explore + join | Relationship | joins | FK | **Relationship** |
| Query | Explore query | Visual query | /load API | Question | **Query** |
| Dashboard | Dashboard + tiles | Report + visuals | (frontend) | Dashboard + cards | **Dashboard + Tiles** |

## BI Platform Interop

Convert between dashboardmd and any BI platform's data model:

```python
from dashboardmd.interop import from_metabase, to_metabase
from dashboardmd.interop import from_lookml, to_lookml
from dashboardmd.interop import from_cube, to_cube_schema
from dashboardmd.interop import from_powerbi, to_powerbi

# Import from Metabase metadata export
entities, relationships = from_metabase(metabase_metadata_dict)

# Export to Cube.js schema
cube_schema = to_cube_schema(entities, relationships)

# Import from LookML model
entities, relationships = from_lookml(lookml_model_dict)

# Export to PowerBI tabular model
powerbi_model = to_powerbi(entities, relationships)
```

## CLI

```bash
dashboardmd discover data/          # Auto-discover entities from data files
dashboardmd query data/ "SELECT COUNT(*) FROM orders"   # Run SQL against data
dashboardmd run dashboard.py        # Execute a dashboard script
```

## Development

```bash
# Install in development mode
make install

# Run tests
make test

# Run linter + type checker + tests
make check
```

## Project Status

dashboardmd is in early development (v0.1.0). See the [design proposal](docs/analytics-dashboard-proposal.md) for the full vision and implementation plan.

## License

[MIT](LICENSE)
