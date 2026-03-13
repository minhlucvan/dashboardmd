# dashboardmd

**Code-first analytics dashboard platform. Connectors for everything. Markdown output.**

> Universal data model — same concepts as Metabase, Looker, PowerBI, Cube —
> but in Python, for agents, with composable connectors that work together.

## The Core Idea

Every analytics platform uses the same foundational concepts:

1. **Entities** (tables/views with semantic meaning)
2. **Dimensions** (attributes you group and filter by)
3. **Measures** (aggregations you compute)
4. **Relationships** (how entities join together)
5. **Queries** (select measures + dimensions → get results)
6. **Dashboards** (tiles bound to queries + global filters)

dashboardmd mirrors these concepts in Python. **Connectors** are the universal
integration layer — whether your data comes from CSV files, REST APIs, BI
platforms, or community packages, everything plugs into the same system.

```
     Connectors                           Built-in
  ┌──────────────────────────┐     ┌───────────────────────┐
  │ Data Connectors          │     │ File Sources           │
  │  • Custom (your code)    │     │  • CSV / Parquet / JSON│
  │  • Community (pip)       │     │  • DuckDB / SQLite     │
  │  • API / Callable        │     │  • PostgreSQL / MySQL  │
  │                          │     │  • pandas DataFrames   │
  │ BI Platform Connectors   │     │                       │
  │  • MetabaseConnector     │     │ Semantic Layer         │
  │  • LookMLConnector       │     │  • Entity / Dimension  │
  │  • CubeConnector         │     │  • Measure / Query     │
  │  • PowerBIConnector      │     │  • Relationship        │
  └────────────┬─────────────┘     └───────────┬───────────┘
               │                               │
               └───────────────┬───────────────┘
                               ▼
                    ┌──────────────────┐
                    │   dashboardmd    │
                    │    Analyst       │  ← DuckDB engine
                    │  analyst.use()   │  ← composable connectors
                    └────────┬─────────┘
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

## Connectors

Connectors are the universal integration layer. A connector bundles everything
needed to analyze a data domain: data sources, cleaning, entity definitions
(dimensions/measures), relationships, and pre-built dashboard widgets.

### Composable by design

Multiple connectors install into one Analyst and work together:

```python
from dashboardmd import Analyst, Relationship
from dashboardmd.connectors import MetabaseConnector

analyst = Analyst()

# BI platform connector — import your existing Metabase model
analyst.use(MetabaseConnector(metabase_metadata))

# Community connector — pip install dashboardmd-github
analyst.use(GitHubConnector(token="...", repo="org/repo"))

# Custom connector — your own data
analyst.use(MyInternalConnector(api_key="..."))

# Cross-connector joins — link data across boundaries
analyst.add_relationship(Relationship(
    "orders", "pull_requests", on=("id", "order_id"), type="one_to_many"
))

# Query across everything as one system
analyst.query(
    measures=["orders.revenue"],
    dimensions=["pull_requests.author"],
)
```

### BI Platform Connectors (built-in)

Import data models from any BI platform. They implement the same
`Connector` interface, so they compose with data connectors:

```python
from dashboardmd import Analyst
from dashboardmd.connectors import MetabaseConnector, LookMLConnector, CubeConnector, PowerBIConnector

# Import from Metabase
analyst = Analyst()
analyst.use(MetabaseConnector(metabase_metadata_dict))

# Import from LookML / Looker
analyst.use(LookMLConnector(lookml_model_dict))

# Import from Cube.js
analyst.use(CubeConnector(cube_schema_dict))

# Import from PowerBI
analyst.use(PowerBIConnector(powerbi_model_dict))
```

### Custom Connectors

Build your own connector for any data domain:

```python
from dashboardmd import Connector, DashboardWidget, Entity, Dimension, Measure
from dashboardmd.sources.base import SourceHandler

class MyAPISource(SourceHandler):
    def __init__(self, url, token):
        self.url = url
        self.token = token

    def register(self, conn, table_name):
        rows = self._fetch()           # your API logic
        rows = self._clean(rows)       # your cleaning logic
        self._register_rows(conn, table_name, rows)  # built-in helper

    def describe(self):
        return {"columns": []}

class MyConnector(Connector):
    def __init__(self, api_key):
        self.api_key = api_key

    def name(self):
        return "my_service"

    def sources(self):
        return {"events": MyAPISource("https://api.example.com/events", self.api_key)}

    def entities(self):
        return [Entity("events", dimensions=[
            Dimension("id", type="number", primary_key=True),
            Dimension("type", type="string"),
            Dimension("timestamp", type="time"),
        ], measures=[
            Measure("count", type="count"),
        ])]

    def widgets(self):
        return [DashboardWidget(
            name="Overview",
            title="Event Analytics",
            requires=["events"],
            build=self._build_overview,
        )]

    def _build_overview(self, dash):
        dash.section("Events")
        dash.tile("events.count", by="events.type")

# Use it
analyst = Analyst()
analyst.use(MyConnector(api_key="..."))
```

### Multi-Connector Dashboards

Connectors contribute widgets to shared dashboards:

```python
from dashboardmd import Analyst, Dashboard, Relationship

analyst = Analyst()
analyst.use(project_connector)
analyst.use(team_connector)

# Cross-connector relationship
analyst.add_relationship(Relationship(
    "tasks", "team_members", on=("assignee", "username"), type="many_to_one"
))

# Build unified dashboard with widgets from both connectors
dash = Dashboard(title="Engineering Velocity", analyst=analyst, output="velocity.md")
project_connector.contribute_widgets(dash, ["Project Status"])
team_connector.contribute_widgets(dash, ["Team Overview"])

# Add custom cross-connector analysis
dash.section("Cross-Team Insights")
dash.tile_sql("Hours by Department", """
    SELECT tm.department, SUM(tl.hours) as total
    FROM time_logs tl JOIN team_members tm ON tl.username = tm.username
    GROUP BY 1 ORDER BY 2 DESC
""")

dash.save()
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
| Integration | — | — | — | — | **Connector** |

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
