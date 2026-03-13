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

```
┌─────────────────────────────────────────────────────────────┐
│                                                             │
│   Metabase  ◄──────┐                                       │
│   Looker    ◄──────┤   dashboardmd                         │
│   PowerBI   ◄──────┤   (same concepts, Python API)         │
│   Cube      ◄──────┘                                       │
│                         │                                   │
│                         ▼                                   │
│                    Markdown report                          │
│                    (.md + assets/)                          │
│                                                             │
└─────────────────────────────────────────────────────────────┘
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

```python
from dashboardmd import Dashboard, Entity, Dimension, Measure, Relationship

# Define entities (like Looker views, PowerBI tables, Cube cubes)
orders = Entity("orders", source="data/orders.csv", dimensions=[
    Dimension("id", type="number", primary_key=True),
    Dimension("date", type="time"),
    Dimension("customer_id", type="number"),
    Dimension("status", type="string"),
], measures=[
    Measure("revenue", type="sum", sql="amount", format="$,.0f"),
    Measure("count", type="count"),
    Measure("aov", type="number", sql="revenue / count", format="$,.2f"),
])

customers = Entity("customers", source="data/customers.csv", dimensions=[
    Dimension("id", type="number", primary_key=True),
    Dimension("name", type="string"),
    Dimension("segment", type="string"),
    Dimension("region", type="string"),
])

# Define relationships (like Looker joins, PowerBI relationships)
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

## BI Platform Interop

```python
# Import from Metabase
from dashboardmd.interop import from_metabase
dash = from_metabase(url="https://metabase.company.com", api_key="mb_xxx", dashboard_id=42)
dash.save()  # → Markdown version of the Metabase dashboard

# Export to Cube schema
from dashboardmd.interop import to_cube_schema
to_cube_schema(dash, output_dir="cube/schema/")
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
