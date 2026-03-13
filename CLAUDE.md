# dashboardmd — Project Instructions

## Overview

dashboardmd is a Python package that provides a **code-first analytics dashboard platform**.
It mirrors the same data model as Metabase, Looker, PowerBI, and Cube — but in Python,
for AI agents, outputting Markdown via notebookmd.

## Architecture

The package uses the same universal BI data model: Entities, Dimensions, Measures,
Relationships, Queries, and Dashboards. It builds on notebookmd for Markdown rendering.

```
dashboardmd/
├── __init__.py              # Public API: Dashboard, Entity, Dimension, Measure, etc.
├── model.py                 # Entity, Dimension, Measure, Relationship dataclasses
├── dashboard.py             # Dashboard: tiles, filters, sections, save()
├── query.py                 # Query builder: resolve joins, generate SQL/pandas
├── engine.py                # Execution engine: run queries against sources
├── time.py                  # Time intelligence: granularity, period comparison
├── suggest.py               # Auto-discovery: detect dimensions, suggest measures
├── refresh.py               # Re-run + metric diff tracking
├── cli.py                   # CLI entry point
├── py.typed                 # PEP 561 type checking marker
└── interop/                 # Platform connectors
    ├── __init__.py
    ├── metabase.py           # from_metabase() / to_metabase()
    ├── lookml.py             # from_lookml() / to_lookml()
    ├── cube.py               # from_cube() / to_cube_schema()
    └── powerbi.py            # from_powerbi() / to_powerbi()
```

### Core Concepts

| Concept | Class | Description |
|---------|-------|-------------|
| Logical table | `Entity` | A table/view with semantic meaning |
| Attribute | `Dimension` | An attribute you group and filter by |
| Aggregation | `Measure` | An aggregation you compute |
| Table link | `Relationship` | How entities join together |
| Query | `Query` | Select measures + dimensions → get results |
| Dashboard | `Dashboard` | Tiles bound to queries + global filters |

## Key Patterns

### Defining a model
```python
from dashboardmd import Dashboard, Entity, Dimension, Measure, Relationship

orders = Entity("orders", source="data/orders.csv", dimensions=[
    Dimension("id", type="number", primary_key=True),
    Dimension("date", type="time"),
    Dimension("status", type="string"),
], measures=[
    Measure("revenue", type="sum", sql="amount", format="$,.0f"),
    Measure("count", type="count"),
])
```

### Building a dashboard
```python
dash = Dashboard(
    title="Weekly Business Review",
    entities=[orders, customers, products],
    relationships=rels,
    output="output/weekly.md",
)

dash.filter("date_range", dimension="orders.date", default="last_30_days")
dash.section("Key Metrics")
dash.tile("orders.revenue", compare="previous_period")
dash.save()
```

## Dependencies

- **Core**: notebookmd (for Markdown rendering)
- **pandas**: `pip install "dashboardmd[pandas]"` — DataFrames, CSV sources
- **matplotlib**: `pip install "dashboardmd[plotting]"` — chart images
- **All**: `pip install "dashboardmd[all]"`

## Testing

```bash
pytest tests/ -v                    # Run all tests
pytest tests/unit/ -v               # Unit tests only
pytest tests/integration/ -v        # Integration tests only
```

## Code Style

- Python 3.11+, type hints everywhere
- Ruff formatting (120 char line length)
- isort via ruff
- Docstrings on all public methods

## Important Rules

- notebookmd is a required dependency — used for all Markdown rendering
- All optional deps (pandas, matplotlib) use try/except with graceful fallback
- Tests must pass with AND without optional dependencies
- The data model must map 1:1 to existing BI platforms (Metabase, Looker, PowerBI, Cube)
- Interop modules should enable import/export with BI platforms
