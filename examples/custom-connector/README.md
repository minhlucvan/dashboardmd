# Custom Connector Example

Demonstrates how to build custom `SourceHandler` subclasses to plug any data source into dashboardmd's DuckDB engine.

## Custom Connectors

Two connectors are implemented in `connectors.py`:

### APISource

Fetches JSON from a REST API endpoint and registers it as a DuckDB table.

```python
from connectors import APISource

source = APISource(
    url="https://api.example.com/users",
    headers={"Authorization": "Bearer token123"},
    json_path="data.users",  # optional: navigate nested JSON
)
```

Features:
- Supports custom headers for authentication
- Supports nested JSON extraction via `json_path`
- Auto-detects column types via DuckDB

### GeneratorSource

Creates a DuckDB table from any Python callable that returns a list of dicts.

```python
from connectors import GeneratorSource

def my_data():
    return [{"id": 1, "value": 42}, {"id": 2, "value": 99}]

source = GeneratorSource(factory=my_data)
```

Use cases:
- Generating synthetic/test data
- Reading custom file formats
- Transforming data in Python before loading
- Connecting to any Python-accessible data source (Redis, S3, internal APIs, etc.)

## Building Your Own Connector

Subclass `SourceHandler` and implement two methods:

```python
from dashboardmd.sources.base import SourceHandler

class MySource(SourceHandler):
    def register(self, conn, table_name):
        """Load your data into DuckDB as a table or view."""
        ...

    def describe(self):
        """Return schema metadata."""
        return {"columns": [("col_name", "col_type"), ...]}
```

Then pass it as an entity source:

```python
entity = Entity("my_table", source=MySource(...), dimensions=[...], measures=[...])
```

## Data

- **data/team.json** — generated at runtime, simulating an API response
- Project and task data are generated in-memory by `GeneratorSource`

## Running

```bash
cd examples/custom-connector
pip install dashboardmd
python dashboard.py
```

The script generates `dashboard.md` in this directory.
