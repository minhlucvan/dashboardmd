"""Custom source connectors for dashboardmd.

Shows how to build your own SourceHandler subclasses to plug any data
source into dashboardmd's DuckDB engine.

Two examples:
    1. APISource     — fetches JSON from a REST API endpoint
    2. GeneratorSource — creates data from a Python callable (factory function)
"""

from __future__ import annotations

import json
import tempfile
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Callable

from dashboardmd.sources.base import SourceHandler

if TYPE_CHECKING:
    import duckdb


# ---------------------------------------------------------------------------
# 1. REST API connector
# ---------------------------------------------------------------------------


@dataclass
class APISource(SourceHandler):
    """Fetch JSON from a REST API and register it as a DuckDB table.

    The endpoint must return a JSON array of objects, e.g.:
        [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]

    Optionally, set ``json_path`` to extract a nested key:
        {"data": {"users": [...]}}  →  json_path="data.users"

    Headers can be passed for authentication:
        APISource(url="https://api.example.com/users",
                  headers={"Authorization": "Bearer token123"})
    """

    url: str
    headers: dict[str, str] = field(default_factory=dict)
    json_path: str | None = None
    timeout: int = 30

    def _fetch(self) -> list[dict[str, Any]]:
        """Fetch JSON data from the API."""
        import urllib.request

        req = urllib.request.Request(self.url, headers=self.headers)
        with urllib.request.urlopen(req, timeout=self.timeout) as resp:
            data = json.loads(resp.read().decode())

        # Navigate into nested JSON if json_path is set
        if self.json_path:
            for key in self.json_path.split("."):
                data = data[key]

        if not isinstance(data, list):
            raise ValueError(f"Expected a JSON array, got {type(data).__name__}")
        return data

    def register(self, conn: duckdb.DuckDBPyConnection, table_name: str) -> None:
        """Fetch data from the API and register as a DuckDB table."""
        rows = self._fetch()
        if not rows:
            conn.execute(f'CREATE OR REPLACE TABLE "{table_name}" AS SELECT 1 WHERE false')
            return

        # Write to a temp file so DuckDB can read it with type inference
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(rows, f)
            tmp_path = f.name

        conn.execute(
            f"CREATE OR REPLACE TABLE \"{table_name}\" AS "
            f"SELECT * FROM read_json_auto('{tmp_path}')"
        )

    def describe(self) -> dict[str, Any]:
        """Describe the schema by inspecting the first response."""
        rows = self._fetch()
        if not rows:
            return {"columns": []}
        return {"columns": [(k, type(v).__name__) for k, v in rows[0].items()]}


# ---------------------------------------------------------------------------
# 2. Python generator / factory connector
# ---------------------------------------------------------------------------


@dataclass
class GeneratorSource(SourceHandler):
    """Create a DuckDB table from a Python callable that returns rows.

    The callable must return a list of dicts:
        def my_data():
            return [{"id": 1, "value": 42}, {"id": 2, "value": 99}]

        source = GeneratorSource(factory=my_data)

    This is useful for:
    - Generating synthetic/test data
    - Reading from custom file formats
    - Transforming data in Python before loading
    - Connecting to any Python-accessible data source
    """

    factory: Callable[[], list[dict[str, Any]]]
    columns: list[tuple[str, str]] | None = None  # Optional schema hint

    def register(self, conn: duckdb.DuckDBPyConnection, table_name: str) -> None:
        """Call the factory function and register the result as a DuckDB table."""
        rows = self.factory()
        if not rows:
            conn.execute(f'CREATE OR REPLACE TABLE "{table_name}" AS SELECT 1 WHERE false')
            return

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(rows, f, default=str)
            tmp_path = f.name

        conn.execute(
            f"CREATE OR REPLACE TABLE \"{table_name}\" AS "
            f"SELECT * FROM read_json_auto('{tmp_path}')"
        )

    def describe(self) -> dict[str, Any]:
        """Describe the schema."""
        if self.columns:
            return {"columns": self.columns}
        rows = self.factory()
        if not rows:
            return {"columns": []}
        return {"columns": [(k, type(v).__name__) for k, v in rows[0].items()]}
