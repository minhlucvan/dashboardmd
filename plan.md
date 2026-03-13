# Connector Plugin API — Full-Stack Data Connectors

## Vision

A **Connector** is a full-stack plugin that bundles everything needed to analyze a data domain:
data fetching, cleaning, semantic model (entities/dimensions/measures/relationships),
and pre-built dashboard widgets. Think Grafana data source plugins with pre-built dashboards.

```python
# End-user experience
from dashboardmd_github import GitHubConnector

gh = GitHubConnector(token="ghp_...", repo="org/repo")

analyst = Analyst()
analyst.use(gh)                    # registers all tables, entities, relationships

# Use pre-built dashboard
dash = gh.dashboard("PR Review")   # complete dashboard, ready to save
dash.save()

# Or cherry-pick entities for custom analysis
analyst.query(measures=["pull_requests.avg_merge_time"], dimensions=["pull_requests.author"])
```

## Architecture

### Layer 1: `Connector` base class

```
dashboardmd/connector.py
```

```python
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Callable

if TYPE_CHECKING:
    import duckdb
    from dashboardmd.analyst import Analyst
    from dashboardmd.dashboard import Dashboard

from dashboardmd.model import Dimension, Entity, Measure, Relationship
from dashboardmd.sources.base import SourceHandler


@dataclass
class DashboardWidget:
    """A pre-built dashboard section contributed by a connector."""

    name: str
    title: str
    description: str = ""
    build: Callable[[Dashboard], None] = lambda d: None


class Connector(ABC):
    """Full-stack data connector: source + model + dashboards.

    Subclass this to create a complete analytics package for a data domain.
    A connector provides:
    1. Data sources (tables registered in DuckDB)
    2. Entities with dimensions and measures (semantic model)
    3. Relationships between entities
    4. Pre-built dashboard widgets
    """

    @abstractmethod
    def name(self) -> str:
        """Unique connector name, e.g. 'github', 'stripe', 'jira'."""
        ...

    @abstractmethod
    def sources(self) -> dict[str, SourceHandler]:
        """Return data sources to register.

        Keys are table names, values are SourceHandler instances.
        Data cleaning/transformation happens inside each SourceHandler.

        Returns:
            {"pull_requests": GitHubPRSource(...), "issues": GitHubIssueSource(...)}
        """
        ...

    @abstractmethod
    def entities(self) -> list[Entity]:
        """Return pre-defined entities with dimensions and measures.

        Each entity's source should match a key from sources().
        """
        ...

    def relationships(self) -> list[Relationship]:
        """Return relationships between entities. Override to customize."""
        return []

    def widgets(self) -> list[DashboardWidget]:
        """Return pre-built dashboard widgets. Override to customize."""
        return []

    # ------------------------------------------------------------------
    # Registration (called by Analyst.use())
    # ------------------------------------------------------------------

    def register(self, analyst: Analyst) -> None:
        """Register all sources, entities, and relationships into an Analyst.

        This is called by analyst.use(connector). Override for custom setup.
        """
        # 1. Register data sources
        for table_name, source in self.sources().items():
            analyst.add(table_name, source)

        # 2. Register entities (sources already loaded, so set source=None)
        for entity in self.entities():
            # Entity source is already registered above, just add the semantic layer
            entity_copy = Entity(
                name=entity.name,
                source=None,  # already registered
                dimensions=entity.dimensions,
                measures=entity.measures,
            )
            analyst._entities[entity.name] = entity_copy
            analyst._query_builder = None

        # 3. Register relationships
        rels = self.relationships()
        if rels:
            analyst._relationships.extend(rels)
            analyst._query_builder = None

    # ------------------------------------------------------------------
    # Dashboard factory
    # ------------------------------------------------------------------

    def dashboard(
        self,
        name: str | None = None,
        output: str = "output/dashboard.md",
        analyst: Analyst | None = None,
    ) -> Dashboard:
        """Create a pre-built dashboard.

        Args:
            name: Widget name to use, or None for all widgets.
            output: Output file path.
            analyst: Existing Analyst, or creates a new one.

        Returns:
            A Dashboard ready to customize or save().
        """
        from dashboardmd.dashboard import Dashboard

        if analyst is None:
            from dashboardmd.analyst import Analyst as AnalystCls
            analyst = AnalystCls()
            self.register(analyst)

        dash = Dashboard(
            title=name or f"{self.name().title()} Analytics",
            entities=self.entities(),
            relationships=self.relationships(),
            output=output,
            analyst=analyst,
        )

        # Apply widgets
        available = self.widgets()
        for widget in available:
            if name is None or widget.name == name:
                widget.build(dash)

        return dash

    def available_dashboards(self) -> list[str]:
        """List available pre-built dashboard names."""
        return [w.name for w in self.widgets()]
```

### Layer 2: Helper source for connectors — `_register_rows()`

Add to `SourceHandler` base so connector authors don't need temp-file boilerplate:

```python
# In sources/base.py — add helper method

class SourceHandler(ABC):
    @abstractmethod
    def register(self, conn, table_name): ...

    @abstractmethod
    def describe(self) -> dict[str, Any]: ...

    def _register_rows(self, conn, table_name, rows: list[dict]) -> None:
        """Helper: load a list of dicts into DuckDB as a table."""
        import json, os, tempfile

        if not rows:
            conn.execute(f'CREATE OR REPLACE TABLE "{table_name}" (placeholder INT)')
            conn.execute(f'DELETE FROM "{table_name}"')
            return
        tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False)
        try:
            json.dump(rows, tmp, default=str)
            tmp.close()
            conn.execute(
                f'CREATE OR REPLACE TABLE "{table_name}" AS '
                f"SELECT * FROM read_json_auto('{tmp.name}')"
            )
        finally:
            os.unlink(tmp.name)
```

### Layer 3: `Analyst.use()` integration

```python
# In analyst.py — add method

def use(self, connector: Connector) -> Analyst:
    """Install a full-stack connector.

    Registers all data sources, entities, relationships, and makes
    pre-built dashboards available.

    Args:
        connector: A Connector instance (e.g., GitHubConnector, StripeConnector).

    Returns:
        self, for chaining.
    """
    connector.register(self)
    self._connectors[connector.name()] = connector
    return self
```

### Layer 4: Built-in connectors for common sources

```python
# dashboardmd/connectors/api.py — REST API connector

class APIConnector(Connector):
    """Quick connector for REST APIs.

    Usage:
        api = APIConnector(
            name="users_api",
            endpoints={
                "users": "https://api.example.com/users",
                "posts": "https://api.example.com/posts",
            },
            headers={"Authorization": "Bearer token"},
        )
        analyst.use(api)
    """
```

## Example: GitHub Connector (external package)

This shows what `dashboardmd-github` would look like:

```python
# dashboardmd_github/connector.py

from dashboardmd.connector import Connector, DashboardWidget
from dashboardmd.model import Dimension, Entity, Measure, Relationship
from dashboardmd.sources.base import SourceHandler


class GitHubPRSource(SourceHandler):
    """Fetch pull requests from GitHub API."""

    def __init__(self, token: str, repo: str):
        self.token = token
        self.repo = repo

    def register(self, conn, table_name):
        rows = self._fetch_prs()
        rows = self._clean(rows)  # normalize dates, flatten nested fields
        self._register_rows(conn, table_name, rows)

    def _fetch_prs(self) -> list[dict]:
        """Paginated GitHub API fetch."""
        import urllib.request, json
        # ... paginated fetch with auth header ...
        return all_prs

    def _clean(self, rows: list[dict]) -> list[dict]:
        """Normalize raw API response into flat, typed rows."""
        cleaned = []
        for pr in rows:
            cleaned.append({
                "number": pr["number"],
                "title": pr["title"],
                "state": pr["state"],
                "author": pr["user"]["login"],
                "created_at": pr["created_at"],
                "merged_at": pr.get("merged_at"),
                "closed_at": pr.get("closed_at"),
                "additions": pr.get("additions", 0),
                "deletions": pr.get("deletions", 0),
                "review_comments": pr.get("review_comments", 0),
                "labels": ",".join(l["name"] for l in pr.get("labels", [])),
            })
        return cleaned

    def describe(self):
        return {"columns": [("number", "INTEGER"), ("title", "VARCHAR"), ...]}


class GitHubConnector(Connector):
    """Full GitHub analytics connector.

    Usage:
        gh = GitHubConnector(token="ghp_...", repo="org/repo")
        analyst.use(gh)
        dash = gh.dashboard("PR Review")
        dash.save()
    """

    def __init__(self, token: str, repo: str):
        self.token = token
        self.repo = repo

    def name(self) -> str:
        return "github"

    def sources(self) -> dict[str, SourceHandler]:
        return {
            "pull_requests": GitHubPRSource(self.token, self.repo),
            "issues": GitHubIssueSource(self.token, self.repo),
            "commits": GitHubCommitSource(self.token, self.repo),
        }

    def entities(self) -> list[Entity]:
        return [
            Entity("pull_requests", dimensions=[
                Dimension("number", type="number", primary_key=True),
                Dimension("state", type="string"),
                Dimension("author", type="string"),
                Dimension("created_at", type="time"),
                Dimension("merged_at", type="time"),
                Dimension("labels", type="string"),
            ], measures=[
                Measure("count", type="count"),
                Measure("avg_merge_time", type="avg",
                        sql="DATEDIFF('hour', created_at, merged_at)"),
                Measure("total_additions", type="sum", sql="additions"),
                Measure("total_deletions", type="sum", sql="deletions"),
            ]),
            Entity("issues", dimensions=[
                Dimension("number", type="number", primary_key=True),
                Dimension("state", type="string"),
                Dimension("author", type="string"),
                Dimension("created_at", type="time"),
                Dimension("closed_at", type="time"),
                Dimension("labels", type="string"),
            ], measures=[
                Measure("count", type="count"),
                Measure("avg_close_time", type="avg",
                        sql="DATEDIFF('hour', created_at, closed_at)"),
            ]),
            Entity("commits", dimensions=[
                Dimension("sha", type="string", primary_key=True),
                Dimension("author", type="string"),
                Dimension("date", type="time"),
                Dimension("message", type="string"),
            ], measures=[
                Measure("count", type="count"),
            ]),
        ]

    def relationships(self) -> list[Relationship]:
        return [
            Relationship("pull_requests", "commits", on=("author", "author")),
            Relationship("issues", "pull_requests", on=("author", "author")),
        ]

    def widgets(self) -> list[DashboardWidget]:
        return [
            DashboardWidget(
                name="PR Review",
                title="Pull Request Review",
                description="PR velocity, merge times, and contributor stats",
                build=self._build_pr_review,
            ),
            DashboardWidget(
                name="Issue Tracker",
                title="Issue Analytics",
                description="Issue volume, resolution time, label breakdown",
                build=self._build_issue_tracker,
            ),
            DashboardWidget(
                name="Contributor",
                title="Contributor Analytics",
                description="Commit frequency, PR activity per author",
                build=self._build_contributor,
            ),
        ]

    def _build_pr_review(self, dash):
        dash.section("PR Overview")
        dash.tile("pull_requests.count")
        dash.tile("pull_requests.avg_merge_time", format=",.1f hrs")
        dash.section("PR by Author")
        dash.tile("pull_requests.count", by="pull_requests.author", top=10, sort="desc")
        dash.section("PR by State")
        dash.tile("pull_requests.count", by="pull_requests.state")
        dash.section("Merge Time Trend")
        dash.tile("pull_requests.avg_merge_time", by="pull_requests.created_at",
                   granularity="week")

    def _build_issue_tracker(self, dash):
        dash.section("Issue Overview")
        dash.tile("issues.count")
        dash.tile("issues.avg_close_time", format=",.1f hrs")
        dash.section("Issues by State")
        dash.tile("issues.count", by="issues.state")
        dash.section("Issue Volume Over Time")
        dash.tile("issues.count", by="issues.created_at", granularity="week")

    def _build_contributor(self, dash):
        dash.section("Top Contributors")
        dash.tile("commits.count", by="commits.author", top=15, sort="desc")
        dash.section("Commit Activity")
        dash.tile("commits.count", by="commits.date", granularity="week")
```

## Key Design Decisions

### 1. Connector vs SourceHandler

| | SourceHandler | Connector |
|---|---|---|
| **Scope** | Single table | Full data domain |
| **Provides** | Data registration | Data + model + dashboards |
| **Use case** | "Give me this table" | "Give me full analytics for X" |
| **Packaging** | Part of core | Core base + external packages |

SourceHandler remains the building block. Connector uses multiple SourceHandlers internally.

### 2. Why `register()` pattern (not declarative config)?

Connectors call imperative `register()` instead of returning static config because:
- Data fetching may need authentication, pagination, rate limiting
- Cleaning logic varies wildly per API
- Some sources need multiple API calls composed together
- Config-based approaches (YAML/JSON) can't express this

### 3. Widget `build` callback

Widgets use `build: Callable[[Dashboard], None]` — a function that receives a Dashboard
and adds sections/tiles to it. This is maximally flexible:
- Simple widgets just call `dash.section()` + `dash.tile()`
- Complex widgets can use `dash.tile_sql()` for custom queries
- Widgets can use notebookmd directly for charts

### 4. Package convention

External connector packages follow: `dashboardmd-{name}` (pip) / `dashboardmd_{name}` (import).

```
dashboardmd-github/
├── pyproject.toml          # depends on dashboardmd
├── dashboardmd_github/
│   ├── __init__.py         # exports GitHubConnector
│   ├── connector.py        # GitHubConnector class
│   ├── sources.py          # GitHubPRSource, GitHubIssueSource, etc.
│   └── widgets.py          # Pre-built dashboard builders
```

### 5. Entry point discovery (future)

```toml
# In dashboardmd-github's pyproject.toml
[project.entry-points."dashboardmd.connectors"]
github = "dashboardmd_github:GitHubConnector"
```

This enables auto-discovery:
```python
# Future: dashboardmd discovers installed connectors
from dashboardmd import list_connectors
list_connectors()  # → ["github", "stripe", "jira"]
```

## Implementation Plan

### Phase 1: Core API (this PR)
| File | Change |
|------|--------|
| `dashboardmd/connector.py` | New — `Connector` base class + `DashboardWidget` |
| `dashboardmd/sources/base.py` | Add `_register_rows()` helper |
| `dashboardmd/analyst.py` | Add `use()` method + `_connectors` dict |
| `dashboardmd/__init__.py` | Export `Connector`, `DashboardWidget` |

### Phase 2: Example connector
| File | Change |
|------|--------|
| `examples/connector-plugin/` | Example showing a custom connector with entities + widgets |

### Phase 3: Built-in utility connectors
| File | Change |
|------|--------|
| `dashboardmd/connectors/api.py` | `APIConnector` — quick REST API connector |
| `dashboardmd/connectors/callable.py` | `CallableConnector` — wrap Python functions |

### Phase 4: External packages (separate repos)
- `dashboardmd-github` — GitHub analytics
- `dashboardmd-stripe` — payment analytics
- `dashboardmd-jira` — project management analytics
