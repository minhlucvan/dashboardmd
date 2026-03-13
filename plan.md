# Connector Plugin API — Full-Stack, Composable Data Connectors

## Vision

A **Connector** is a full-stack plugin that bundles everything needed to analyze a data domain.
But the real power is **composability** — connectors are designed to be mixed freely.
Install GitHub + Jira + custom connectors, they all register into the same Analyst,
and you query across them as if they were one system.

```python
# The composability story: multiple connectors → one unified analyst
from dashboardmd import Analyst, Dashboard
from dashboardmd_github import GitHubConnector
from dashboardmd_jira import JiraConnector

analyst = Analyst()

# Install multiple connectors — they all share the same DuckDB
analyst.use(GitHubConnector(token="ghp_...", repo="org/repo"))
analyst.use(JiraConnector(url="https://org.atlassian.net", project="PROJ"))

# Cross-connector relationships — link Jira tickets to GitHub PRs
analyst.add_relationship(Relationship(
    "jira_issues", "pull_requests",
    on=("key", "jira_key"),  # PR branch names contain Jira keys
    type="one_to_many",
))

# Query across connectors as if they're one system
analyst.query(
    measures=["pull_requests.avg_merge_time"],
    dimensions=["jira_issues.priority"],
)

# Build a unified dashboard mixing widgets from different connectors
dash = Dashboard(title="Engineering Velocity", analyst=analyst, output="velocity.md")
gh.contribute_widgets(dash, ["PR Review"])        # GitHub's PR widget
jira.contribute_widgets(dash, ["Sprint Board"])   # Jira's sprint widget
dash.section("Cross-Platform")                    # Custom cross-connector section
dash.tile_sql("PRs by Jira Priority", """
    SELECT j.priority, COUNT(*) as pr_count, AVG(p.merge_hours) as avg_merge
    FROM pull_requests p JOIN jira_issues j ON p.jira_key = j.key
    GROUP BY 1 ORDER BY 2 DESC
""")
dash.save()
```

## Core Principles

### 1. Connectors are additive, not exclusive

Multiple `analyst.use()` calls stack — each connector adds its tables, entities,
and relationships into the shared DuckDB. No connector "owns" the analyst.

### 2. Cross-connector joins are first-class

Users can define relationships between entities from different connectors.
The semantic query engine resolves joins across connector boundaries transparently.

### 3. Widgets are portable

A connector's widgets work on any Dashboard that has the required entities registered.
Widgets from GitHub, Jira, and custom connectors can coexist in one dashboard.

### 4. Mix core, community, and custom

```python
analyst = Analyst()

# Community connector (pip install dashboardmd-github)
analyst.use(GitHubConnector(token="...", repo="org/repo"))

# Core built-in connector
analyst.use(APIConnector("crm", endpoints={"deals": "https://api.crm.com/deals"}))

# Custom inline connector
analyst.use(CustomConnector(
    name="team",
    data={"members": load_team_csv()},
    entities=[Entity("members", dimensions=[...], measures=[...])],
))

# All three are now queryable together
analyst.sql("SELECT * FROM pull_requests p JOIN members m ON p.author = m.github_handle")
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
    """A pre-built dashboard section contributed by a connector.

    Widgets are portable — they work on any Dashboard that has the
    required entities registered, regardless of which connector
    registered them.
    """

    name: str
    title: str
    description: str = ""
    requires: list[str] = field(default_factory=list)  # entity names this widget needs
    build: Callable[[Dashboard], None] = lambda d: None


class Connector(ABC):
    """Full-stack data connector: source + model + dashboards.

    Connectors are designed to be composed freely. Multiple connectors
    can be installed into the same Analyst, and their entities can be
    joined together via cross-connector relationships.

    Subclass this to create a complete analytics package for a data domain.
    A connector provides:
    1. Data sources (tables registered in DuckDB)
    2. Entities with dimensions and measures (semantic model)
    3. Relationships between entities (within this connector)
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

        Each entity's name should match a key from sources().
        """
        ...

    def relationships(self) -> list[Relationship]:
        """Return relationships between this connector's entities.

        For cross-connector relationships, users add them directly
        via analyst.add_relationship() after installing both connectors.
        """
        return []

    def widgets(self) -> list[DashboardWidget]:
        """Return pre-built dashboard widgets.

        Widgets should declare their required entities in `requires`
        so the system can validate they're available before building.
        """
        return []

    # ------------------------------------------------------------------
    # Registration (called by Analyst.use())
    # ------------------------------------------------------------------

    def register(self, analyst: Analyst) -> None:
        """Register all sources, entities, and relationships into an Analyst.

        Called by analyst.use(connector). Multiple connectors can register
        into the same analyst — they all share the same DuckDB instance.
        Override for custom setup logic.
        """
        # 1. Register data sources
        for table_name, source in self.sources().items():
            analyst.add(table_name, source)

        # 2. Register entities (sources already loaded, so set source=None)
        for entity in self.entities():
            entity_copy = Entity(
                name=entity.name,
                source=None,  # already registered via sources()
                dimensions=entity.dimensions,
                measures=entity.measures,
            )
            analyst._entities[entity.name] = entity_copy
            analyst._query_builder = None

        # 3. Register relationships (additive — extends existing)
        rels = self.relationships()
        if rels:
            analyst._relationships.extend(rels)
            analyst._query_builder = None

    # ------------------------------------------------------------------
    # Dashboard contribution
    # ------------------------------------------------------------------

    def contribute_widgets(
        self,
        dashboard: Dashboard,
        widget_names: list[str] | None = None,
    ) -> None:
        """Add this connector's widgets to an existing dashboard.

        This is the composability API — multiple connectors contribute
        widgets to the same dashboard.

        Args:
            dashboard: Target dashboard to add widgets to.
            widget_names: Specific widgets to add, or None for all.
        """
        for widget in self.widgets():
            if widget_names is None or widget.name in widget_names:
                widget.build(dashboard)

    def dashboard(
        self,
        name: str | None = None,
        output: str = "output/dashboard.md",
        analyst: Analyst | None = None,
    ) -> Dashboard:
        """Create a standalone dashboard from this connector's widgets.

        For multi-connector dashboards, use contribute_widgets() instead.

        Args:
            name: Widget name to use, or None for all widgets.
            output: Output file path.
            analyst: Existing Analyst (with data already loaded), or creates new one.

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
            output=output,
            analyst=analyst,
        )

        self.contribute_widgets(dash, [name] if name else None)
        return dash

    def available_dashboards(self) -> list[str]:
        """List available pre-built dashboard/widget names."""
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

### Layer 3: `Analyst` integration

```python
# In analyst.py — add methods

class Analyst:
    def __init__(self, ...):
        ...
        self._connectors: dict[str, Connector] = {}

    def use(self, connector: Connector) -> Analyst:
        """Install a full-stack connector.

        Multiple connectors can be installed — they all share the same
        DuckDB instance. Use add_relationship() to define cross-connector joins.

        Args:
            connector: A Connector instance.

        Returns:
            self, for chaining.
        """
        connector.register(self)
        self._connectors[connector.name()] = connector
        return self

    def add_relationship(self, relationship: Relationship) -> Analyst:
        """Add a relationship (useful for cross-connector joins).

        Args:
            relationship: A Relationship linking entities from any connectors.

        Returns:
            self, for chaining.
        """
        self._relationships.append(relationship)
        self._query_builder = None
        return self

    @property
    def connectors(self) -> dict[str, Connector]:
        """Installed connectors by name."""
        return self._connectors
```

## Composability Examples

### Example 1: GitHub + Jira — Engineering Velocity

```python
analyst = Analyst()
analyst.use(GitHubConnector(token="...", repo="org/repo"))
analyst.use(JiraConnector(url="...", project="PROJ"))

# Link PRs to Jira tickets (PR branches contain ticket keys)
analyst.add_relationship(Relationship(
    "jira_issues", "pull_requests", on=("key", "jira_key"), type="one_to_many"
))

# Unified dashboard
dash = Dashboard(title="Engineering Velocity", analyst=analyst, output="velocity.md")
gh.contribute_widgets(dash, ["PR Review"])
jira.contribute_widgets(dash, ["Sprint Board"])
dash.section("Cross-Platform Insights")
dash.tile_sql("Cycle Time by Priority", """
    SELECT j.priority, AVG(DATEDIFF('hour', j.created, p.merged_at)) as cycle_hours
    FROM jira_issues j JOIN pull_requests p ON j.key = p.jira_key
    WHERE p.merged_at IS NOT NULL
    GROUP BY 1
""")
dash.save()
```

### Example 2: Stripe + Custom CRM — Revenue Analytics

```python
analyst = Analyst()
analyst.use(StripeConnector(api_key="sk_..."))
analyst.use(CustomConnector(
    name="crm",
    data={"customers": load_crm_data()},
    entities=[Entity("customers", dimensions=[
        Dimension("id", type="number", primary_key=True),
        Dimension("segment", type="string"),
        Dimension("region", type="string"),
    ], measures=[
        Measure("count", type="count"),
    ])],
))

# Link Stripe charges to CRM customers
analyst.add_relationship(Relationship(
    "stripe_charges", "customers", on=("customer_email", "email"), type="many_to_one"
))

# Revenue by CRM segment
analyst.query(
    measures=["stripe_charges.total_amount"],
    dimensions=["customers.segment"],
)
```

### Example 3: Community + Core + Custom

```python
analyst = Analyst()

# Community: pip install dashboardmd-github
analyst.use(GitHubConnector(token="...", repo="org/repo"))

# Core built-in: REST API
analyst.use(APIConnector("deploy", endpoints={
    "deploys": "https://internal-api.com/deploys"
}, entities=[Entity("deploys", ...)]))

# Custom: inline data
analyst.add("team", [
    {"name": "Alice", "github": "alice", "team": "backend"},
    {"name": "Bob", "github": "bob", "team": "frontend"},
])

# All three sources, one query
analyst.sql("""
    SELECT t.team, COUNT(DISTINCT p.number) as prs, COUNT(DISTINCT d.id) as deploys
    FROM team t
    JOIN pull_requests p ON t.github = p.author
    JOIN deploys d ON t.github = d.deployed_by
    GROUP BY 1
""")
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
        rows = self._clean(rows)
        self._register_rows(conn, table_name, rows)

    def _fetch_prs(self) -> list[dict]:
        """Paginated GitHub API fetch."""
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
        return {"columns": [("number", "INTEGER"), ("title", "VARCHAR")]}


class GitHubConnector(Connector):
    """Full GitHub analytics connector.

    Designed to compose with other connectors:

        analyst = Analyst()
        analyst.use(GitHubConnector(token="...", repo="org/repo"))
        analyst.use(JiraConnector(...))  # works together seamlessly

        # Cross-connector query
        analyst.query(
            measures=["pull_requests.count"],
            dimensions=["jira_issues.sprint"],
        )
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
        """Internal relationships between GitHub entities."""
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
                requires=["pull_requests"],
                build=self._build_pr_review,
            ),
            DashboardWidget(
                name="Issue Tracker",
                title="Issue Analytics",
                description="Issue volume, resolution time, label breakdown",
                requires=["issues"],
                build=self._build_issue_tracker,
            ),
            DashboardWidget(
                name="Contributor",
                title="Contributor Analytics",
                description="Commit frequency, PR activity per author",
                requires=["commits", "pull_requests"],
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

    def _build_issue_tracker(self, dash):
        dash.section("Issue Overview")
        dash.tile("issues.count")
        dash.tile("issues.avg_close_time", format=",.1f hrs")
        dash.section("Issues by State")
        dash.tile("issues.count", by="issues.state")

    def _build_contributor(self, dash):
        dash.section("Top Contributors")
        dash.tile("commits.count", by="commits.author", top=15, sort="desc")
```

## Key Design Decisions

### 1. Composability is the default

The entire API assumes multiple connectors coexist:
- `analyst.use()` is additive (never replaces)
- `_relationships` is a list that extends (not replaces)
- `contribute_widgets()` adds to existing dashboards
- Cross-connector `add_relationship()` is a first-class operation

### 2. Connector vs SourceHandler

| | SourceHandler | Connector |
|---|---|---|
| **Scope** | Single table | Full data domain |
| **Provides** | Data registration | Data + model + dashboards |
| **Composability** | N/A — building block | Designed to mix with others |
| **Packaging** | Part of core | Core base + community packages |

SourceHandler remains the building block. Connector uses multiple SourceHandlers internally.

### 3. Why `register()` pattern (not declarative config)?

Connectors call imperative `register()` instead of returning static config because:
- Data fetching may need authentication, pagination, rate limiting
- Cleaning logic varies wildly per API
- Some sources need multiple API calls composed together
- Config-based approaches (YAML/JSON) can't express this

### 4. Widgets declare dependencies

Widgets have a `requires` field listing entity names. This enables:
- Validation before building (are the required entities registered?)
- Documentation (what entities does this widget need?)
- Cross-connector widgets that require entities from multiple connectors

### 5. Package convention

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

### 6. Entry point discovery (future)

```toml
# In dashboardmd-github's pyproject.toml
[project.entry-points."dashboardmd.connectors"]
github = "dashboardmd_github:GitHubConnector"
```

This enables auto-discovery:
```python
from dashboardmd import list_connectors
list_connectors()  # → ["github", "stripe", "jira"]
```

## Implementation Plan

### Phase 1: Core API (this PR)
| File | Change |
|------|--------|
| `dashboardmd/connector.py` | New — `Connector` base class + `DashboardWidget` |
| `dashboardmd/sources/base.py` | Add `_register_rows()` helper |
| `dashboardmd/analyst.py` | Add `use()`, `add_relationship()`, `_connectors` dict |
| `dashboardmd/__init__.py` | Export `Connector`, `DashboardWidget` |

### Phase 2: Example connector
| File | Change |
|------|--------|
| `examples/connector-plugin/` | Example showing composable connectors with cross-source queries |

### Phase 3: Built-in utility connectors
| File | Change |
|------|--------|
| `dashboardmd/connectors/api.py` | `APIConnector` — quick REST API connector |
| `dashboardmd/connectors/callable.py` | `CallableConnector` — wrap Python functions |

### Phase 4: External packages (separate repos)
- `dashboardmd-github` — GitHub analytics
- `dashboardmd-stripe` — payment analytics
- `dashboardmd-jira` — project management analytics
