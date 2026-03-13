"""Connector: full-stack, composable data connector plugin.

A Connector bundles everything needed to analyze a data domain:
data fetching, cleaning, semantic model (entities/dimensions/measures),
relationships, and pre-built dashboard widgets.

Connectors are designed to be composed freely — install multiple connectors
into the same Analyst and query across them as one system.

    analyst = Analyst()
    analyst.use(GitHubConnector(token="...", repo="org/repo"))
    analyst.use(JiraConnector(url="...", project="PROJ"))

    # Cross-connector relationship
    analyst.add_relationship(Relationship(
        "jira_issues", "pull_requests", on=("key", "jira_key")
    ))

    # Query across connectors
    analyst.query(
        measures=["pull_requests.count"],
        dimensions=["jira_issues.priority"],
    )
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Callable

if TYPE_CHECKING:
    from dashboardmd.analyst import Analyst
    from dashboardmd.dashboard import Dashboard

from dashboardmd.model import Entity, Relationship
from dashboardmd.sources.base import SourceHandler


@dataclass
class DashboardWidget:
    """A pre-built dashboard section contributed by a connector.

    Widgets are portable — they work on any Dashboard that has the
    required entities registered, regardless of which connector
    registered them.

    Attributes:
        name: Widget identifier, e.g. "PR Review".
        title: Display title for the dashboard section.
        description: Human-readable description of what this widget shows.
        requires: Entity names this widget needs to function.
        build: Callback that adds sections/tiles to a Dashboard.
    """

    name: str
    title: str
    description: str = ""
    requires: list[str] = field(default_factory=list)
    build: Callable[[Dashboard], None] = field(default=lambda d: None)


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
            Mapping of table name to SourceHandler, e.g.
            {"pull_requests": PRSource(...), "issues": IssueSource(...)}
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

        Widgets should declare their required entities in ``requires``
        so the system can validate they're available before building.
        """
        return []

    # ------------------------------------------------------------------
    # Registration (called by Analyst.use())
    # ------------------------------------------------------------------

    def register(self, analyst: Analyst) -> None:
        """Register all sources, entities, and relationships into an Analyst.

        Called by ``analyst.use(connector)``. Multiple connectors can register
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
        from dashboardmd.dashboard import Dashboard as DashboardCls

        if analyst is None:
            from dashboardmd.analyst import Analyst as AnalystCls

            analyst = AnalystCls()
            self.register(analyst)

        dash = DashboardCls(
            title=name or f"{self.name().title()} Analytics",
            output=output,
            analyst=analyst,
        )

        self.contribute_widgets(dash, [name] if name else None)
        return dash

    def available_dashboards(self) -> list[str]:
        """List available pre-built dashboard/widget names."""
        return [w.name for w in self.widgets()]
