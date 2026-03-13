"""Dashboard: tiles, filters, sections, save().

Dashboard is a structured report builder on top of Analyst. It provides
BI-style abstractions (sections, tiles, filters) that generate SQL
queries executed by the underlying Analyst.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from dashboardmd.analyst import Analyst, QueryResult, _format_cell


# ---------------------------------------------------------------------------
# Supporting dataclasses
# ---------------------------------------------------------------------------


@dataclass
class Tile:
    """A single visualization bound to a query."""

    measures: list[str]
    by: str | None = None
    type: str | None = None
    viz_type: str | None = None
    compare: str | None = None
    granularity: str | None = None
    top: int | None = None
    sort: str | None = None
    format: str | None = None
    sql: str | None = None  # Raw SQL override — bypasses semantic layer


@dataclass
class Section:
    """A named group of tiles."""

    title: str
    tiles: list[Tile] = field(default_factory=list)


@dataclass
class Filter:
    """A global filter applied across all tiles."""

    name: str
    dimension: str
    default: str | None = None


# ---------------------------------------------------------------------------
# Smart viz selection
# ---------------------------------------------------------------------------


def infer_viz_type(
    measures: list[str],
    dimensions: list[str],
    has_time: bool = False,
    compare: str | None = None,
) -> str:
    """Auto-pick visualization type based on query shape."""
    n_measures = len(measures)
    n_dims = len(dimensions)

    if n_dims == 0:
        return "metric"
    if has_time and n_dims == 1:
        return "line_chart"
    if n_dims == 1 and n_measures <= 2:
        return "bar_chart"
    return "table"


# ---------------------------------------------------------------------------
# Dashboard
# ---------------------------------------------------------------------------


class Dashboard:
    """Structured report builder on top of Analyst.

    Provides BI-style sections, tiles, and filters. Under the hood,
    everything is executed by an Analyst instance (which uses DuckDB).

    Usage:
        dash = Dashboard(title="Weekly Review", entities=[orders, customers], ...)
        dash.section("Key Metrics")
        dash.tile("orders.revenue", compare="previous_period")
        dash.save()   # → Markdown file
    """

    def __init__(
        self,
        title: str = "Dashboard",
        entities: list[Any] | None = None,
        relationships: list[Any] | None = None,
        output: str = "output/dashboard.md",
        analyst: Analyst | None = None,
    ) -> None:
        self.title = title
        self.output = output
        self.sections: list[Section] = []
        self.filters: list[Filter] = []

        # Use provided analyst or create one
        if analyst is not None:
            self._analyst = analyst
        else:
            self._analyst = Analyst()

        # Register entities and relationships
        self._entities = entities or []
        self._relationships = relationships or []

        if self._relationships:
            self._analyst.set_relationships(self._relationships)
        for entity in self._entities:
            self._analyst.add_entity(entity)

    @property
    def analyst(self) -> Analyst:
        """The underlying Analyst instance."""
        return self._analyst

    @property
    def entities(self) -> list[Any]:
        """Registered entities."""
        return self._entities

    @property
    def relationships(self) -> list[Any]:
        """Registered relationships."""
        return self._relationships

    # ------------------------------------------------------------------
    # Building
    # ------------------------------------------------------------------

    def section(self, title: str) -> Section:
        """Add a named section."""
        s = Section(title=title)
        self.sections.append(s)
        return s

    def tile(
        self,
        measures: str | list[str],
        by: str | None = None,
        type: str | None = None,
        compare: str | None = None,
        granularity: str | None = None,
        top: int | None = None,
        sort: str | None = None,
        format: str | None = None,
        sql: str | None = None,
    ) -> Tile:
        """Add a tile to the current (last) section.

        Args:
            measures: "orders.revenue" or ["orders.revenue", "orders.count"]
            by: Dimension to group by, e.g. "customers.segment"
            type: Explicit viz type ("metric", "bar_chart", "line_chart", "table")
            compare: "previous_period" or "previous_year"
            granularity: "day", "week", "month" for time dimensions
            top: Limit to top N results
            sort: Sort order, e.g. "desc"
            format: Display format string
            sql: Raw SQL override — use DuckDB SQL instead of semantic query
        """
        if isinstance(measures, str):
            measures = [measures]

        t = Tile(
            measures=measures,
            by=by,
            type=type,
            viz_type=type,
            compare=compare,
            granularity=granularity,
            top=top,
            sort=sort,
            format=format,
            sql=sql,
        )

        # Add to last section, or create a default one
        if not self.sections:
            self.section("Overview")
        self.sections[-1].tiles.append(t)
        return t

    def tile_sql(self, title: str, sql: str) -> Tile:
        """Add a tile powered by raw SQL.

        This gives agents maximum flexibility — write any DuckDB SQL
        and it gets rendered as a table in the dashboard.

        Args:
            title: Label for this tile (used as column header context).
            sql: Any valid DuckDB SQL query.
        """
        return self.tile(measures=[], sql=sql)

    def filter(
        self,
        name: str,
        dimension: str,
        default: str | None = None,
    ) -> Filter:
        """Add a global filter."""
        f = Filter(name=name, dimension=dimension, default=default)
        self.filters.append(f)
        return f

    # ------------------------------------------------------------------
    # Query execution
    # ------------------------------------------------------------------

    def query(
        self,
        measures: list[str] | str | None = None,
        dimensions: list[str] | str | None = None,
        filters: list[tuple[str, str, str]] | None = None,
    ) -> QueryResult:
        """Execute a semantic query via the underlying Analyst."""
        return self._analyst.query(
            measures=measures,
            dimensions=dimensions,
            filters=filters,
        )

    def execute_sql(self, sql: str) -> QueryResult:
        """Execute raw SQL via the underlying Analyst."""
        return self._analyst.sql(sql)

    # ------------------------------------------------------------------
    # Render + Save
    # ------------------------------------------------------------------

    def save(self) -> str:
        """Render the dashboard to Markdown and save to output path.

        Returns:
            The generated Markdown string.
        """
        lines: list[str] = [f"# {self.title}", ""]

        for section in self.sections:
            lines.append(f"## {section.title}")
            lines.append("")

            for tile_def in section.tiles:
                try:
                    result = self._execute_tile(tile_def)
                    lines.append(result.to_markdown_table())
                except Exception as e:
                    lines.append(f"> Error: {e}")
                lines.append("")

        md_content = "\n".join(lines)

        # Write to file
        path = Path(self.output)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(md_content)

        return md_content

    def _execute_tile(self, tile: Tile) -> QueryResult:
        """Execute a tile's query via Analyst."""
        # Raw SQL tiles bypass the semantic layer entirely
        if tile.sql:
            return self._analyst.sql(tile.sql)

        # Semantic tile — build query from measures/dimensions
        dimensions = [tile.by] if tile.by else None
        limit = tile.top

        return self._analyst.query(
            measures=tile.measures,
            dimensions=dimensions,
            limit=limit,
        )

    # ------------------------------------------------------------------
    # Auto-dashboard
    # ------------------------------------------------------------------

    def auto_dashboard(self) -> Dashboard:
        """Auto-generate sections and tiles from registered entities.

        Creates a section per entity with count + aggregation tiles.
        """
        for entity in self._analyst.entities.values():
            self.section(f"{entity.name.replace('_', ' ').title()}")
            for measure in entity.measures:
                self.tile(f"{entity.name}.{measure.name}")
        return self
