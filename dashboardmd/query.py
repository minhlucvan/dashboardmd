"""Query builder: resolve joins, generate SQL/pandas operations."""

from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import dataclass, field
from typing import Any

from dashboardmd.model import Entity, Measure, Relationship


@dataclass
class JoinClause:
    """A resolved JOIN clause for SQL generation."""

    from_entity: str
    to_entity: str
    from_col: str
    to_col: str
    join_type: str = "JOIN"


@dataclass
class Query:
    """A semantic query: measures + dimensions + filters."""

    measures: list[str] = field(default_factory=list)
    dimensions: list[str] | None = field(default=None)
    filters: list[tuple[str, str, str]] = field(default_factory=list)
    time_granularity: str | None = None
    sort: tuple[str, str] | None = None
    limit: int | None = None
    compare: str | None = None

    def __post_init__(self) -> None:
        if self.dimensions is None:
            self.dimensions = []


class JoinResolver:
    """Build a graph from Relationships, find shortest path between entities."""

    def __init__(self, relationships: list[Relationship]) -> None:
        self.graph: dict[str, list[tuple[str, Relationship]]] = defaultdict(list)
        for rel in relationships:
            self.graph[rel.from_entity].append((rel.to_entity, rel))
            self.graph[rel.to_entity].append((rel.from_entity, rel))

    def resolve(self, entities: set[str]) -> list[JoinClause]:
        """Given a set of entities needed, return the JOIN clauses."""
        if len(entities) <= 1:
            return []

        entities_list = list(entities)
        start = entities_list[0]
        remaining = set(entities_list[1:])
        visited: set[str] = {start}
        joins: list[JoinClause] = []

        for target in remaining:
            path = self._bfs(start, target)
            if path is None:
                raise ValueError(f"No join path found between '{start}' and '{target}'")

            for i in range(len(path) - 1):
                a, b = path[i], path[i + 1]
                if (a, b) not in {(j.from_entity, j.to_entity) for j in joins} and \
                   (b, a) not in {(j.from_entity, j.to_entity) for j in joins}:
                    rel = self._find_relationship(a, b)
                    if a == rel.from_entity:
                        joins.append(JoinClause(
                            from_entity=rel.from_entity,
                            to_entity=rel.to_entity,
                            from_col=rel.on[0],
                            to_col=rel.on[1],
                        ))
                    else:
                        joins.append(JoinClause(
                            from_entity=rel.to_entity,
                            to_entity=rel.from_entity,
                            from_col=rel.on[1],
                            to_col=rel.on[0],
                        ))
                    visited.add(a)
                    visited.add(b)

        return joins

    def _bfs(self, start: str, target: str) -> list[str] | None:
        """BFS to find shortest path between two entities."""
        queue: deque[list[str]] = deque([[start]])
        visited: set[str] = {start}

        while queue:
            path = queue.popleft()
            current = path[-1]

            if current == target:
                return path

            for neighbor, _ in self.graph.get(current, []):
                if neighbor not in visited:
                    visited.add(neighbor)
                    queue.append(path + [neighbor])

        return None

    def _find_relationship(self, a: str, b: str) -> Relationship:
        """Find the relationship between two entities."""
        for neighbor, rel in self.graph.get(a, []):
            if neighbor == b:
                return rel
        raise ValueError(f"No relationship between '{a}' and '{b}'")


class QueryBuilder:
    """Translates semantic queries into SQL strings."""

    def __init__(
        self,
        entities: list[Entity],
        relationships: list[Relationship] | None = None,
    ) -> None:
        self.entities = {e.name: e for e in entities}
        self.resolver = JoinResolver(relationships or [])

    def build_sql(self, query: Query) -> str:
        """Generate SQL from a semantic Query."""
        # Parse entity.field references
        select_parts: list[str] = []
        needed_entities: set[str] = set()
        group_by_parts: list[str] = []

        # Dimensions
        for dim_ref in query.dimensions or []:
            entity_name, dim_name = dim_ref.split(".", 1)
            needed_entities.add(entity_name)
            entity = self.entities[entity_name]
            dim = entity.get_dimension(dim_name)
            col = dim.sql if (dim and dim.sql) else dim_name
            qualified = f"{entity_name}.{col}"
            select_parts.append(f"{qualified} AS {dim_name}")
            group_by_parts.append(qualified)

        # Measures
        for measure_ref in query.measures:
            entity_name, measure_name = measure_ref.split(".", 1)
            needed_entities.add(entity_name)
            entity = self.entities[entity_name]
            measure = entity.get_measure(measure_name)
            if measure is None:
                raise ValueError(f"Measure '{measure_name}' not found on entity '{entity_name}'")
            agg_sql = self._measure_to_sql(measure, entity_name)
            select_parts.append(f"{agg_sql} AS {measure_name}")

        # FROM + JOINs
        base_entity = list(needed_entities)[0]
        from_clause = base_entity
        joins = self.resolver.resolve(needed_entities)
        join_clauses = []
        for j in joins:
            join_clauses.append(
                f"JOIN {j.to_entity} ON {j.from_entity}.{j.from_col} = {j.to_entity}.{j.to_col}"
            )

        # WHERE
        where_parts: list[str] = []
        for filter_ref, op, value in query.filters:
            entity_name, col_name = filter_ref.split(".", 1)
            if op == "equals":
                where_parts.append(f"{entity_name}.{col_name} = '{value}'")
            elif op == "not_equals":
                where_parts.append(f"{entity_name}.{col_name} != '{value}'")
            elif op in ("gt", "gte", "lt", "lte"):
                sql_op = {"gt": ">", "gte": ">=", "lt": "<", "lte": "<="}[op]
                where_parts.append(f"{entity_name}.{col_name} {sql_op} '{value}'")

        # Build SQL
        sql = f"SELECT {', '.join(select_parts)} FROM {from_clause}"
        if join_clauses:
            sql += " " + " ".join(join_clauses)
        if where_parts:
            sql += " WHERE " + " AND ".join(where_parts)
        if group_by_parts:
            sql += " GROUP BY " + ", ".join(group_by_parts)

        # ORDER BY
        if query.sort:
            sort_ref, sort_dir = query.sort
            if "." in sort_ref:
                _, sort_col = sort_ref.split(".", 1)
            else:
                sort_col = sort_ref
            sql += f" ORDER BY {sort_col} {sort_dir.upper()}"

        # LIMIT
        if query.limit:
            sql += f" LIMIT {query.limit}"

        return sql

    def _measure_to_sql(self, measure: Measure, entity_name: str) -> str:
        """Convert a Measure to its SQL aggregation expression."""
        col = measure.sql or "*"
        qualified_col = f"{entity_name}.{col}" if col != "*" else "*"

        match measure.type:
            case "sum":
                return f"SUM({qualified_col})"
            case "count":
                return "COUNT(*)"
            case "count_distinct":
                return f"COUNT(DISTINCT {qualified_col})"
            case "avg":
                return f"AVG({qualified_col})"
            case "min":
                return f"MIN({qualified_col})"
            case "max":
                return f"MAX({qualified_col})"
            case "number":
                return col
            case _:
                raise ValueError(f"Unknown measure type: {measure.type}")
