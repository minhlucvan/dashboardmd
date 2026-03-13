"""Example connectors demonstrating the composable Connector plugin API.

Two connectors that work independently or together:
- ProjectConnector: task tracking data (tasks + time logs)
- TeamConnector: team directory data

When composed, they enable cross-connector queries like
"hours by department" or "task completion by role".
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from dashboardmd.connector import Connector, DashboardWidget
from dashboardmd.model import Dimension, Entity, Measure, Relationship
from dashboardmd.sources.base import SourceHandler


# ---------------------------------------------------------------------------
# Source handlers (data fetching + cleaning)
# ---------------------------------------------------------------------------


@dataclass
class JSONFileSource(SourceHandler):
    """Load data from a JSON file."""

    path: str

    def register(self, conn: Any, table_name: str) -> None:
        conn.execute(
            f"CREATE OR REPLACE VIEW \"{table_name}\" AS "
            f"SELECT * FROM read_json_auto('{self.path}')"
        )

    def describe(self) -> dict[str, Any]:
        with open(self.path) as f:
            data = json.load(f)
        if data:
            return {"columns": [(k, "VARCHAR") for k in data[0].keys()]}
        return {"columns": []}


@dataclass
class CSVFileSource(SourceHandler):
    """Load data from a CSV file."""

    path: str

    def register(self, conn: Any, table_name: str) -> None:
        conn.execute(
            f"CREATE OR REPLACE VIEW \"{table_name}\" AS "
            f"SELECT * FROM read_csv_auto('{self.path}')"
        )

    def describe(self) -> dict[str, Any]:
        return {"columns": []}


# ---------------------------------------------------------------------------
# ProjectConnector — task tracking with pre-built dashboards
# ---------------------------------------------------------------------------


class ProjectConnector(Connector):
    """Project management connector with tasks and time logs.

    Provides:
    - tasks: task tracking with status, priority, estimates
    - time_logs: detailed time entries per task

    Pre-built dashboards:
    - "Project Status": task counts, completion rates, burndown
    - "Time Tracking": hours logged, estimate accuracy
    """

    def __init__(self, tasks_path: str, time_logs_path: str) -> None:
        self.tasks_path = tasks_path
        self.time_logs_path = time_logs_path

    def name(self) -> str:
        return "project"

    def sources(self) -> dict[str, SourceHandler]:
        return {
            "tasks": JSONFileSource(path=self.tasks_path),
            "time_logs": CSVFileSource(path=self.time_logs_path),
        }

    def entities(self) -> list[Entity]:
        return [
            Entity("tasks", dimensions=[
                Dimension("id", type="number", primary_key=True),
                Dimension("title", type="string"),
                Dimension("status", type="string"),
                Dimension("priority", type="string"),
                Dimension("assignee", type="string"),
                Dimension("project", type="string"),
                Dimension("created", type="time"),
                Dimension("completed", type="time"),
            ], measures=[
                Measure("count", type="count"),
                Measure("total_estimate", type="sum", sql="estimate_hours"),
                Measure("total_actual", type="sum", sql="actual_hours"),
            ]),
            Entity("time_logs", dimensions=[
                Dimension("id", type="number", primary_key=True),
                Dimension("task_id", type="number"),
                Dimension("username", type="string"),
                Dimension("date", type="time"),
                Dimension("note", type="string"),
            ], measures=[
                Measure("total_hours", type="sum", sql="hours"),
                Measure("log_count", type="count"),
                Measure("avg_hours", type="avg", sql="hours", format=".1f"),
            ]),
        ]

    def relationships(self) -> list[Relationship]:
        return [
            Relationship("time_logs", "tasks", on=("task_id", "id"), type="many_to_one"),
        ]

    def widgets(self) -> list[DashboardWidget]:
        return [
            DashboardWidget(
                name="Project Status",
                title="Project Status Overview",
                description="Task counts, completion rates, priority breakdown",
                requires=["tasks"],
                build=self._build_status,
            ),
            DashboardWidget(
                name="Time Tracking",
                title="Time Tracking Report",
                description="Hours logged, estimate vs actual, daily breakdown",
                requires=["tasks", "time_logs"],
                build=self._build_time,
            ),
        ]

    def _build_status(self, dash: Any) -> None:
        dash.section("Task Overview")
        dash.tile("tasks.count")
        dash.section("Tasks by Status")
        dash.tile("tasks.count", by="tasks.status")
        dash.section("Tasks by Priority")
        dash.tile("tasks.count", by="tasks.priority", sort="desc")
        dash.section("Tasks by Project")
        dash.tile("tasks.count", by="tasks.project")

    def _build_time(self, dash: Any) -> None:
        dash.section("Time Summary")
        dash.tile("time_logs.total_hours")
        dash.tile("time_logs.log_count")
        dash.section("Hours by Team Member")
        dash.tile("time_logs.total_hours", by="time_logs.username", sort="desc")
        dash.section("Estimate vs Actual")
        dash.tile(["tasks.total_estimate", "tasks.total_actual"], by="tasks.project")


# ---------------------------------------------------------------------------
# TeamConnector — team directory with pre-built dashboards
# ---------------------------------------------------------------------------


class TeamConnector(Connector):
    """Team directory connector.

    Provides:
    - team_members: employee directory with roles and departments

    Pre-built dashboards:
    - "Team Overview": headcount by department, role, location
    """

    def __init__(self, csv_path: str) -> None:
        self.csv_path = csv_path

    def name(self) -> str:
        return "team"

    def sources(self) -> dict[str, SourceHandler]:
        return {
            "team_members": CSVFileSource(path=self.csv_path),
        }

    def entities(self) -> list[Entity]:
        return [
            Entity("team_members", dimensions=[
                Dimension("username", type="string", primary_key=True),
                Dimension("full_name", type="string"),
                Dimension("role", type="string"),
                Dimension("department", type="string"),
                Dimension("location", type="string"),
                Dimension("start_date", type="time"),
            ], measures=[
                Measure("headcount", type="count"),
            ]),
        ]

    def widgets(self) -> list[DashboardWidget]:
        return [
            DashboardWidget(
                name="Team Overview",
                title="Team Directory",
                description="Headcount by department, role, location",
                requires=["team_members"],
                build=self._build_overview,
            ),
        ]

    def _build_overview(self, dash: Any) -> None:
        dash.section("Team Size")
        dash.tile("team_members.headcount")
        dash.section("By Department")
        dash.tile("team_members.headcount", by="team_members.department")
        dash.section("By Location")
        dash.tile("team_members.headcount", by="team_members.location")
