"""Custom Connector Dashboard.

Demonstrates building custom SourceHandler subclasses to plug any
data source into dashboardmd. Uses two custom connectors:

    1. APISource      — fetches JSON from a REST API
    2. GeneratorSource — creates data from a Python callable

Since we can't rely on external APIs in an example, we simulate the
API connector with a local JSON file and use GeneratorSource to create
synthetic project data.
"""

import json
import os
import random
from datetime import date, timedelta

from connectors import APISource, GeneratorSource

from dashboardmd import Analyst, Dashboard, Dimension, Entity, Measure, Relationship

# ---------------------------------------------------------------------------
# 1. GeneratorSource — synthetic project & task data
# ---------------------------------------------------------------------------


def generate_projects() -> list[dict]:
    """Generate sample project data."""
    return [
        {"id": 1, "name": "Website Redesign", "team": "Frontend", "status": "active", "budget": 50000},
        {"id": 2, "name": "API Platform", "team": "Backend", "status": "active", "budget": 80000},
        {"id": 3, "name": "Mobile App v2", "team": "Mobile", "status": "active", "budget": 120000},
        {"id": 4, "name": "Data Pipeline", "team": "Data", "status": "completed", "budget": 45000},
        {"id": 5, "name": "Auth Service", "team": "Backend", "status": "active", "budget": 30000},
    ]


def generate_tasks() -> list[dict]:
    """Generate sample task data with time tracking."""
    random.seed(42)
    tasks = []
    task_id = 1
    statuses = ["todo", "in_progress", "done", "done", "done"]  # bias toward done
    priorities = ["low", "medium", "medium", "high", "high", "critical"]
    assignees = ["Alice", "Bob", "Carol", "Dan", "Eva", "Frank"]
    base_date = date(2025, 1, 6)

    for project_id in range(1, 6):
        n_tasks = random.randint(8, 15)
        for _ in range(n_tasks):
            created = base_date + timedelta(days=random.randint(0, 60))
            hours = round(random.uniform(1, 40), 1)
            status = random.choice(statuses)
            tasks.append({
                "id": task_id,
                "project_id": project_id,
                "assignee": random.choice(assignees),
                "priority": random.choice(priorities),
                "status": status,
                "created_date": created.isoformat(),
                "hours_logged": hours if status != "todo" else 0,
                "estimated_hours": round(hours * random.uniform(0.8, 1.5), 1),
            })
            task_id += 1
        pass

    return tasks


# ---------------------------------------------------------------------------
# 2. APISource — load team members from a local JSON file
#    (In production you'd point this at a real API endpoint)
# ---------------------------------------------------------------------------

# Create sample API response file
team_data = [
    {"id": 1, "name": "Alice", "role": "Senior Engineer", "team": "Frontend", "hourly_rate": 95},
    {"id": 2, "name": "Bob", "role": "Tech Lead", "team": "Backend", "hourly_rate": 115},
    {"id": 3, "name": "Carol", "role": "Designer", "team": "Frontend", "hourly_rate": 85},
    {"id": 4, "name": "Dan", "role": "Engineer", "team": "Mobile", "hourly_rate": 90},
    {"id": 5, "name": "Eva", "role": "Data Engineer", "team": "Data", "hourly_rate": 100},
    {"id": 6, "name": "Frank", "role": "Engineer", "team": "Backend", "hourly_rate": 90},
]

# Write the JSON so APISource can fetch it via file:// URL
os.makedirs("data", exist_ok=True)
json_path = os.path.join("data", "team.json")
with open(json_path, "w") as f:
    json.dump(team_data, f)

# ---------------------------------------------------------------------------
# Define entities using custom connectors
# ---------------------------------------------------------------------------

projects = Entity(
    "projects",
    source=GeneratorSource(factory=generate_projects),
    dimensions=[
        Dimension("id", type="number", primary_key=True),
        Dimension("name", type="string"),
        Dimension("team", type="string"),
        Dimension("status", type="string"),
    ],
    measures=[
        Measure("project_count", type="count"),
        Measure("total_budget", type="sum", sql="budget"),
    ],
)

tasks = Entity(
    "tasks",
    source=GeneratorSource(factory=generate_tasks),
    dimensions=[
        Dimension("id", type="number", primary_key=True),
        Dimension("project_id", type="number"),
        Dimension("assignee", type="string"),
        Dimension("priority", type="string"),
        Dimension("status", type="string"),
        Dimension("created_date", type="time"),
    ],
    measures=[
        Measure("task_count", type="count"),
        Measure("total_hours", type="sum", sql="hours_logged"),
        Measure("avg_hours", type="avg", sql="hours_logged"),
        Measure("total_estimated", type="sum", sql="estimated_hours"),
    ],
)

# For the team entity, we load from the local JSON file directly
# In production, you'd use: APISource(url="https://api.company.com/team")
team = Entity(
    "team",
    source="data/team.json",
    dimensions=[
        Dimension("id", type="number", primary_key=True),
        Dimension("name", type="string"),
        Dimension("role", type="string"),
        Dimension("team", type="string"),
    ],
    measures=[
        Measure("member_count", type="count"),
    ],
)

relationships = [
    Relationship("tasks", "projects", on=("project_id", "id"), type="many_to_one"),
]

# ---------------------------------------------------------------------------
# Build dashboard
# ---------------------------------------------------------------------------

dash = Dashboard(
    title="Project Tracker Dashboard",
    entities=[projects, tasks, team],
    relationships=relationships,
    output="dashboard.md",
)

# Portfolio overview
dash.section("Portfolio Overview")
dash.tile("projects.project_count")
dash.tile("projects.total_budget")
dash.tile("tasks.task_count")
dash.tile("tasks.total_hours")

# Project breakdown
dash.section("Budget by Team")
dash.tile("projects.total_budget", by="projects.team", type="bar_chart")

# Task distribution
dash.section("Tasks by Status")
dash.tile("tasks.task_count", by="tasks.status")

dash.section("Tasks by Priority")
dash.tile("tasks.task_count", by="tasks.priority")

# Workload by assignee
dash.section("Workload by Assignee")
dash.tile_sql(
    "Hours by Person",
    """
    SELECT
        t.assignee,
        COUNT(*) AS tasks,
        ROUND(SUM(t.hours_logged), 1) AS hours_logged,
        ROUND(SUM(t.estimated_hours), 1) AS estimated_hours,
        ROUND(SUM(t.hours_logged) / NULLIF(SUM(t.estimated_hours), 0) * 100, 1)
            AS efficiency_pct
    FROM tasks t
    WHERE t.status != 'todo'
    GROUP BY 1
    ORDER BY hours_logged DESC
    """,
)

# Project health
dash.section("Project Health")
dash.tile_sql(
    "Completion Rates",
    """
    SELECT
        p.name AS project,
        p.budget,
        COUNT(t.id) AS total_tasks,
        COUNT(t.id) FILTER (WHERE t.status = 'done') AS completed,
        ROUND(COUNT(t.id) FILTER (WHERE t.status = 'done') * 100.0 / COUNT(t.id), 1)
            AS completion_pct,
        ROUND(SUM(t.hours_logged), 1) AS hours_spent
    FROM projects p
    JOIN tasks t ON p.id = t.project_id
    GROUP BY 1, 2
    ORDER BY completion_pct DESC
    """,
)

# Team cost analysis (joining generated tasks with JSON-loaded team rates)
dash.section("Team Cost Analysis")
dash.tile_sql(
    "Cost by Team",
    """
    SELECT
        tm.team,
        COUNT(DISTINCT tm.id) AS members,
        ROUND(SUM(t.hours_logged), 1) AS total_hours,
        ROUND(SUM(t.hours_logged * tm.hourly_rate), 2) AS total_cost,
        ROUND(AVG(tm.hourly_rate), 2) AS avg_rate
    FROM tasks t
    JOIN team tm ON t.assignee = tm.name
    GROUP BY 1
    ORDER BY total_cost DESC
    """,
)

# Generate
dash.save()
print(f"Dashboard saved to {dash.output}")
