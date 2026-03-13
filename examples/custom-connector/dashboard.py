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

import plotly.express as px
import plotly.graph_objects as go
from connectors import GeneratorSource
from notebookmd import nb

from dashboardmd import Analyst, Entity

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

    return tasks


# ---------------------------------------------------------------------------
# 2. Prepare team data (simulating API response as local JSON)
# ---------------------------------------------------------------------------

team_data = [
    {"id": 1, "name": "Alice", "role": "Senior Engineer", "team": "Frontend", "hourly_rate": 95},
    {"id": 2, "name": "Bob", "role": "Tech Lead", "team": "Backend", "hourly_rate": 115},
    {"id": 3, "name": "Carol", "role": "Designer", "team": "Frontend", "hourly_rate": 85},
    {"id": 4, "name": "Dan", "role": "Engineer", "team": "Mobile", "hourly_rate": 90},
    {"id": 5, "name": "Eva", "role": "Data Engineer", "team": "Data", "hourly_rate": 100},
    {"id": 6, "name": "Frank", "role": "Engineer", "team": "Backend", "hourly_rate": 90},
]

os.makedirs("data", exist_ok=True)
with open("data/team.json", "w") as f:
    json.dump(team_data, f)

# ---------------------------------------------------------------------------
# Register entities with custom connectors
# ---------------------------------------------------------------------------

projects_entity = Entity("projects", source=GeneratorSource(factory=generate_projects))
tasks_entity = Entity("tasks", source=GeneratorSource(factory=generate_tasks))

analyst = Analyst()
analyst.add_entity(projects_entity)
analyst.add_entity(tasks_entity)
analyst.add("team", "data/team.json")

# ---------------------------------------------------------------------------
# Build report with notebookmd + plotly
# ---------------------------------------------------------------------------

n = nb("dashboard.md", title="Project Tracker Dashboard")

# --- Portfolio Overview ---
with n.section("Portfolio Overview"):
    kpis = analyst.sql("""
        SELECT COUNT(*) AS projects, SUM(budget) AS budget FROM projects
    """).fetchone()
    task_kpis = analyst.sql("""
        SELECT COUNT(*) AS tasks, ROUND(SUM(hours_logged), 1) AS hours FROM tasks
    """).fetchone()

    n.metric_row([
        {"label": "Projects", "value": kpis[0]},
        {"label": "Total Budget", "value": f"${kpis[1]:,.0f}"},
        {"label": "Tasks", "value": task_kpis[0]},
        {"label": "Hours Logged", "value": task_kpis[1]},
    ])

# --- Budget by Team ---
with n.section("Budget by Team"):
    rows = analyst.sql("""
        SELECT team, SUM(budget) AS budget FROM projects GROUP BY 1 ORDER BY budget DESC
    """).fetchall()

    fig = px.bar(
        x=[r[0] for r in rows], y=[r[1] for r in rows],
        labels={"x": "Team", "y": "Budget ($)"},
        color_discrete_sequence=["#636EFA"],
    )
    fig.update_layout(showlegend=False)
    n.plotly_chart(fig, filename="budget_by_team.html", caption="Budget allocation by team")

# --- Tasks by Status ---
with n.section("Tasks by Status"):
    rows = analyst.sql("""
        SELECT status, COUNT(*) AS count FROM tasks GROUP BY 1 ORDER BY count DESC
    """).fetchall()

    fig = px.pie(
        names=[r[0] for r in rows], values=[r[1] for r in rows],
        color_discrete_map={"done": "#2ecc71", "in_progress": "#f39c12", "todo": "#95a5a6"},
    )
    n.plotly_chart(fig, filename="tasks_by_status.html", caption="Task status distribution")

# --- Tasks by Priority ---
with n.section("Tasks by Priority"):
    rows = analyst.sql("""
        SELECT priority, COUNT(*) AS count FROM tasks GROUP BY 1 ORDER BY count DESC
    """).fetchall()

    colors = {"critical": "#e74c3c", "high": "#e67e22", "medium": "#f1c40f", "low": "#2ecc71"}
    fig = px.bar(
        x=[r[0] for r in rows], y=[r[1] for r in rows],
        labels={"x": "Priority", "y": "Count"},
        color=[r[0] for r in rows], color_discrete_map=colors,
    )
    fig.update_layout(showlegend=False)
    n.plotly_chart(fig, filename="tasks_by_priority.html", caption="Task count by priority level")

# --- Workload by Assignee ---
with n.section("Workload by Assignee"):
    rows = analyst.sql("""
        SELECT t.assignee, COUNT(*) AS tasks,
               ROUND(SUM(t.hours_logged), 1) AS hours_logged,
               ROUND(SUM(t.estimated_hours), 1) AS estimated_hours,
               ROUND(SUM(t.hours_logged) / NULLIF(SUM(t.estimated_hours), 0) * 100, 1) AS efficiency_pct
        FROM tasks t WHERE t.status != 'todo'
        GROUP BY 1 ORDER BY hours_logged DESC
    """).fetchall()

    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=[r[0] for r in rows], y=[r[2] for r in rows],
        name="Hours Logged", marker_color="#636EFA",
    ))
    fig.add_trace(go.Bar(
        x=[r[0] for r in rows], y=[r[3] for r in rows],
        name="Estimated Hours", marker_color="#C8C8C8",
    ))
    fig.update_layout(
        barmode="group", yaxis_title="Hours",
        legend={"orientation": "h", "yanchor": "bottom", "y": 1.02},
    )
    n.plotly_chart(fig, filename="workload_by_assignee.html", caption="Actual vs estimated hours by assignee")

# --- Project Health ---
with n.section("Project Health"):
    rows = analyst.sql("""
        SELECT p.name AS project, p.budget, COUNT(t.id) AS total_tasks,
               COUNT(t.id) FILTER (WHERE t.status = 'done') AS completed,
               ROUND(COUNT(t.id) FILTER (WHERE t.status = 'done') * 100.0 / COUNT(t.id), 1) AS completion_pct,
               ROUND(SUM(t.hours_logged), 1) AS hours_spent
        FROM projects p JOIN tasks t ON p.id = t.project_id
        GROUP BY 1, 2 ORDER BY completion_pct DESC
    """).fetchall()

    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=[r[0] for r in rows], y=[r[4] for r in rows],
        text=[f"{r[4]}%" for r in rows], textposition="outside",
        marker_color=["#2ecc71" if r[4] >= 60 else "#f39c12" for r in rows],
    ))
    fig.update_layout(yaxis_title="Completion %", yaxis_range=[0, 100], showlegend=False)
    n.plotly_chart(fig, filename="project_health.html", caption="Project completion rates")

    n.table(
        [{"Project": r[0], "Budget": f"${r[1]:,}", "Tasks": r[2],
          "Done": r[3], "Completion": f"{r[4]}%", "Hours": r[5]} for r in rows],
        name="Project Details",
    )

# --- Team Cost Analysis ---
with n.section("Team Cost Analysis"):
    rows = analyst.sql("""
        SELECT tm.team, COUNT(DISTINCT tm.id) AS members,
               ROUND(SUM(t.hours_logged), 1) AS total_hours,
               ROUND(SUM(t.hours_logged * tm.hourly_rate), 2) AS total_cost,
               ROUND(AVG(tm.hourly_rate), 2) AS avg_rate
        FROM tasks t JOIN team tm ON t.assignee = tm.name
        GROUP BY 1 ORDER BY total_cost DESC
    """).fetchall()

    fig = px.bar(
        x=[r[0] for r in rows], y=[r[3] for r in rows],
        text=[f"${r[3]:,.0f}" for r in rows],
        labels={"x": "Team", "y": "Total Cost ($)"},
        color_discrete_sequence=["#EF553B"],
    )
    fig.update_traces(textposition="outside")
    fig.update_layout(showlegend=False)
    n.plotly_chart(fig, filename="team_cost.html", caption="Total labor cost by team")

n.save()
print("Dashboard saved to dashboard.md")
