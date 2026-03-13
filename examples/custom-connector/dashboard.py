"""Custom Connector Dashboard.

Demonstrates building custom SourceHandler subclasses to plug any
data source into dashboardmd. Uses two custom connectors:

    1. APISource      — fetches JSON from a REST API
    2. GeneratorSource — creates data from a Python callable

Uses matplotlib for PNG chart output via notebookmd.
"""

import json
import os
import random
from datetime import date, timedelta

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
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
    statuses = ["todo", "in_progress", "done", "done", "done"]
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
# Build report with notebookmd + matplotlib
# ---------------------------------------------------------------------------

n = nb("dashboard.md", title="Project Tracker Dashboard")

# --- Portfolio Overview ---
with n.section("Portfolio Overview"):
    kpis = analyst.sql("SELECT COUNT(*) AS projects, SUM(budget) AS budget FROM projects").fetchone()
    task_kpis = analyst.sql("SELECT COUNT(*) AS tasks, ROUND(SUM(hours_logged), 1) AS hours FROM tasks").fetchone()

    n.metric_row([
        {"label": "Projects", "value": kpis[0]},
        {"label": "Total Budget", "value": f"${kpis[1]:,.0f}"},
        {"label": "Tasks", "value": task_kpis[0]},
        {"label": "Hours Logged", "value": task_kpis[1]},
    ])

# --- Budget by Team ---
with n.section("Budget by Team"):
    df = analyst.sql("SELECT team, SUM(budget) AS budget FROM projects GROUP BY 1 ORDER BY budget DESC").df()
    n.bar_chart(df, x="team", y="budget", title="Budget by Team")

# --- Tasks by Status ---
with n.section("Tasks by Status"):
    rows = analyst.sql("SELECT status, COUNT(*) AS count FROM tasks GROUP BY 1 ORDER BY count DESC").fetchall()

    colors = {"done": "#2ecc71", "in_progress": "#f39c12", "todo": "#95a5a6"}
    fig, ax = plt.subplots(figsize=(5, 4))
    ax.pie([r[1] for r in rows], labels=[r[0] for r in rows], autopct="%1.0f%%",
           colors=[colors.get(r[0], "#ccc") for r in rows])
    ax.set_title("Task Status Distribution")
    n.figure(fig, "tasks_by_status.png", caption="Task status distribution")
    plt.close(fig)

# --- Tasks by Priority ---
with n.section("Tasks by Priority"):
    df = analyst.sql("SELECT priority, COUNT(*) AS count FROM tasks GROUP BY 1 ORDER BY count DESC").df()

    pcolors = {"critical": "#e74c3c", "high": "#e67e22", "medium": "#f1c40f", "low": "#2ecc71"}
    fig, ax = plt.subplots(figsize=(6, 4))
    bars = ax.bar(df["priority"], df["count"], color=[pcolors.get(p, "#ccc") for p in df["priority"]])
    ax.bar_label(bars)
    ax.set_ylabel("Count")
    ax.set_title("Tasks by Priority")
    fig.tight_layout()
    n.figure(fig, "tasks_by_priority.png", caption="Task count by priority level")
    plt.close(fig)

# --- Workload by Assignee ---
with n.section("Workload by Assignee"):
    df = analyst.sql("""
        SELECT t.assignee, ROUND(SUM(t.hours_logged), 1) AS hours_logged,
               ROUND(SUM(t.estimated_hours), 1) AS estimated_hours
        FROM tasks t WHERE t.status != 'todo'
        GROUP BY 1 ORDER BY hours_logged DESC
    """).df()

    x = np.arange(len(df))
    width = 0.35
    fig, ax = plt.subplots(figsize=(7, 4))
    ax.bar(x - width / 2, df["hours_logged"], width, label="Hours Logged", color="#636EFA")
    ax.bar(x + width / 2, df["estimated_hours"], width, label="Estimated", color="#C8C8C8")
    ax.set_xticks(x)
    ax.set_xticklabels(df["assignee"])
    ax.set_ylabel("Hours")
    ax.legend()
    ax.set_title("Actual vs Estimated Hours")
    fig.tight_layout()
    n.figure(fig, "workload_by_assignee.png", caption="Actual vs estimated hours by assignee")
    plt.close(fig)

# --- Project Health ---
with n.section("Project Health"):
    df = analyst.sql("""
        SELECT p.name AS project, p.budget, COUNT(t.id) AS total_tasks,
               COUNT(t.id) FILTER (WHERE t.status = 'done') AS completed,
               ROUND(COUNT(t.id) FILTER (WHERE t.status = 'done') * 100.0 / COUNT(t.id), 1) AS completion_pct,
               ROUND(SUM(t.hours_logged), 1) AS hours_spent
        FROM projects p JOIN tasks t ON p.id = t.project_id
        GROUP BY 1, 2 ORDER BY completion_pct DESC
    """).df()

    fig, ax = plt.subplots(figsize=(7, 4))
    colors = ["#2ecc71" if p >= 60 else "#f39c12" for p in df["completion_pct"]]
    bars = ax.bar(df["project"], df["completion_pct"], color=colors)
    ax.bar_label(bars, labels=[f"{p}%" for p in df["completion_pct"]], padding=3)
    ax.set_ylabel("Completion %")
    ax.set_ylim(0, 100)
    ax.set_title("Project Completion Rates")
    fig.tight_layout()
    n.figure(fig, "project_health.png", caption="Project completion rates")
    plt.close(fig)

    n.table(
        [{"Project": r["project"], "Budget": f"${r['budget']:,}", "Tasks": r["total_tasks"],
          "Done": r["completed"], "Completion": f"{r['completion_pct']}%", "Hours": r["hours_spent"]}
         for _, r in df.iterrows()],
        name="Project Details",
    )

# --- Team Cost Analysis ---
with n.section("Team Cost Analysis"):
    df = analyst.sql("""
        SELECT tm.team, COUNT(DISTINCT tm.id) AS members,
               ROUND(SUM(t.hours_logged), 1) AS total_hours,
               ROUND(SUM(t.hours_logged * tm.hourly_rate), 2) AS total_cost
        FROM tasks t JOIN team tm ON t.assignee = tm.name
        GROUP BY 1 ORDER BY total_cost DESC
    """).df()

    fig, ax = plt.subplots(figsize=(6, 4))
    bars = ax.bar(df["team"], df["total_cost"], color="#EF553B")
    ax.bar_label(bars, labels=[f"${c:,.0f}" for c in df["total_cost"]], padding=3)
    ax.set_ylabel("Total Cost ($)")
    ax.set_title("Labor Cost by Team")
    fig.tight_layout()
    n.figure(fig, "team_cost.png", caption="Total labor cost by team")
    plt.close(fig)

n.save()
print("Dashboard saved to dashboard.md")
