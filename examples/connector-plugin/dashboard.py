#!/usr/bin/env python3
"""Connector Plugin Example — Composable Connectors.

Demonstrates the key idea: multiple connectors install into one Analyst,
their entities join across connector boundaries, and widgets from different
connectors compose into a unified dashboard.

Two connectors:
- ProjectConnector: tasks + time logs (project management)
- TeamConnector: team directory (HR)

Combined, they answer questions neither could alone:
"Which department logs the most hours?" or "Task completion rate by role?"
"""

from pathlib import Path

from notebookmd import nb

from dashboardmd import Analyst, Relationship

# Import our two connectors
from connectors import ProjectConnector, TeamConnector

# ---------------------------------------------------------------------------
# Setup: install both connectors into one Analyst
# ---------------------------------------------------------------------------

data_dir = Path(__file__).parent / "data"

project = ProjectConnector(
    tasks_path=str(data_dir / "tasks.json"),
    time_logs_path=str(data_dir / "time_logs.csv"),
)

team = TeamConnector(
    csv_path=str(data_dir / "team.csv"),
)

analyst = Analyst()
analyst.use(project)  # registers: tasks, time_logs
analyst.use(team)     # registers: team_members

# Cross-connector relationship: link tasks/time_logs to team_members
analyst.add_relationship(Relationship(
    "tasks", "team_members",
    on=("assignee", "username"),
    type="many_to_one",
))
analyst.add_relationship(Relationship(
    "time_logs", "team_members",
    on=("username", "username"),
    type="many_to_one",
))

# ---------------------------------------------------------------------------
# Build a unified dashboard mixing widgets + custom cross-connector analysis
# ---------------------------------------------------------------------------

output_dir = Path(__file__).parent
n = nb(str(output_dir / "dashboard.md"))

n.title("Engineering Team Dashboard")
n.text("*Composable connector example — Project + Team data joined together*")
n.divider()

# --- Section 1: Project data (from ProjectConnector) ---
n.header("Project Status")
n.text("*Data from ProjectConnector*")

result = analyst.sql("""
    SELECT status, COUNT(*) as count
    FROM tasks GROUP BY status
    ORDER BY count DESC
""")
df = result.df()
n.bar_chart(df, x="status", y="count")

result = analyst.sql("""
    SELECT priority, COUNT(*) as count
    FROM tasks GROUP BY priority
    ORDER BY CASE priority
        WHEN 'high' THEN 1 WHEN 'medium' THEN 2 WHEN 'low' THEN 3
    END
""")
n.table(result.df())

# --- Section 2: Team data (from TeamConnector) ---
n.header("Team Directory")
n.text("*Data from TeamConnector*")

result = analyst.sql("""
    SELECT department, COUNT(*) as headcount
    FROM team_members GROUP BY department
""")
n.table(result.df())

result = analyst.sql("""
    SELECT full_name, role, department, location
    FROM team_members ORDER BY department, full_name
""")
n.table(result.df())

# --- Section 3: Cross-connector analysis (the magic!) ---
n.divider()
n.header("Cross-Connector Insights")
n.text("*Queries that span both connectors — only possible because they compose*")

# Hours by department (time_logs × team_members join)
n.subheader("Hours Logged by Department")
result = analyst.sql("""
    SELECT
        tm.department,
        SUM(tl.hours) as total_hours,
        COUNT(DISTINCT tl.username) as contributors
    FROM time_logs tl
    JOIN team_members tm ON tl.username = tm.username
    GROUP BY tm.department
    ORDER BY total_hours DESC
""")
df = result.df()
n.bar_chart(df, x="department", y="total_hours")
n.table(df)

# Task completion by role
n.subheader("Task Completion by Role")
result = analyst.sql("""
    SELECT
        tm.role,
        COUNT(*) as total_tasks,
        SUM(CASE WHEN t.status = 'done' THEN 1 ELSE 0 END) as completed,
        ROUND(100.0 * SUM(CASE WHEN t.status = 'done' THEN 1 ELSE 0 END) / COUNT(*), 1)
            as completion_pct
    FROM tasks t
    JOIN team_members tm ON t.assignee = tm.username
    GROUP BY tm.role
    ORDER BY completion_pct DESC
""")
n.table(result.df())

# Estimate accuracy by department
n.subheader("Estimate Accuracy by Department")
result = analyst.sql("""
    SELECT
        tm.department,
        SUM(t.estimate_hours) as estimated,
        SUM(t.actual_hours) as actual,
        ROUND(100.0 * SUM(t.actual_hours) / NULLIF(SUM(t.estimate_hours), 0), 1)
            as accuracy_pct
    FROM tasks t
    JOIN team_members tm ON t.assignee = tm.username
    WHERE t.actual_hours > 0
    GROUP BY tm.department
    ORDER BY accuracy_pct
""")
n.table(result.df())

# Workload distribution: who's doing what?
n.subheader("Workload by Team Member")
result = analyst.sql("""
    SELECT
        tm.full_name,
        tm.role,
        COUNT(DISTINCT t.id) as tasks_assigned,
        SUM(tl.hours) as hours_logged,
        ROUND(SUM(tl.hours) / NULLIF(COUNT(DISTINCT tl.date), 0), 1) as avg_hours_per_day
    FROM team_members tm
    LEFT JOIN tasks t ON tm.username = t.assignee
    LEFT JOIN time_logs tl ON tm.username = tl.username
    GROUP BY tm.full_name, tm.role
    ORDER BY hours_logged DESC
""")
n.table(result.df())

# --- Section 4: Show installed connectors ---
n.divider()
n.header("Installed Connectors")

for cname, connector in analyst.connectors.items():
    n.subheader(cname)
    dashboards = connector.available_dashboards()
    if dashboards:
        n.text(f"Available widgets: {', '.join(dashboards)}")
    entities = connector.entities()
    entity_names = [e.name for e in entities]
    n.text(f"Entities: {', '.join(entity_names)}")

n.save()
print(f"Dashboard saved to {output_dir / 'dashboard.md'}")
print(f"\nInstalled connectors: {list(analyst.connectors.keys())}")
print(f"All tables: {analyst.tables()}")
