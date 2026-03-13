"""HR Dashboard.

Workforce analytics covering headcount, compensation, and department metrics.
Uses Analyst for queries and notebookmd with Plotly for rich visualizations.
"""

import plotly.express as px
import plotly.graph_objects as go
from notebookmd import nb

from dashboardmd import Analyst

# ---------------------------------------------------------------------------
# Set up analyst with data
# ---------------------------------------------------------------------------

analyst = Analyst()
analyst.add("employees", "data/employees.csv")
analyst.add("departments", "data/departments.csv")

# ---------------------------------------------------------------------------
# Build report with notebookmd + plotly
# ---------------------------------------------------------------------------

n = nb("dashboard.md", title="HR Dashboard")

# --- Headcount Summary ---
with n.section("Headcount Summary"):
    kpis = analyst.sql("""
        SELECT
            COUNT(*) AS total,
            COUNT(*) FILTER (WHERE status = 'active') AS active,
            COUNT(*) FILTER (WHERE status = 'terminated') AS terminated,
            ROUND(AVG(salary), 0) AS avg_salary
        FROM employees
    """).fetchone()

    n.metric_row([
        {"label": "Total Employees", "value": kpis[0]},
        {"label": "Active", "value": kpis[1]},
        {"label": "Terminated", "value": kpis[2]},
        {"label": "Avg Salary", "value": f"${kpis[3]:,.0f}"},
    ])

# --- Headcount by Department ---
with n.section("Headcount by Department"):
    rows = analyst.sql("""
        SELECT d.name AS department, COUNT(e.id) AS headcount, ROUND(AVG(e.salary), 0) AS avg_salary
        FROM employees e JOIN departments d ON e.department_id = d.id
        WHERE e.status = 'active'
        GROUP BY 1 ORDER BY headcount DESC
    """).fetchall()

    fig = px.bar(
        x=[r[0] for r in rows], y=[r[1] for r in rows],
        text=[f"${r[2]:,.0f} avg" for r in rows],
        labels={"x": "Department", "y": "Headcount"},
        color_discrete_sequence=["#636EFA"],
    )
    fig.update_traces(textposition="outside")
    fig.update_layout(showlegend=False)
    n.plotly_chart(fig, filename="headcount_by_dept.html", caption="Headcount and average salary by department")

# --- Budget Utilization ---
with n.section("Department Budget Utilization"):
    rows = analyst.sql("""
        SELECT d.name AS department, d.budget, SUM(e.salary) AS total_salaries,
               ROUND(SUM(e.salary) * 100.0 / d.budget, 1) AS utilization_pct
        FROM departments d
        LEFT JOIN employees e ON d.id = e.department_id AND e.status = 'active'
        GROUP BY 1, 2 ORDER BY utilization_pct DESC
    """).fetchall()

    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=[r[0] for r in rows], y=[r[1] for r in rows],
        name="Budget", marker_color="#C8C8C8",
    ))
    fig.add_trace(go.Bar(
        x=[r[0] for r in rows], y=[r[2] for r in rows],
        name="Salaries", marker_color="#636EFA",
        text=[f"{r[3]}%" for r in rows], textposition="outside",
    ))
    fig.update_layout(
        barmode="group", yaxis_title="Amount ($)",
        legend={"orientation": "h", "yanchor": "bottom", "y": 1.02},
    )
    n.plotly_chart(fig, filename="budget_utilization.html", caption="Budget vs salary spend by department")

# --- Salary Distribution ---
with n.section("Salary Distribution by Title"):
    rows = analyst.sql("""
        SELECT title, COUNT(*) AS count, ROUND(AVG(salary), 0) AS avg_salary,
               MIN(salary) AS min_salary, MAX(salary) AS max_salary
        FROM employees WHERE status = 'active'
        GROUP BY 1 ORDER BY avg_salary DESC
    """).fetchall()

    fig = px.bar(
        x=[r[2] for r in rows], y=[r[0] for r in rows],
        orientation="h",
        labels={"x": "Average Salary ($)", "y": "Title"},
        color_discrete_sequence=["#00CC96"],
    )
    fig.update_layout(yaxis={"categoryorder": "total ascending"}, showlegend=False)
    n.plotly_chart(fig, filename="salary_by_title.html", caption="Average salary by title")

# --- Tenure Distribution ---
with n.section("Tenure Distribution"):
    rows = analyst.sql("""
        SELECT
            CASE
                WHEN DATEDIFF('month', hire_date, CURRENT_DATE) < 12 THEN '< 1 year'
                WHEN DATEDIFF('month', hire_date, CURRENT_DATE) < 24 THEN '1-2 years'
                WHEN DATEDIFF('month', hire_date, CURRENT_DATE) < 36 THEN '2-3 years'
                ELSE '3+ years'
            END AS tenure_bucket,
            COUNT(*) AS employees
        FROM employees WHERE status = 'active'
        GROUP BY 1 ORDER BY 1
    """).fetchall()

    fig = px.pie(
        names=[r[0] for r in rows], values=[r[1] for r in rows],
        color_discrete_sequence=px.colors.qualitative.Pastel,
    )
    n.plotly_chart(fig, filename="tenure_distribution.html", caption="Employee tenure distribution")

# --- Recent Hires ---
with n.section("Recent Hires (Last 12 Months)"):
    result = analyst.sql("""
        SELECT e.name, d.name AS department, e.title, e.hire_date, e.salary
        FROM employees e JOIN departments d ON e.department_id = d.id
        WHERE e.hire_date >= CURRENT_DATE - INTERVAL '12 months' AND e.status = 'active'
        ORDER BY e.hire_date DESC
    """)
    n.table(result.df(), name="Recent Hires")

n.save()
print("Dashboard saved to dashboard.md")
