"""HR Dashboard.

Workforce analytics covering headcount, compensation, and department metrics.
Uses Analyst for queries and notebookmd with matplotlib for PNG chart output.
"""

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
from notebookmd import nb

from dashboardmd import Analyst

# ---------------------------------------------------------------------------
# Set up analyst with data
# ---------------------------------------------------------------------------

analyst = Analyst()
analyst.add("employees", "data/employees.csv")
analyst.add("departments", "data/departments.csv")

# ---------------------------------------------------------------------------
# Build report with notebookmd + matplotlib
# ---------------------------------------------------------------------------

n = nb("dashboard.md", title="HR Dashboard")

# --- Headcount Summary ---
with n.section("Headcount Summary"):
    kpis = analyst.sql("""
        SELECT COUNT(*) AS total,
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
    df = analyst.sql("""
        SELECT d.name AS department, COUNT(e.id) AS headcount, ROUND(AVG(e.salary), 0) AS avg_salary
        FROM employees e JOIN departments d ON e.department_id = d.id
        WHERE e.status = 'active'
        GROUP BY 1 ORDER BY headcount DESC
    """).df()

    fig, ax = plt.subplots(figsize=(7, 4))
    bars = ax.bar(df["department"], df["headcount"], color="#636EFA")
    ax.bar_label(bars, labels=[f"${s:,.0f} avg" for s in df["avg_salary"]], padding=3, fontsize=8)
    ax.set_ylabel("Headcount")
    ax.set_title("Headcount by Department")
    fig.tight_layout()
    n.figure(fig, "headcount_by_dept.png", caption="Headcount and average salary by department")
    plt.close(fig)

# --- Budget Utilization ---
with n.section("Department Budget Utilization"):
    df = analyst.sql("""
        SELECT d.name AS department, d.budget, SUM(e.salary) AS total_salaries,
               ROUND(SUM(e.salary) * 100.0 / d.budget, 1) AS utilization_pct
        FROM departments d
        LEFT JOIN employees e ON d.id = e.department_id AND e.status = 'active'
        GROUP BY 1, 2 ORDER BY utilization_pct DESC
    """).df()

    x = np.arange(len(df))
    width = 0.35
    fig, ax = plt.subplots(figsize=(7, 4))
    ax.bar(x - width / 2, df["budget"], width, label="Budget", color="#C8C8C8")
    bars2 = ax.bar(x + width / 2, df["total_salaries"], width, label="Salaries", color="#636EFA")
    ax.bar_label(bars2, labels=[f"{p}%" for p in df["utilization_pct"]], padding=3, fontsize=8)
    ax.set_xticks(x)
    ax.set_xticklabels(df["department"])
    ax.set_ylabel("Amount ($)")
    ax.legend()
    ax.set_title("Budget vs Salary Spend")
    fig.tight_layout()
    n.figure(fig, "budget_utilization.png", caption="Budget vs salary spend by department")
    plt.close(fig)

# --- Salary Distribution ---
with n.section("Salary Distribution by Title"):
    df = analyst.sql("""
        SELECT title, COUNT(*) AS count, ROUND(AVG(salary), 0) AS avg_salary
        FROM employees WHERE status = 'active'
        GROUP BY 1 ORDER BY avg_salary DESC
    """).df()

    fig, ax = plt.subplots(figsize=(7, 5))
    ax.barh(df["title"], df["avg_salary"], color="#00CC96")
    ax.set_xlabel("Average Salary ($)")
    ax.set_title("Salary by Title")
    ax.invert_yaxis()
    fig.tight_layout()
    n.figure(fig, "salary_by_title.png", caption="Average salary by title")
    plt.close(fig)

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

    fig, ax = plt.subplots(figsize=(5, 4))
    ax.pie([r[1] for r in rows], labels=[r[0] for r in rows], autopct="%1.0f%%",
           colors=["#FFB3BA", "#BAFFC9", "#BAE1FF", "#FFFFBA"])
    ax.set_title("Tenure Distribution")
    n.figure(fig, "tenure_distribution.png", caption="Employee tenure distribution")
    plt.close(fig)

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
