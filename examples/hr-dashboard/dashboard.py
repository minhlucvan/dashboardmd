"""HR Dashboard.

Workforce analytics covering headcount, compensation, and department metrics.
Demonstrates the Analyst.to_md() approach for SQL-driven reports.
"""

from dashboardmd import Analyst

# ---------------------------------------------------------------------------
# Set up analyst with data sources (no semantic layer — pure SQL)
# ---------------------------------------------------------------------------

analyst = Analyst()
analyst.add("employees", "data/employees.csv")
analyst.add("departments", "data/departments.csv")

# ---------------------------------------------------------------------------
# Build report via to_md() — a list of (section_title, sql) pairs
# ---------------------------------------------------------------------------

analyst.to_md(
    "dashboard.md",
    title="HR Dashboard",
    queries=[
        (
            "Headcount Summary",
            """
            SELECT
                COUNT(*) AS total_employees,
                COUNT(*) FILTER (WHERE status = 'active') AS active,
                COUNT(*) FILTER (WHERE status = 'terminated') AS terminated,
                ROUND(AVG(salary), 0) AS avg_salary
            FROM employees
            """,
        ),
        (
            "Headcount by Department",
            """
            SELECT
                d.name AS department,
                COUNT(e.id) AS headcount,
                ROUND(AVG(e.salary), 0) AS avg_salary,
                MIN(e.salary) AS min_salary,
                MAX(e.salary) AS max_salary
            FROM employees e
            JOIN departments d ON e.department_id = d.id
            WHERE e.status = 'active'
            GROUP BY 1
            ORDER BY headcount DESC
            """,
        ),
        (
            "Department Budget Utilization",
            """
            SELECT
                d.name AS department,
                d.budget,
                SUM(e.salary) AS total_salaries,
                ROUND(SUM(e.salary) * 100.0 / d.budget, 1) AS utilization_pct
            FROM departments d
            LEFT JOIN employees e ON d.id = e.department_id AND e.status = 'active'
            GROUP BY 1, 2
            ORDER BY utilization_pct DESC
            """,
        ),
        (
            "Tenure Distribution",
            """
            SELECT
                CASE
                    WHEN DATEDIFF('month', hire_date, CURRENT_DATE) < 12 THEN '< 1 year'
                    WHEN DATEDIFF('month', hire_date, CURRENT_DATE) < 24 THEN '1-2 years'
                    WHEN DATEDIFF('month', hire_date, CURRENT_DATE) < 36 THEN '2-3 years'
                    ELSE '3+ years'
                END AS tenure_bucket,
                COUNT(*) AS employees
            FROM employees
            WHERE status = 'active'
            GROUP BY 1
            ORDER BY 1
            """,
        ),
        (
            "Salary Distribution by Title",
            """
            SELECT
                title,
                COUNT(*) AS count,
                ROUND(AVG(salary), 0) AS avg_salary,
                MIN(salary) AS min_salary,
                MAX(salary) AS max_salary
            FROM employees
            WHERE status = 'active'
            GROUP BY 1
            ORDER BY avg_salary DESC
            """,
        ),
        (
            "Recent Hires (Last 12 Months)",
            """
            SELECT
                e.name,
                d.name AS department,
                e.title,
                e.hire_date,
                e.salary
            FROM employees e
            JOIN departments d ON e.department_id = d.id
            WHERE e.hire_date >= CURRENT_DATE - INTERVAL '12 months'
              AND e.status = 'active'
            ORDER BY e.hire_date DESC
            """,
        ),
    ],
)

print("Dashboard saved to dashboard.md")
