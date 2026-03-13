# HR Dashboard Example

A workforce analytics dashboard covering headcount, compensation, budget utilization, and tenure.

## Data

- **employees.csv** — 15 employees with department, title, hire date, salary, and status
- **departments.csv** — 4 departments with budget and location

## What It Demonstrates

- Using `Analyst` for SQL queries + `notebookmd` for rendering
- Matplotlib charts rendered as PNG images (no browser needed)
- KPI metric cards for headcount summary
- DuckDB features in SQL (FILTER, DATEDIFF, CASE expressions)
- Tables for detailed data (recent hires)

## Charts

- Headcount by department with avg salary annotations (bar)
- Budget vs salary spend comparison (grouped bar)
- Salary distribution by title (horizontal bar)
- Tenure distribution (pie)

## Running

```bash
cd examples/hr-dashboard
pip install "dashboardmd[plotting]"
python dashboard.py
```

Generates `dashboard.md` with PNG chart images in `assets/`.
