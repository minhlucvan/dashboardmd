# HR Dashboard Example

A workforce analytics dashboard covering headcount, compensation, budget utilization, and tenure.

## Data

- **employees.csv** — 15 employees with department, title, hire date, salary, and status
- **departments.csv** — 4 departments with budget and location

## What It Demonstrates

- Using `Analyst.to_md()` for SQL-driven reports (no semantic layer needed)
- Pure SQL queries with DuckDB features (FILTER, DATEDIFF, CASE expressions)
- Multiple analysis sections: headcount, budgets, tenure, salary distribution
- Simple setup — just register CSV files and write SQL

## Running

```bash
cd examples/hr-dashboard
pip install dashboardmd
python dashboard.py
```

The script generates `dashboard.md` in this directory.
