# Examples

Each example is a self-contained project with its own data, Python script, generated dashboard output, and README.

## Projects

| Example | Description | Approach |
|---------|-------------|----------|
| [sales-analytics](sales-analytics/) | E-commerce dashboard with orders, customers, and products | `Dashboard` with semantic entities + raw SQL tiles |
| [hr-dashboard](hr-dashboard/) | Workforce analytics — headcount, compensation, budget utilization | `Analyst.to_md()` with pure SQL queries |
| [web-traffic](web-traffic/) | Website traffic, conversion funnel, and source attribution | `Dashboard` mixing semantic tiles and SQL tiles |

## Structure

Each example follows the same layout:

```
example-name/
├── data/           # CSV data files
├── dashboard.py    # Python script that builds the dashboard
├── dashboard.md    # Generated Markdown output
└── README.md       # What it demonstrates and how to run it
```

## Running

```bash
pip install dashboardmd

cd examples/sales-analytics
python dashboard.py
# → generates dashboard.md

cd ../hr-dashboard
python dashboard.py
# → generates dashboard.md

cd ../web-traffic
python dashboard.py
# → generates dashboard.md
```
