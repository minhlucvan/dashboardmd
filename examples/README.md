# Examples

Each example is a self-contained project with its own data, Python script, generated dashboard output, and README.

All examples use **Plotly** for interactive charts via **notebookmd**'s native `plotly_chart()` support.

## Projects

| Example | Description | Approach |
|---------|-------------|----------|
| [sales-analytics](sales-analytics/) | E-commerce dashboard with orders, customers, and products | `Analyst` + `notebookmd` + Plotly bar/pie/dual-axis charts |
| [hr-dashboard](hr-dashboard/) | Workforce analytics — headcount, compensation, budget utilization | `Analyst` + `notebookmd` + Plotly grouped bars, horizontal bars, pie |
| [web-traffic](web-traffic/) | Website traffic, conversion funnel, and source attribution | `Analyst` + `notebookmd` + Plotly funnel, multi-line, grouped bars |
| [custom-connector](custom-connector/) | Custom `SourceHandler` subclasses — API and generator connectors | Custom connectors + `Analyst` + `notebookmd` + Plotly |

## Structure

Each example follows the same layout:

```
example-name/
├── data/           # CSV/JSON data files
├── assets/         # Generated Plotly chart HTML files
├── dashboard.py    # Python script that builds the dashboard
├── dashboard.md    # Generated Markdown output
└── README.md       # What it demonstrates and how to run it
```

## Running

```bash
pip install "dashboardmd[plotly]"

cd examples/sales-analytics
python dashboard.py
# → generates dashboard.md + assets/*.html

cd ../hr-dashboard
python dashboard.py

cd ../web-traffic
python dashboard.py

cd ../custom-connector
python dashboard.py
```
