# Examples

Each example is a self-contained project with its own data, Python script, generated dashboard output, and README.

All examples use **matplotlib** for PNG chart images via **notebookmd**'s native `bar_chart()`, `line_chart()`, and `figure()` support. No browser required.

## Projects

| Example | Description | Charts |
|---------|-------------|--------|
| [sales-analytics](sales-analytics/) | E-commerce dashboard with orders, customers, and products | Bar, pie, dual-axis trend |
| [hr-dashboard](hr-dashboard/) | Workforce analytics — headcount, compensation, budget utilization | Grouped bars, horizontal bars, pie |
| [web-traffic](web-traffic/) | Website traffic, conversion funnel, and source attribution | Funnel, multi-line, grouped bars |
| [custom-connector](custom-connector/) | Custom `SourceHandler` subclasses — API and generator connectors | Color-coded bars, pie, grouped bars |

## Structure

Each example follows the same layout:

```
example-name/
├── data/           # CSV/JSON data files
├── assets/         # Generated PNG chart images
├── dashboard.py    # Python script that builds the dashboard
├── dashboard.md    # Generated Markdown output
└── README.md       # What it demonstrates and how to run it
```

## Running

```bash
pip install "dashboardmd[plotting]"

cd examples/sales-analytics
python dashboard.py
# → generates dashboard.md + assets/*.png

cd ../hr-dashboard
python dashboard.py

cd ../web-traffic
python dashboard.py

cd ../custom-connector
python dashboard.py
```
