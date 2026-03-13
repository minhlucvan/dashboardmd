# Web Traffic Example

A website analytics dashboard tracking pageviews, traffic sources, devices, and conversion funnel.

## Data

- **pageviews.csv** — 30 pageview events with page, source, device, session, and duration
- **conversions.csv** — 6 conversion events (trial signups and purchases)

## What It Demonstrates

- Using `Analyst` for SQL queries + `notebookmd` for rendering
- Plotly funnel chart for conversion analysis
- Grouped bar charts for source attribution (sessions vs conversions)
- Multi-line time series for daily traffic trends
- Horizontal bar charts with duration annotations
- Pie chart for device distribution

## Charts

- Traffic by source (bar)
- Device distribution (pie)
- Top pages ranked by views (horizontal bar with avg duration)
- Conversion funnel: sessions → pricing → signup → trial → purchase (funnel)
- Sessions vs conversions by source (grouped bar with conversion rate)
- Daily traffic trend: pageviews, sessions, visitors (multi-line)

## Running

```bash
cd examples/web-traffic
pip install "dashboardmd[plotly]"
python dashboard.py
```

Generates `dashboard.md` with interactive charts in `assets/`.
