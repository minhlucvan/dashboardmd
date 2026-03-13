# Web Traffic Example

A website analytics dashboard tracking pageviews, traffic sources, devices, and conversion funnel.

## Data

- **pageviews.csv** — 30 pageview events with page, source, device, session, and duration
- **conversions.csv** — 6 conversion events (trial signups and purchases)

## What It Demonstrates

- Combining semantic tiles (views by source, by device) with raw SQL tiles
- Conversion funnel analysis using CTEs
- Source attribution with LEFT JOIN for conversion rates
- Count-distinct measures for unique sessions and visitors
- Daily traffic trend aggregation

## Running

```bash
cd examples/web-traffic
pip install dashboardmd
python dashboard.py
```

The script generates `dashboard.md` in this directory.
