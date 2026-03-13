# Sales Analytics Example

A sales analytics dashboard analyzing orders, customers, and products for an e-commerce business.

## Data

- **orders.csv** — 25 orders with date, customer, product, quantity, amount, and status
- **customers.csv** — 12 customers across Enterprise, Mid-Market, and SMB segments in 4 regions
- **products.csv** — 5 products in Widgets, Gadgets, and Accessories categories

## What It Demonstrates

- Defining entities with dimensions and measures (semantic layer)
- Setting up relationships between entities (orders → customers, orders → products)
- KPI tiles (single-value metrics like total revenue, order count)
- Grouped tiles (revenue broken down by segment, category, status)
- Raw SQL tiles for advanced analysis (top customers, monthly trends, regional performance)
- Global filters

## Running

```bash
cd examples/sales-analytics
pip install dashboardmd
python dashboard.py
```

The script generates `dashboard.md` in this directory.
