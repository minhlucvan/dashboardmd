# Sales Analytics Example

A sales analytics dashboard analyzing orders, customers, and products for an e-commerce business.

## Data

- **orders.csv** — 25 orders with date, customer, product, quantity, amount, and status
- **customers.csv** — 12 customers across Enterprise, Mid-Market, and SMB segments in 4 regions
- **products.csv** — 5 products in Widgets, Gadgets, and Accessories categories

## What It Demonstrates

- Using `Analyst` for SQL queries + `notebookmd` for rendering
- Matplotlib charts rendered as PNG images (no browser needed)
- KPI metric cards via `n.metric_row()`
- Built-in `n.bar_chart()` for simple charts, `n.figure()` for custom matplotlib plots
- Tables via `n.table()` for detailed data (top customers)

## Charts

- Revenue by customer segment (bar)
- Revenue by product category (pie)
- Order status distribution (color-coded bar)
- Monthly revenue + order count trend (dual axis)
- Regional performance (annotated bar)

## Running

```bash
cd examples/sales-analytics
pip install "dashboardmd[plotting]"
python dashboard.py
```

Generates `dashboard.md` with PNG chart images in `assets/`.
