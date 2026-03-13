"""Sales Analytics Dashboard.

Analyzes order data across customers, products, and time periods.
Demonstrates both semantic (BI-style) queries and raw SQL tiles.
"""

from dashboardmd import Analyst, Dashboard, Dimension, Entity, Measure, Relationship

# ---------------------------------------------------------------------------
# Define entities (semantic layer)
# ---------------------------------------------------------------------------

orders = Entity(
    "orders",
    source="data/orders.csv",
    dimensions=[
        Dimension("id", type="number", primary_key=True),
        Dimension("date", type="time"),
        Dimension("customer_id", type="number"),
        Dimension("product_id", type="number"),
        Dimension("status", type="string"),
    ],
    measures=[
        Measure("revenue", type="sum", sql="amount"),
        Measure("order_count", type="count"),
        Measure("avg_order_value", type="avg", sql="amount"),
        Measure("total_quantity", type="sum", sql="quantity"),
    ],
)

customers = Entity(
    "customers",
    source="data/customers.csv",
    dimensions=[
        Dimension("id", type="number", primary_key=True),
        Dimension("name", type="string"),
        Dimension("segment", type="string"),
        Dimension("region", type="string"),
    ],
    measures=[
        Measure("customer_count", type="count_distinct", sql="id"),
    ],
)

products = Entity(
    "products",
    source="data/products.csv",
    dimensions=[
        Dimension("id", type="number", primary_key=True),
        Dimension("name", type="string"),
        Dimension("category", type="string"),
    ],
    measures=[
        Measure("product_count", type="count_distinct", sql="id"),
    ],
)

# ---------------------------------------------------------------------------
# Relationships
# ---------------------------------------------------------------------------

relationships = [
    Relationship("orders", "customers", on=("customer_id", "id"), type="many_to_one"),
    Relationship("orders", "products", on=("product_id", "id"), type="many_to_one"),
]

# ---------------------------------------------------------------------------
# Build dashboard
# ---------------------------------------------------------------------------

dash = Dashboard(
    title="Sales Analytics Dashboard",
    entities=[orders, customers, products],
    relationships=relationships,
    output="dashboard.md",
)

# Global filter
dash.filter("date_range", dimension="orders.date", default="last_90_days")

# KPI section — single-value metrics
dash.section("Key Metrics")
dash.tile("orders.revenue")
dash.tile("orders.order_count")
dash.tile("orders.avg_order_value")

# Revenue breakdown by customer segment
dash.section("Revenue by Customer Segment")
dash.tile("orders.revenue", by="customers.segment", type="bar_chart")

# Revenue breakdown by product category
dash.section("Revenue by Product Category")
dash.tile("orders.revenue", by="products.category", type="bar_chart")

# Order status distribution
dash.section("Order Status")
dash.tile("orders.order_count", by="orders.status")

# Custom SQL tiles for advanced analysis
dash.section("Top 5 Customers by Revenue")
dash.tile_sql(
    "Top Customers",
    """
    SELECT c.name, c.segment, SUM(o.amount) AS total_revenue, COUNT(*) AS orders
    FROM orders o
    JOIN customers c ON o.customer_id = c.id
    WHERE o.status = 'completed'
    GROUP BY 1, 2
    ORDER BY total_revenue DESC
    LIMIT 5
    """,
)

dash.section("Monthly Revenue Trend")
dash.tile_sql(
    "Monthly Trend",
    """
    SELECT
        strftime(date, '%Y-%m') AS month,
        COUNT(*) AS orders,
        SUM(amount) AS revenue,
        ROUND(AVG(amount), 2) AS avg_order
    FROM orders
    WHERE status = 'completed'
    GROUP BY 1
    ORDER BY 1
    """,
)

dash.section("Regional Performance")
dash.tile_sql(
    "By Region",
    """
    SELECT
        c.region,
        COUNT(DISTINCT c.id) AS customers,
        COUNT(o.id) AS orders,
        SUM(o.amount) AS revenue
    FROM orders o
    JOIN customers c ON o.customer_id = c.id
    WHERE o.status = 'completed'
    GROUP BY 1
    ORDER BY revenue DESC
    """,
)

# Generate the dashboard
dash.save()
print(f"Dashboard saved to {dash.output}")
