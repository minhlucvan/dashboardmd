"""Sales Analytics Dashboard.

Analyzes order data across customers, products, and time periods.
Uses Analyst for queries and notebookmd with matplotlib for PNG chart output.
"""

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd
from notebookmd import nb

from dashboardmd import Analyst

# ---------------------------------------------------------------------------
# Set up analyst with data
# ---------------------------------------------------------------------------

analyst = Analyst()
analyst.add("orders", "data/orders.csv")
analyst.add("customers", "data/customers.csv")
analyst.add("products", "data/products.csv")

# ---------------------------------------------------------------------------
# Build report with notebookmd + matplotlib
# ---------------------------------------------------------------------------

n = nb("dashboard.md", title="Sales Analytics Dashboard")

# --- Key Metrics ---
with n.section("Key Metrics"):
    kpis = analyst.sql("""
        SELECT
            SUM(amount) AS revenue,
            COUNT(*) AS order_count,
            ROUND(AVG(amount), 2) AS avg_order_value,
            SUM(quantity) AS total_quantity
        FROM orders
    """).fetchone()

    n.metric_row([
        {"label": "Total Revenue", "value": f"${kpis[0]:,.2f}"},
        {"label": "Orders", "value": kpis[1]},
        {"label": "Avg Order Value", "value": f"${kpis[2]:,.2f}"},
        {"label": "Units Sold", "value": kpis[3]},
    ])

# --- Revenue by Customer Segment ---
with n.section("Revenue by Customer Segment"):
    df = analyst.sql("""
        SELECT c.segment, SUM(o.amount) AS revenue
        FROM orders o JOIN customers c ON o.customer_id = c.id
        WHERE o.status = 'completed'
        GROUP BY 1 ORDER BY revenue DESC
    """).df()

    n.bar_chart(df, x="segment", y="revenue", title="Revenue by Segment")

# --- Revenue by Product Category ---
with n.section("Revenue by Product Category"):
    rows = analyst.sql("""
        SELECT p.category, SUM(o.amount) AS revenue
        FROM orders o JOIN products p ON o.product_id = p.id
        WHERE o.status = 'completed'
        GROUP BY 1 ORDER BY revenue DESC
    """).fetchall()

    fig, ax = plt.subplots(figsize=(6, 4))
    ax.pie([r[1] for r in rows], labels=[r[0] for r in rows], autopct="%1.0f%%",
           colors=["#66c2a5", "#fc8d62", "#8da0cb"])
    ax.set_title("Revenue by Category")
    n.figure(fig, "revenue_by_category.png", caption="Revenue share by product category")
    plt.close(fig)

# --- Order Status ---
with n.section("Order Status"):
    df = analyst.sql("""
        SELECT status, COUNT(*) AS count FROM orders GROUP BY 1 ORDER BY count DESC
    """).df()

    colors = {"completed": "#2ecc71", "pending": "#f39c12", "cancelled": "#e74c3c"}
    fig, ax = plt.subplots(figsize=(6, 4))
    bars = ax.bar(df["status"], df["count"], color=[colors.get(s, "#636EFA") for s in df["status"]])
    ax.set_ylabel("Count")
    ax.set_title("Order Status")
    ax.bar_label(bars)
    fig.tight_layout()
    n.figure(fig, "order_status.png", caption="Order count by status")
    plt.close(fig)

# --- Monthly Revenue Trend ---
with n.section("Monthly Revenue Trend"):
    df = analyst.sql("""
        SELECT strftime(date, '%Y-%m') AS month, COUNT(*) AS orders,
               SUM(amount) AS revenue, ROUND(AVG(amount), 2) AS avg_order
        FROM orders WHERE status = 'completed'
        GROUP BY 1 ORDER BY 1
    """).df()

    fig, ax1 = plt.subplots(figsize=(8, 4))
    ax1.bar(df["month"], df["orders"], color="#EF553B", alpha=0.5, label="Orders")
    ax1.set_ylabel("Orders")
    ax2 = ax1.twinx()
    ax2.plot(df["month"], df["revenue"], color="#636EFA", marker="o", linewidth=2, label="Revenue")
    ax2.set_ylabel("Revenue ($)")
    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, loc="upper left")
    ax1.set_title("Monthly Revenue & Orders")
    fig.tight_layout()
    n.figure(fig, "monthly_trend.png", caption="Monthly revenue and order trend")
    plt.close(fig)

# --- Top 5 Customers ---
with n.section("Top 5 Customers by Revenue"):
    result = analyst.sql("""
        SELECT c.name, c.segment, SUM(o.amount) AS total_revenue, COUNT(*) AS orders
        FROM orders o JOIN customers c ON o.customer_id = c.id
        WHERE o.status = 'completed'
        GROUP BY 1, 2 ORDER BY total_revenue DESC LIMIT 5
    """)
    n.table(result.df(), name="Top Customers")

# --- Regional Performance ---
with n.section("Regional Performance"):
    df = analyst.sql("""
        SELECT c.region, COUNT(DISTINCT c.id) AS customers,
               COUNT(o.id) AS orders, SUM(o.amount) AS revenue
        FROM orders o JOIN customers c ON o.customer_id = c.id
        WHERE o.status = 'completed'
        GROUP BY 1 ORDER BY revenue DESC
    """).df()

    fig, ax = plt.subplots(figsize=(6, 4))
    bars = ax.bar(df["region"], df["revenue"], color="#AB63FA")
    ax.bar_label(bars, labels=[f"{o} orders" for o in df["orders"]], padding=3)
    ax.set_ylabel("Revenue ($)")
    ax.set_title("Revenue by Region")
    fig.tight_layout()
    n.figure(fig, "regional_performance.png", caption="Revenue by region")
    plt.close(fig)

n.save()
print("Dashboard saved to dashboard.md")
