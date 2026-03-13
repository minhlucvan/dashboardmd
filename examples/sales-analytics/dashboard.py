"""Sales Analytics Dashboard.

Analyzes order data across customers, products, and time periods.
Uses Analyst for queries and notebookmd with Plotly for rich visualizations.
"""

import plotly.express as px
import plotly.graph_objects as go
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
# Build report with notebookmd + plotly
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
    rows = analyst.sql("""
        SELECT c.segment, SUM(o.amount) AS revenue
        FROM orders o
        JOIN customers c ON o.customer_id = c.id
        WHERE o.status = 'completed'
        GROUP BY 1 ORDER BY revenue DESC
    """).fetchall()

    fig = px.bar(
        x=[r[0] for r in rows], y=[r[1] for r in rows],
        labels={"x": "Segment", "y": "Revenue ($)"},
        color_discrete_sequence=["#636EFA"],
    )
    fig.update_layout(showlegend=False)
    n.plotly_chart(fig, filename="revenue_by_segment.html", caption="Revenue by customer segment")

# --- Revenue by Product Category ---
with n.section("Revenue by Product Category"):
    rows = analyst.sql("""
        SELECT p.category, SUM(o.amount) AS revenue
        FROM orders o
        JOIN products p ON o.product_id = p.id
        WHERE o.status = 'completed'
        GROUP BY 1 ORDER BY revenue DESC
    """).fetchall()

    fig = px.pie(
        names=[r[0] for r in rows], values=[r[1] for r in rows],
        color_discrete_sequence=px.colors.qualitative.Set2,
    )
    n.plotly_chart(fig, filename="revenue_by_category.html", caption="Revenue share by product category")

# --- Order Status ---
with n.section("Order Status"):
    rows = analyst.sql("""
        SELECT status, COUNT(*) AS count FROM orders GROUP BY 1 ORDER BY count DESC
    """).fetchall()

    fig = px.bar(
        x=[r[0] for r in rows], y=[r[1] for r in rows],
        labels={"x": "Status", "y": "Count"},
        color=[r[0] for r in rows],
        color_discrete_map={"completed": "#2ecc71", "pending": "#f39c12", "cancelled": "#e74c3c"},
    )
    fig.update_layout(showlegend=False)
    n.plotly_chart(fig, filename="order_status.html", caption="Order count by status")

# --- Monthly Revenue Trend ---
with n.section("Monthly Revenue Trend"):
    rows = analyst.sql("""
        SELECT
            strftime(date, '%Y-%m') AS month,
            COUNT(*) AS orders,
            SUM(amount) AS revenue,
            ROUND(AVG(amount), 2) AS avg_order
        FROM orders
        WHERE status = 'completed'
        GROUP BY 1 ORDER BY 1
    """).fetchall()

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=[r[0] for r in rows], y=[r[2] for r in rows],
        mode="lines+markers", name="Revenue",
        line={"color": "#636EFA", "width": 3},
    ))
    fig.add_trace(go.Bar(
        x=[r[0] for r in rows], y=[r[1] for r in rows],
        name="Orders", yaxis="y2", marker_color="#EF553B", opacity=0.5,
    ))
    fig.update_layout(
        yaxis={"title": "Revenue ($)"},
        yaxis2={"title": "Orders", "overlaying": "y", "side": "right"},
        legend={"orientation": "h", "yanchor": "bottom", "y": 1.02},
    )
    n.plotly_chart(fig, filename="monthly_trend.html", caption="Monthly revenue and order trend")

# --- Top 5 Customers ---
with n.section("Top 5 Customers by Revenue"):
    result = analyst.sql("""
        SELECT c.name, c.segment, SUM(o.amount) AS total_revenue, COUNT(*) AS orders
        FROM orders o
        JOIN customers c ON o.customer_id = c.id
        WHERE o.status = 'completed'
        GROUP BY 1, 2
        ORDER BY total_revenue DESC
        LIMIT 5
    """)
    n.table(result.df(), name="Top Customers")

# --- Regional Performance ---
with n.section("Regional Performance"):
    rows = analyst.sql("""
        SELECT c.region, COUNT(DISTINCT c.id) AS customers,
               COUNT(o.id) AS orders, SUM(o.amount) AS revenue
        FROM orders o
        JOIN customers c ON o.customer_id = c.id
        WHERE o.status = 'completed'
        GROUP BY 1 ORDER BY revenue DESC
    """).fetchall()

    fig = px.bar(
        x=[r[0] for r in rows], y=[r[3] for r in rows],
        text=[f"{r[2]} orders" for r in rows],
        labels={"x": "Region", "y": "Revenue ($)"},
        color_discrete_sequence=["#AB63FA"],
    )
    fig.update_traces(textposition="outside")
    fig.update_layout(showlegend=False)
    n.plotly_chart(fig, filename="regional_performance.html", caption="Revenue by region")

n.save()
print("Dashboard saved to dashboard.md")
