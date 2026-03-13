"""Web Traffic Dashboard.

Analyzes website pageviews, traffic sources, and conversion funnel.
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
analyst.add("pageviews", "data/pageviews.csv")
analyst.add("conversions", "data/conversions.csv")

# ---------------------------------------------------------------------------
# Build report with notebookmd + plotly
# ---------------------------------------------------------------------------

n = nb("dashboard.md", title="Web Traffic Dashboard")

# --- Traffic Overview ---
with n.section("Traffic Overview"):
    kpis = analyst.sql("""
        SELECT
            COUNT(*) AS views,
            COUNT(DISTINCT session_id) AS sessions,
            COUNT(DISTINCT user_id) AS visitors,
            ROUND(AVG(duration_sec), 0) AS avg_duration
        FROM pageviews
    """).fetchone()

    n.metric_row([
        {"label": "Pageviews", "value": kpis[0]},
        {"label": "Sessions", "value": kpis[1]},
        {"label": "Unique Visitors", "value": kpis[2]},
        {"label": "Avg Duration (s)", "value": kpis[3]},
    ])

# --- Traffic by Source ---
with n.section("Traffic by Source"):
    rows = analyst.sql("""
        SELECT source, COUNT(*) AS views FROM pageviews GROUP BY 1 ORDER BY views DESC
    """).fetchall()

    fig = px.bar(
        x=[r[0] for r in rows], y=[r[1] for r in rows],
        labels={"x": "Source", "y": "Pageviews"},
        color_discrete_sequence=["#636EFA"],
    )
    fig.update_layout(showlegend=False)
    n.plotly_chart(fig, filename="traffic_by_source.html", caption="Pageviews by traffic source")

# --- Traffic by Device ---
with n.section("Traffic by Device"):
    rows = analyst.sql("""
        SELECT device, COUNT(*) AS views FROM pageviews GROUP BY 1 ORDER BY views DESC
    """).fetchall()

    fig = px.pie(
        names=[r[0] for r in rows], values=[r[1] for r in rows],
        color_discrete_sequence=px.colors.qualitative.Set2,
    )
    n.plotly_chart(fig, filename="traffic_by_device.html", caption="Device distribution")

# --- Top Pages ---
with n.section("Top Pages"):
    rows = analyst.sql("""
        SELECT page, COUNT(*) AS views, COUNT(DISTINCT session_id) AS sessions,
               ROUND(AVG(duration_sec), 1) AS avg_duration_sec
        FROM pageviews GROUP BY 1 ORDER BY views DESC
    """).fetchall()

    fig = px.bar(
        x=[r[1] for r in rows], y=[r[0] for r in rows],
        orientation="h", text=[f"{r[3]}s avg" for r in rows],
        labels={"x": "Pageviews", "y": "Page"},
        color_discrete_sequence=["#AB63FA"],
    )
    fig.update_traces(textposition="outside")
    fig.update_layout(yaxis={"categoryorder": "total ascending"}, showlegend=False)
    n.plotly_chart(fig, filename="top_pages.html", caption="Pages ranked by views with average duration")

# --- Conversion Funnel ---
with n.section("Conversion Funnel"):
    funnel = analyst.sql("""
        WITH sessions AS (
            SELECT COUNT(DISTINCT session_id) AS v FROM pageviews
        ),
        pricing AS (
            SELECT COUNT(DISTINCT session_id) AS v FROM pageviews WHERE page = '/pricing'
        ),
        signup AS (
            SELECT COUNT(DISTINCT session_id) AS v FROM pageviews WHERE page = '/signup'
        ),
        trials AS (
            SELECT COUNT(*) AS v FROM conversions WHERE event = 'trial_signup'
        ),
        purchases AS (
            SELECT COUNT(*) AS v FROM conversions WHERE event = 'purchase'
        )
        SELECT s.v, p.v, su.v, t.v, pu.v
        FROM sessions s, pricing p, signup su, trials t, purchases pu
    """).fetchone()

    stages = ["All Sessions", "Viewed Pricing", "Viewed Signup", "Trial Signup", "Purchase"]
    values = list(funnel)

    fig = go.Figure(go.Funnel(
        y=stages, x=values,
        textinfo="value+percent initial",
        marker={"color": ["#636EFA", "#EF553B", "#00CC96", "#AB63FA", "#FFA15A"]},
    ))
    fig.update_layout(showlegend=False)
    n.plotly_chart(fig, filename="conversion_funnel.html", caption="Conversion funnel from session to purchase")

# --- Conversions by Source ---
with n.section("Conversions by Source"):
    rows = analyst.sql("""
        SELECT p.source, COUNT(DISTINCT p.session_id) AS sessions,
               COUNT(DISTINCT c.id) AS conversions,
               ROUND(COUNT(DISTINCT c.id) * 100.0 / COUNT(DISTINCT p.session_id), 1) AS conv_rate,
               COALESCE(SUM(c.value), 0) AS revenue
        FROM pageviews p
        LEFT JOIN conversions c ON p.session_id = c.session_id
        GROUP BY 1 ORDER BY conversions DESC
    """).fetchall()

    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=[r[0] for r in rows], y=[r[1] for r in rows],
        name="Sessions", marker_color="#C8C8C8",
    ))
    fig.add_trace(go.Bar(
        x=[r[0] for r in rows], y=[r[2] for r in rows],
        name="Conversions", marker_color="#00CC96",
        text=[f"{r[3]}%" for r in rows], textposition="outside",
    ))
    fig.update_layout(
        barmode="group", yaxis_title="Count",
        legend={"orientation": "h", "yanchor": "bottom", "y": 1.02},
    )
    n.plotly_chart(fig, filename="conversions_by_source.html", caption="Sessions vs conversions by source")

# --- Daily Traffic Trend ---
with n.section("Daily Traffic Trend"):
    rows = analyst.sql("""
        SELECT CAST(timestamp AS DATE) AS date,
               COUNT(*) AS pageviews, COUNT(DISTINCT session_id) AS sessions,
               COUNT(DISTINCT user_id) AS visitors
        FROM pageviews GROUP BY 1 ORDER BY 1
    """).fetchall()

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=[str(r[0]) for r in rows], y=[r[1] for r in rows],
        mode="lines+markers", name="Pageviews", line={"width": 3},
    ))
    fig.add_trace(go.Scatter(
        x=[str(r[0]) for r in rows], y=[r[2] for r in rows],
        mode="lines+markers", name="Sessions",
    ))
    fig.add_trace(go.Scatter(
        x=[str(r[0]) for r in rows], y=[r[3] for r in rows],
        mode="lines+markers", name="Visitors",
    ))
    fig.update_layout(
        xaxis_title="Date", yaxis_title="Count",
        legend={"orientation": "h", "yanchor": "bottom", "y": 1.02},
    )
    n.plotly_chart(fig, filename="daily_trend.html", caption="Daily traffic trend")

n.save()
print("Dashboard saved to dashboard.md")
