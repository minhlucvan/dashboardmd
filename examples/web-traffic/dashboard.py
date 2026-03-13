"""Web Traffic Dashboard.

Analyzes website pageviews, traffic sources, and conversion funnel.
Uses Analyst for queries and notebookmd with matplotlib for PNG chart output.
"""

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from notebookmd import nb

from dashboardmd import Analyst

# ---------------------------------------------------------------------------
# Set up analyst with data
# ---------------------------------------------------------------------------

analyst = Analyst()
analyst.add("pageviews", "data/pageviews.csv")
analyst.add("conversions", "data/conversions.csv")

# ---------------------------------------------------------------------------
# Build report with notebookmd + matplotlib
# ---------------------------------------------------------------------------

n = nb("dashboard.md", title="Web Traffic Dashboard")

# --- Traffic Overview ---
with n.section("Traffic Overview"):
    kpis = analyst.sql("""
        SELECT COUNT(*) AS views, COUNT(DISTINCT session_id) AS sessions,
               COUNT(DISTINCT user_id) AS visitors, ROUND(AVG(duration_sec), 0) AS avg_duration
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
    df = analyst.sql("""
        SELECT source, COUNT(*) AS views FROM pageviews GROUP BY 1 ORDER BY views DESC
    """).df()

    n.bar_chart(df, x="source", y="views", title="Pageviews by Source")

# --- Traffic by Device ---
with n.section("Traffic by Device"):
    rows = analyst.sql("""
        SELECT device, COUNT(*) AS views FROM pageviews GROUP BY 1 ORDER BY views DESC
    """).fetchall()

    fig, ax = plt.subplots(figsize=(5, 4))
    ax.pie([r[1] for r in rows], labels=[r[0] for r in rows], autopct="%1.0f%%",
           colors=["#66c2a5", "#fc8d62", "#8da0cb"])
    ax.set_title("Device Distribution")
    n.figure(fig, "traffic_by_device.png", caption="Device distribution")
    plt.close(fig)

# --- Top Pages ---
with n.section("Top Pages"):
    df = analyst.sql("""
        SELECT page, COUNT(*) AS views, ROUND(AVG(duration_sec), 1) AS avg_duration_sec
        FROM pageviews GROUP BY 1 ORDER BY views DESC
    """).df()

    fig, ax = plt.subplots(figsize=(7, 4))
    bars = ax.barh(df["page"], df["views"], color="#AB63FA")
    ax.bar_label(bars, labels=[f"{d}s avg" for d in df["avg_duration_sec"]], padding=3, fontsize=8)
    ax.set_xlabel("Pageviews")
    ax.set_title("Top Pages")
    ax.invert_yaxis()
    fig.tight_layout()
    n.figure(fig, "top_pages.png", caption="Pages ranked by views with average duration")
    plt.close(fig)

# --- Conversion Funnel ---
with n.section("Conversion Funnel"):
    funnel = analyst.sql("""
        WITH sessions AS (SELECT COUNT(DISTINCT session_id) AS v FROM pageviews),
             pricing AS (SELECT COUNT(DISTINCT session_id) AS v FROM pageviews WHERE page = '/pricing'),
             signup AS (SELECT COUNT(DISTINCT session_id) AS v FROM pageviews WHERE page = '/signup'),
             trials AS (SELECT COUNT(*) AS v FROM conversions WHERE event = 'trial_signup'),
             purchases AS (SELECT COUNT(*) AS v FROM conversions WHERE event = 'purchase')
        SELECT s.v, p.v, su.v, t.v, pu.v
        FROM sessions s, pricing p, signup su, trials t, purchases pu
    """).fetchone()

    stages = ["All Sessions", "Viewed Pricing", "Viewed Signup", "Trial Signup", "Purchase"]
    values = list(funnel)
    colors = ["#636EFA", "#EF553B", "#00CC96", "#AB63FA", "#FFA15A"]

    fig, ax = plt.subplots(figsize=(7, 4))
    bars = ax.barh(stages[::-1], values[::-1], color=colors[::-1])
    ax.bar_label(bars, labels=[f"{v} ({v * 100 // values[0]}%)" for v in values[::-1]], padding=3)
    ax.set_xlabel("Count")
    ax.set_title("Conversion Funnel")
    fig.tight_layout()
    n.figure(fig, "conversion_funnel.png", caption="Conversion funnel from session to purchase")
    plt.close(fig)

# --- Conversions by Source ---
with n.section("Conversions by Source"):
    df = analyst.sql("""
        SELECT p.source, COUNT(DISTINCT p.session_id) AS sessions,
               COUNT(DISTINCT c.id) AS conversions,
               ROUND(COUNT(DISTINCT c.id) * 100.0 / COUNT(DISTINCT p.session_id), 1) AS conv_rate
        FROM pageviews p
        LEFT JOIN conversions c ON p.session_id = c.session_id
        GROUP BY 1 ORDER BY conversions DESC
    """).df()

    import numpy as np
    x = np.arange(len(df))
    width = 0.35
    fig, ax = plt.subplots(figsize=(7, 4))
    ax.bar(x - width / 2, df["sessions"], width, label="Sessions", color="#C8C8C8")
    bars2 = ax.bar(x + width / 2, df["conversions"], width, label="Conversions", color="#00CC96")
    ax.bar_label(bars2, labels=[f"{r}%" for r in df["conv_rate"]], padding=3, fontsize=8)
    ax.set_xticks(x)
    ax.set_xticklabels(df["source"])
    ax.set_ylabel("Count")
    ax.legend()
    ax.set_title("Sessions vs Conversions by Source")
    fig.tight_layout()
    n.figure(fig, "conversions_by_source.png", caption="Sessions vs conversions by source")
    plt.close(fig)

# --- Daily Traffic Trend ---
with n.section("Daily Traffic Trend"):
    df = analyst.sql("""
        SELECT CAST(timestamp AS DATE) AS date,
               COUNT(*) AS pageviews, COUNT(DISTINCT session_id) AS sessions,
               COUNT(DISTINCT user_id) AS visitors
        FROM pageviews GROUP BY 1 ORDER BY 1
    """).df()
    df["date"] = df["date"].astype(str)

    n.line_chart(df, x="date", y=["pageviews", "sessions", "visitors"], title="Daily Traffic Trend")

n.save()
print("Dashboard saved to dashboard.md")
