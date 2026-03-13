"""Web Traffic Dashboard.

Analyzes website pageviews, traffic sources, and conversion funnel.
Demonstrates combining semantic entities with raw SQL tiles in a Dashboard.
"""

from dashboardmd import Dashboard, Dimension, Entity, Measure, Relationship

# ---------------------------------------------------------------------------
# Define entities
# ---------------------------------------------------------------------------

pageviews = Entity(
    "pageviews",
    source="data/pageviews.csv",
    dimensions=[
        Dimension("id", type="number", primary_key=True),
        Dimension("timestamp", type="time"),
        Dimension("page", type="string"),
        Dimension("source", type="string"),
        Dimension("device", type="string"),
        Dimension("session_id", type="string"),
    ],
    measures=[
        Measure("views", type="count"),
        Measure("unique_sessions", type="count_distinct", sql="session_id"),
        Measure("unique_visitors", type="count_distinct", sql="user_id"),
        Measure("avg_duration", type="avg", sql="duration_sec"),
    ],
)

conversions = Entity(
    "conversions",
    source="data/conversions.csv",
    dimensions=[
        Dimension("id", type="number", primary_key=True),
        Dimension("timestamp", type="time"),
        Dimension("session_id", type="string"),
        Dimension("event", type="string"),
    ],
    measures=[
        Measure("conversion_count", type="count"),
        Measure("total_value", type="sum", sql="value"),
    ],
)

relationships = [
    Relationship("conversions", "pageviews", on=("session_id", "session_id"), type="many_to_one"),
]

# ---------------------------------------------------------------------------
# Build dashboard
# ---------------------------------------------------------------------------

dash = Dashboard(
    title="Web Traffic Dashboard",
    entities=[pageviews, conversions],
    relationships=relationships,
    output="dashboard.md",
)

# Traffic overview
dash.section("Traffic Overview")
dash.tile("pageviews.views")
dash.tile("pageviews.unique_sessions")
dash.tile("pageviews.unique_visitors")
dash.tile("pageviews.avg_duration")

# By source
dash.section("Traffic by Source")
dash.tile("pageviews.views", by="pageviews.source", type="bar_chart")

# By device
dash.section("Traffic by Device")
dash.tile("pageviews.views", by="pageviews.device")

# Top pages
dash.section("Top Pages")
dash.tile_sql(
    "Page Views",
    """
    SELECT
        page,
        COUNT(*) AS views,
        COUNT(DISTINCT session_id) AS sessions,
        ROUND(AVG(duration_sec), 1) AS avg_duration_sec
    FROM pageviews
    GROUP BY 1
    ORDER BY views DESC
    """,
)

# Conversion funnel
dash.section("Conversion Funnel")
dash.tile_sql(
    "Funnel",
    """
    WITH sessions AS (
        SELECT COUNT(DISTINCT session_id) AS total_sessions FROM pageviews
    ),
    pricing_sessions AS (
        SELECT COUNT(DISTINCT session_id) AS pricing_views FROM pageviews WHERE page = '/pricing'
    ),
    signup_sessions AS (
        SELECT COUNT(DISTINCT session_id) AS signup_views FROM pageviews WHERE page = '/signup'
    ),
    trial_signups AS (
        SELECT COUNT(*) AS trials FROM conversions WHERE event = 'trial_signup'
    ),
    purchases AS (
        SELECT COUNT(*) AS purchases FROM conversions WHERE event = 'purchase'
    )
    SELECT
        s.total_sessions AS "Total Sessions",
        p.pricing_views AS "Viewed Pricing",
        su.signup_views AS "Viewed Signup",
        t.trials AS "Trial Signups",
        pu.purchases AS "Purchases"
    FROM sessions s, pricing_sessions p, signup_sessions su, trial_signups t, purchases pu
    """,
)

# Source attribution
dash.section("Conversions by Source")
dash.tile_sql(
    "Source Attribution",
    """
    SELECT
        p.source,
        COUNT(DISTINCT p.session_id) AS sessions,
        COUNT(DISTINCT c.id) AS conversions,
        ROUND(COUNT(DISTINCT c.id) * 100.0 / COUNT(DISTINCT p.session_id), 1) AS conversion_rate_pct,
        COALESCE(SUM(c.value), 0) AS revenue
    FROM pageviews p
    LEFT JOIN conversions c ON p.session_id = c.session_id
    GROUP BY 1
    ORDER BY conversions DESC
    """,
)

# Daily trend
dash.section("Daily Traffic Trend")
dash.tile_sql(
    "Daily Trend",
    """
    SELECT
        CAST(timestamp AS DATE) AS date,
        COUNT(*) AS pageviews,
        COUNT(DISTINCT session_id) AS sessions,
        COUNT(DISTINCT user_id) AS visitors
    FROM pageviews
    GROUP BY 1
    ORDER BY 1
    """,
)

# Generate the dashboard
dash.save()
print(f"Dashboard saved to {dash.output}")
