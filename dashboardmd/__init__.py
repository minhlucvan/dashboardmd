"""dashboardmd — Code-first analytics dashboard platform.

Same data model as Metabase, Looker, PowerBI, Cube — but in Python,
for agents, outputting Markdown.

Core concept: Analyst is the foundation. Everything else builds on it.

    # Direct SQL (maximum power)
    analyst = Analyst()
    analyst.add("orders", "data/orders.csv")
    analyst.sql("SELECT status, SUM(amount) FROM orders GROUP BY 1")

    # Semantic queries (BI-style)
    analyst.add_entity(orders_entity)
    analyst.query(measures=["orders.revenue"], dimensions=["orders.status"])

    # Dashboard (structured reports)
    dash = Dashboard(title="Review", entities=[orders], output="report.md")
    dash.tile("orders.revenue", by="orders.status")
    dash.save()
"""

__version__ = "0.1.0"

from dashboardmd.analyst import Analyst, QueryResult
from dashboardmd.dashboard import Dashboard
from dashboardmd.model import Dimension, Entity, Measure, Relationship
from dashboardmd.sources import Source

__all__ = [
    "Analyst",
    "Dashboard",
    "Dimension",
    "Entity",
    "Measure",
    "QueryResult",
    "Relationship",
    "Source",
]
