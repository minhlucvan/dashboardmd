"""dashboardmd — Code-first analytics dashboard platform.

Same data model as Metabase, Looker, PowerBI, Cube — but in Python,
for agents, outputting Markdown.
"""

__version__ = "0.1.0"

from dashboardmd.analyst import Analyst, QueryResult
from dashboardmd.model import Dimension, Entity, Measure, Relationship
from dashboardmd.sources import Source

__all__ = [
    "Analyst",
    "Dimension",
    "Entity",
    "Measure",
    "QueryResult",
    "Relationship",
    "Source",
]
