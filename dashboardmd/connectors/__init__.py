"""Built-in connectors: BI platforms, APIs, and utility connectors.

BI platform connectors import/export data models from Metabase, Looker,
Cube, and PowerBI. They implement the same Connector interface as data
connectors (GitHub, Stripe, etc.), so everything composes.

    from dashboardmd.connectors import MetabaseConnector, LookMLConnector

    analyst = Analyst()
    analyst.use(MetabaseConnector(metadata_dict))
    analyst.use(MyDataConnector(...))  # composes freely
"""

from dashboardmd.connectors.cube import CubeConnector
from dashboardmd.connectors.lookml import LookMLConnector
from dashboardmd.connectors.metabase import MetabaseConnector
from dashboardmd.connectors.powerbi import PowerBIConnector

__all__ = [
    "CubeConnector",
    "LookMLConnector",
    "MetabaseConnector",
    "PowerBIConnector",
]
