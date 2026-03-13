"""BI platform connectors — import data models from Metabase, Looker, Cube, and PowerBI.

For the full connector experience, use the Connector classes in dashboardmd.connectors:

    from dashboardmd.connectors import MetabaseConnector
    analyst.use(MetabaseConnector(metadata))
"""

from dashboardmd.interop.cube import from_cube
from dashboardmd.interop.lookml import from_lookml
from dashboardmd.interop.metabase import from_metabase
from dashboardmd.interop.powerbi import from_powerbi

__all__ = [
    "from_cube",
    "from_lookml",
    "from_metabase",
    "from_powerbi",
]
