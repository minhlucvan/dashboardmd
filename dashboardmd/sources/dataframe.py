"""DataFrame source handler: pandas and polars."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from dashboardmd.sources.base import SourceHandler

if TYPE_CHECKING:
    import duckdb
    import pandas as pd


@dataclass
class DataFrameSource(SourceHandler):
    """Register a pandas or polars DataFrame in DuckDB."""

    df: Any = field(repr=False)

    def register(self, conn: duckdb.DuckDBPyConnection, table_name: str) -> None:
        # DuckDB can query DataFrames registered by name
        conn.register(table_name, self.df)

    def describe(self) -> dict[str, Any]:
        cols: list[tuple[str, str]] = []
        if hasattr(self.df, "dtypes"):
            # pandas
            for col_name, dtype in self.df.dtypes.items():
                cols.append((str(col_name), str(dtype)))
        return {"columns": cols}
