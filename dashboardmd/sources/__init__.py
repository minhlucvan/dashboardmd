"""Data source handlers — pluggable connectors that register data into DuckDB.

Usage:
    Source.csv("data/orders.csv")
    Source.parquet("data/events.parquet")
    Source.json("data/logs.json")
    Source.postgres("postgresql://host/db", table="orders")
    Source.mysql("mysql://host/db", table="orders")
    Source.duckdb("analytics.duckdb", table="orders")
    Source.dataframe(df)
    Source.sql("SELECT * FROM read_csv('data/orders.csv')")
"""

from __future__ import annotations

from typing import Any

from dashboardmd.sources.base import SourceHandler
from dashboardmd.sources.database import DuckDBSource, MySQLSource, PostgresSource, SQLiteSource
from dashboardmd.sources.dataframe import DataFrameSource
from dashboardmd.sources.file import CSVSource, JSONSource, ParquetSource
from dashboardmd.sources.sql import RawSQLSource


class Source:
    """Factory for creating source handlers."""

    @staticmethod
    def csv(path: str) -> CSVSource:
        """Create a CSV file source."""
        return CSVSource(path=path)

    @staticmethod
    def parquet(path: str) -> ParquetSource:
        """Create a Parquet file source."""
        return ParquetSource(path=path)

    @staticmethod
    def json(path: str) -> JSONSource:
        """Create a JSON file source."""
        return JSONSource(path=path)

    @staticmethod
    def postgres(dsn: str, table: str, schema: str = "public") -> PostgresSource:
        """Create a PostgreSQL source."""
        return PostgresSource(dsn=dsn, table=table, schema=schema)

    @staticmethod
    def mysql(dsn: str, table: str) -> MySQLSource:
        """Create a MySQL source."""
        return MySQLSource(dsn=dsn, table=table)

    @staticmethod
    def sqlite(path: str, table: str) -> SQLiteSource:
        """Create a SQLite source."""
        return SQLiteSource(path=path, table=table)

    @staticmethod
    def duckdb(path: str, table: str) -> DuckDBSource:
        """Create a DuckDB file source."""
        return DuckDBSource(path=path, table=table)

    @staticmethod
    def dataframe(df: Any) -> DataFrameSource:
        """Create a DataFrame source (pandas or polars)."""
        return DataFrameSource(df=df)

    @staticmethod
    def sql(sql: str) -> RawSQLSource:
        """Create a raw SQL source."""
        return RawSQLSource(sql=sql)


__all__ = [
    "Source",
    "SourceHandler",
    "CSVSource",
    "ParquetSource",
    "JSONSource",
    "PostgresSource",
    "MySQLSource",
    "SQLiteSource",
    "DuckDBSource",
    "DataFrameSource",
    "RawSQLSource",
]
