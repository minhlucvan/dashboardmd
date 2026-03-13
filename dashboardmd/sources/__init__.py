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
