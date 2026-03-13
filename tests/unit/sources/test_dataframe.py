"""Tests for DataFrame source handler."""

from __future__ import annotations

import pytest


class TestDataFrameSource:
    """DataFrameSource registers a pandas/polars DataFrame in DuckDB."""

    @pytest.mark.requires_pandas
    def test_register_pandas_dataframe(self, duckdb_conn, sample_dataframe) -> None:
        """A pandas DataFrame should be queryable after registration."""
        from dashboardmd.sources.dataframe import DataFrameSource

        source = DataFrameSource(df=sample_dataframe)
        source.register(duckdb_conn, "orders")

        result = duckdb_conn.execute("SELECT count(*) FROM orders").fetchone()
        assert result is not None
        assert result[0] == 20

    @pytest.mark.requires_pandas
    def test_dataframe_column_types(self, duckdb_conn, sample_dataframe) -> None:
        """DataFrame column types should be preserved through DuckDB."""
        from dashboardmd.sources.dataframe import DataFrameSource

        source = DataFrameSource(df=sample_dataframe)
        source.register(duckdb_conn, "orders")

        # amount should be numeric and aggregatable
        result = duckdb_conn.execute("SELECT SUM(amount) FROM orders").fetchone()
        assert result is not None
        assert result[0] > 0

    @pytest.mark.requires_pandas
    def test_empty_dataframe(self, duckdb_conn) -> None:
        """An empty DataFrame should register with zero rows."""
        pd = pytest.importorskip("pandas")
        from dashboardmd.sources.dataframe import DataFrameSource

        empty_df = pd.DataFrame({"id": [], "name": [], "value": []})
        source = DataFrameSource(df=empty_df)
        source.register(duckdb_conn, "empty")

        result = duckdb_conn.execute("SELECT count(*) FROM empty").fetchone()
        assert result is not None
        assert result[0] == 0

    @pytest.mark.requires_pandas
    def test_dataframe_with_mixed_types(self, duckdb_conn) -> None:
        """DataFrame with int, float, string, datetime columns should register correctly."""
        pd = pytest.importorskip("pandas")
        from dashboardmd.sources.dataframe import DataFrameSource

        df = pd.DataFrame({
            "id": [1, 2, 3],
            "value": [1.5, 2.7, 3.9],
            "name": ["a", "b", "c"],
            "date": pd.to_datetime(["2026-01-01", "2026-01-02", "2026-01-03"]),
        })
        source = DataFrameSource(df=df)
        source.register(duckdb_conn, "mixed")

        result = duckdb_conn.execute("SELECT count(*) FROM mixed").fetchone()
        assert result is not None
        assert result[0] == 3

    @pytest.mark.requires_pandas
    def test_dataframe_aggregation(self, duckdb_conn, sample_dataframe) -> None:
        """Should support GROUP BY queries on DataFrame data."""
        from dashboardmd.sources.dataframe import DataFrameSource

        source = DataFrameSource(df=sample_dataframe)
        source.register(duckdb_conn, "orders")

        result = duckdb_conn.execute(
            "SELECT status, COUNT(*) as cnt FROM orders GROUP BY status"
        ).fetchall()
        assert len(result) >= 2  # at least completed and pending

    @pytest.mark.requires_pandas
    def test_describe_returns_schema(self, duckdb_conn, sample_dataframe) -> None:
        """describe() should return column info from the DataFrame."""
        from dashboardmd.sources.dataframe import DataFrameSource

        source = DataFrameSource(df=sample_dataframe)
        metadata = source.describe()
        assert "columns" in metadata
        col_names = [c[0] for c in metadata["columns"]]
        assert "id" in col_names
        assert "amount" in col_names
