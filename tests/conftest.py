"""Shared pytest fixtures for dashboardmd test suite."""

from __future__ import annotations

from pathlib import Path

import pytest


# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

SAMPLES_DIR = Path(__file__).parent / "samples"


@pytest.fixture
def samples_dir() -> Path:
    """Path to the tests/samples/ directory containing CSV fixtures."""
    return SAMPLES_DIR


@pytest.fixture
def orders_csv(samples_dir: Path) -> Path:
    """Path to orders.csv sample data."""
    return samples_dir / "orders.csv"


@pytest.fixture
def customers_csv(samples_dir: Path) -> Path:
    """Path to customers.csv sample data."""
    return samples_dir / "customers.csv"


@pytest.fixture
def products_csv(samples_dir: Path) -> Path:
    """Path to products.csv sample data."""
    return samples_dir / "products.csv"


@pytest.fixture
def tmp_output_dir(tmp_path: Path) -> Path:
    """Temporary directory for dashboard output."""
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    return output_dir


@pytest.fixture
def tmp_parquet(tmp_path: Path, orders_csv: Path) -> Path:
    """Create a temporary parquet file from orders.csv for testing.

    Requires: duckdb
    """
    duckdb = pytest.importorskip("duckdb")
    parquet_path = tmp_path / "orders.parquet"
    conn = duckdb.connect()
    conn.execute(
        f"COPY (SELECT * FROM read_csv_auto('{orders_csv}')) TO '{parquet_path}' (FORMAT PARQUET)"
    )
    conn.close()
    return parquet_path


@pytest.fixture
def tmp_json(tmp_path: Path, orders_csv: Path) -> Path:
    """Create a temporary JSON file from orders.csv for testing.

    Requires: duckdb
    """
    duckdb = pytest.importorskip("duckdb")
    json_path = tmp_path / "orders.json"
    conn = duckdb.connect()
    conn.execute(
        f"COPY (SELECT * FROM read_csv_auto('{orders_csv}')) TO '{json_path}' (FORMAT JSON, ARRAY true)"
    )
    conn.close()
    return json_path


@pytest.fixture
def duckdb_conn():
    """Provide a fresh in-memory DuckDB connection, closed after test."""
    duckdb = pytest.importorskip("duckdb")
    conn = duckdb.connect()
    yield conn
    conn.close()


@pytest.fixture
def tmp_duckdb(tmp_path: Path, orders_csv: Path) -> Path:
    """Create a temporary .duckdb file with an orders table.

    Requires: duckdb
    """
    duckdb = pytest.importorskip("duckdb")
    db_path = tmp_path / "test.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute(
        f"CREATE TABLE orders AS SELECT * FROM read_csv_auto('{orders_csv}')"
    )
    conn.close()
    return db_path


@pytest.fixture
def sample_dataframe(orders_csv: Path):
    """Return orders data as a pandas DataFrame.

    Requires: pandas
    """
    pd = pytest.importorskip("pandas")
    return pd.read_csv(orders_csv)
