# Source Layer Tests

Tests for `dashboardmd.sources` — the pluggable data source handlers.

## Architecture

Each handler implements `SourceHandler.register(conn, table_name)` to make data
available as a DuckDB view. Tests verify that registration works and data is queryable.

## Test Files

| File | Handler | What's Tested |
|------|---------|--------------|
| `test_base.py` | `SourceHandler` ABC | Interface contract, cannot instantiate directly |
| `test_file.py` | `CSVSource`, `ParquetSource`, `JSONSource` | File registration, schema detection, missing files |
| `test_dataframe.py` | `DataFrameSource` | pandas/polars DataFrame registration |
| `test_database.py` | `DuckDBSource`, `SQLiteSource` | Local database attachment |
| `test_sql.py` | `RawSQLSource` | Arbitrary SQL as a source |

## Test Scenarios

### Base Handler (`test_base.py`)
- Abstract class cannot be instantiated
- Subclass must implement `register()` and `describe()`
- `register()` makes data queryable via DuckDB
- `describe()` returns column names and types

### File Handlers (`test_file.py`)
- **CSV**: register CSV → query rows → correct count and values
- **CSV**: schema detection (column names, inferred types)
- **CSV**: missing file raises clear error
- **CSV**: empty file registers with zero rows
- **CSV**: file with special characters in path
- **Parquet**: register parquet → query rows → correct data
- **Parquet**: columnar pushdown works (only reads needed columns)
- **JSON**: register JSON → query rows → correct data
- **All**: `describe()` returns accurate column metadata

### DataFrame Handler (`test_dataframe.py`)
- Register pandas DataFrame → query via DuckDB
- DataFrame column types map correctly
- Empty DataFrame registers with zero rows
- DataFrame with mixed types (int, float, string, datetime)
- Polars DataFrame (if polars available)

### Database Handlers (`test_database.py`)
- DuckDB file: attach and query an existing .duckdb file
- SQLite: attach and query an existing .sqlite file
- PostgreSQL/MySQL: skipped unless external DB available (integration tests)

### SQL Handler (`test_sql.py`)
- Raw SQL expression as source
- SQL with WHERE clause filters data
- SQL with JOIN across registered sources
- Invalid SQL raises clear error
