# Unit Tests

Tests for individual layers in isolation. Each test file corresponds to one module.

## Test Files by Layer

| Layer | File | Module Under Test |
|-------|------|------------------|
| Source | `sources/test_base.py` | `dashboardmd.sources.base` |
| Source | `sources/test_file.py` | `dashboardmd.sources.file` |
| Source | `sources/test_dataframe.py` | `dashboardmd.sources.dataframe` |
| Source | `sources/test_database.py` | `dashboardmd.sources.database` |
| Source | `sources/test_sql.py` | `dashboardmd.sources.sql` |
| Semantic | `test_model.py` | `dashboardmd.model` |
| Query | `test_query.py` | `dashboardmd.query` |
| Query | `test_engine.py` | `dashboardmd.engine` |
| Query | `test_time.py` | `dashboardmd.time` |
| Render | `test_dashboard.py` | `dashboardmd.dashboard` |
| Other | `test_suggest.py` | `dashboardmd.suggest` |
| Other | `test_refresh.py` | `dashboardmd.refresh` |
| Other | `test_import.py` | `dashboardmd` (package) |

## Running

```bash
pytest tests/unit/ -v
```
