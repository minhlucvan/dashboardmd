# Integration Tests

End-to-end tests that verify multiple layers working together.

## Test Files

| File | Scope | What's Tested |
|------|-------|--------------|
| `test_csv_to_dashboard.py` | Source → Semantic → Query → Render | Full pipeline from CSV files to Markdown output |
| `test_multi_source.py` | Source → Engine | Multiple source types registered and joined |
| `test_end_to_end.py` | All layers | Complete user scenarios from the README examples |

## Running

```bash
pytest tests/integration/ -v -m integration
```

## Requirements

- DuckDB must be installed
- No external databases needed (all tests use local files and in-memory DuckDB)
- Tests marked `@pytest.mark.integration`
