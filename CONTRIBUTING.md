# Contributing to dashboardmd

Thank you for your interest in contributing to dashboardmd! This guide will help you get started.

## Development Setup

### Prerequisites

- Python 3.11 or higher
- Git

### Getting Started

1. **Fork and clone the repository:**

   ```bash
   git clone https://github.com/<your-username>/dashboardmd.git
   cd dashboardmd
   ```

2. **Create a virtual environment:**

   ```bash
   python -m venv .venv
   source .venv/bin/activate  # Linux/macOS
   # or .venv\Scripts\activate on Windows
   ```

3. **Install in development mode:**

   ```bash
   pip install -e ".[all,dev]"
   pip install ruff mypy
   ```

4. **Verify your setup:**

   ```bash
   make test
   make lint
   ```

## Development Workflow

### Branch Naming

- `feat/description` — new features
- `fix/description` — bug fixes
- `docs/description` — documentation changes
- `refactor/description` — code refactoring

### Making Changes

1. Create a branch from `main`
2. Make your changes
3. Add or update tests as needed
4. Run the full quality check: `make check`
5. Commit with a clear message
6. Open a pull request

### Running Tests

```bash
# Run all tests
make test

# Run only unit tests
pytest tests/unit/ -v

# Run only integration tests
pytest tests/integration/ -v
```

### Code Quality

```bash
# Run linter
make lint

# Auto-fix lint issues
make fix

# Type checking
make typecheck

# Run everything
make check
```

## Architecture

dashboardmd mirrors the universal BI data model:

- **`model.py`** — `Entity`, `Dimension`, `Measure`, `Relationship` dataclasses
- **`dashboard.py`** — `Dashboard` class: tiles, filters, sections, save()
- **`query.py`** — Query builder: resolve joins, generate SQL/pandas
- **`engine.py`** — Execution engine: run queries against data sources
- **`interop/`** — Platform connectors (Metabase, Looker, Cube, PowerBI)

### Key Principles

- **BI-compatible data model.** Concepts must map 1:1 to Metabase, Looker, PowerBI, and Cube.
- **Markdown output via notebookmd.** All rendering goes through notebookmd widgets.
- **Agent-friendly API.** The API should be easy for AI agents to use programmatically.

## Pull Request Guidelines

- Keep PRs focused — one feature or fix per PR
- Include tests for new functionality
- Update documentation if adding public API methods
- Ensure all CI checks pass before requesting review
- Write a clear PR description explaining *what* and *why*

## Reporting Issues

- Use the [bug report template](https://github.com/minhlucvan/dashboardmd/issues/new?template=bug_report.yml) for bugs
- Use the [feature request template](https://github.com/minhlucvan/dashboardmd/issues/new?template=feature_request.yml) for ideas
- Check existing issues before creating a new one

## Code Style

- **Formatter:** Ruff (line length 120)
- **Linter:** Ruff
- **Type checker:** mypy with `disallow_untyped_defs`
- **Docstrings:** Required on all public methods
- **Type hints:** Required everywhere

## License

By contributing to dashboardmd, you agree that your contributions will be licensed under the [MIT License](LICENSE).
