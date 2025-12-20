# CLAUDE.md

## Project Overview

Daffy is a DataFrame validation library providing decorators (`@df_in`, `@df_out`, `@df_log`) for Pandas, Polars, Modin, and PyArrow. It validates column names, data types, and row values through Pydantic.

## Commands

```bash
uv run pytest                    # Run tests
uv run pytest --cov              # Run with coverage
uv run ruff format               # Format code
uv run ruff check --fix          # Lint and fix
uv run pyrefly check .           # Type check
```

For testing with isolated optional dependencies, see `TESTING_OPTIONAL_DEPS.md`.

## Key Constraints

- Python 3.9+ compatibility
- DataFrame libraries are optional - only narwhals is required
- 100% test coverage
- Avoid obvious comments - use good names instead

## Workflow

1. Write tests first
2. Small, focused commits
3. Update CHANGELOG.md for changes
4. Update docs if public API changes
