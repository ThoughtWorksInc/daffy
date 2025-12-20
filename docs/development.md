# Development Guide

## Setup

To set up the development environment, clone the repository and install dependencies with [uv](https://github.com/astral-sh/uv):

```sh
git clone https://github.com/ThoughtWorksInc/daffy.git
cd daffy
uv sync --group test --group dev
```

## Code Quality

### Pre-commit Hooks

To enable linting on each commit, run:

```sh
pre-commit install
```

This will run `ruff format` and `ruff check` on every commit.

### Linting and Formatting

```sh
# Format code
uv run ruff format

# Run linter
uv run ruff check

# Run linter with auto-fix
uv run ruff check --fix
```

### Type Checking

```sh
uv run pyrefly check .
```

## Testing

The project uses pytest for testing. Tests are parametrized to run against multiple DataFrame backends (Pandas, Polars, Modin, and PyArrow).

### Test Files

- `test_df_in.py`: Tests for the `@df_in` decorator
- `test_df_out.py`: Tests for the `@df_out` decorator
- `test_df_log.py`: Tests for the `@df_log` decorator
- `test_decorators.py`: Tests for decorator combinations
- `test_config.py`: Tests for configuration functionality
- `test_row_validation.py`: Tests for Pydantic row validation
- `test_type_compatibility.py`: Tests for type hint compatibility

### Running Tests

```sh
# Run all tests
uv run pytest

# Run tests with coverage
uv run pytest --cov --cov-report=html

# Run specific test file
uv run pytest tests/test_df_in.py

# Run tests matching a pattern
uv run pytest -k "test_missing_columns"
```

### Coverage Requirements

The project requires a minimum of **95% test coverage**. Coverage is enforced in CI.

## Optional Dependencies

Daffy supports pandas-only, polars-only, or both. See `TESTING_OPTIONAL_DEPS.md` for details on testing optional dependency scenarios.

## Contributing

1. Create a feature branch from `master`
2. Write tests for new functionality (test-first is encouraged)
3. Ensure all tests pass: `uv run pytest`
4. Ensure code quality: `uv run ruff format && uv run ruff check`
5. Update documentation as necessary
6. Update `CHANGELOG.md` with your changes
