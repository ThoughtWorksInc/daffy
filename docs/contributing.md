# Contributing

We welcome contributions! This guide will help you get set up for development.

## Development Setup

Clone the repository and install dependencies:

```bash
git clone https://github.com/vertti/daffy.git
cd daffy
```

The repo includes a `mise.toml` that sets up [uv](https://github.com/astral-sh/uv). If you don't use [mise](https://mise.jdx.dev/), install uv manually first.

```bash
mise install    # skip if you installed uv manually
uv sync --group test --group dev  # installs Python, creates venv, installs all deps
```

## Code Quality

### Pre-commit Hooks

Enable linting on each commit:

```bash
pre-commit install
```

This runs `ruff format` and `ruff check` automatically.

### Manual Linting

```bash
# Format code
uv run ruff format

# Run linter
uv run ruff check

# Run linter with auto-fix
uv run ruff check --fix
```

### Type Checking

```bash
uv run pyrefly check .
```

## Testing

Daffy uses pytest with tests parametrized across multiple DataFrame backends (pandas, Polars, Modin, PyArrow).

### Running Tests

```bash
# Run all tests
uv run pytest

# Run with coverage
uv run pytest --cov --cov-report=html

# Run specific test file
uv run pytest tests/test_df_in.py

# Run tests matching a pattern
uv run pytest -k "test_missing_columns"
```

### Test Files

| File                         | Description                       |
| ---------------------------- | --------------------------------- |
| `test_df_in.py`              | Tests for `@df_in` decorator      |
| `test_df_out.py`             | Tests for `@df_out` decorator     |
| `test_df_log.py`             | Tests for `@df_log` decorator     |
| `test_decorators.py`         | Tests for decorator combinations  |
| `test_config.py`             | Tests for configuration           |
| `test_row_validation.py`     | Tests for Pydantic row validation |
| `test_type_compatibility.py` | Tests for type hint compatibility |

### Coverage Requirements

The project requires **95% minimum test coverage**, enforced in CI.

## Making Changes

1. **Create a feature branch** from `master`
2. **Write tests first** — test-driven development is encouraged
3. **Run the test suite** — `uv run pytest`
4. **Check code quality** — `uv run ruff format && uv run ruff check`
5. **Update documentation** if changing public API
6. **Update CHANGELOG.md** with your changes
7. **Submit a pull request**

## Testing Different Environments

Daffy is tested across different dependency combinations (pandas-only, Polars-only, both, neither) to ensure it works in various project environments. See `TESTING_OPTIONAL_DEPS.md` for how to run these isolation tests.
