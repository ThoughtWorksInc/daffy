# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Daffy is a DataFrame column validator library that provides runtime validation decorators (`@df_in`, `@df_out`, `@df_log`) for Pandas, Polars, Modin, and PyArrow DataFrames. It validates column names (including regex patterns), data types, and enforces strictness rules through simple function decorators.

## Workflow

When working on this repository, follow this structured approach:

### 1. Plan First
- Understand the full scope of the task before starting
- Identify which modules will be affected
- Consider edge cases and testing requirements
- Break the work into logical, incremental steps

### 2. Small Working Commits
- Each commit should be a complete, working unit of functionality
- Break large features into multiple small commits
- Each commit should pass all tests and linting
- Commit messages should be clear and descriptive

### 3. Test-Driven Development Cycle

For each commit, follow this order:

```bash
# 1. Write or update tests first
# Add tests to appropriate tests/test_*.py file

# 2. Run tests to see them fail
uv run pytest tests/test_your_feature.py

# 3. Implement the feature
# Make changes to daffy/*.py files

# 4. Run tests to see them pass
uv run pytest tests/test_your_feature.py

# 5. Run full test suite
uv run pytest

# 6. Run linting and formatting
uv run ruff format
uv run ruff check --fix
uv run pyrefly check .

# 7. Commit the changes
git add .
git commit -m "Descriptive commit message"
```

### 4. Before Creating a PR

Check if documentation needs updating:
- **README.md** - If public API or examples changed
- **docs/usage.md** - If usage patterns or features changed
- **docs/development.md** - If development workflow changed
- **CHANGELOG.md** - Always update with changes (see existing format)
- **Type hints** - Ensure all new functions have proper annotations

### 5. Important: Commit and PR Messages
- **NEVER mention AI tools or assistants** in commit messages, PR descriptions, or code comments
- Write commit messages as if you wrote the code yourself
- Use conventional commit format when appropriate (e.g., "fix:", "feat:", "docs:")
- Focus on what changed and why, not how it was developed

## Development Commands

### Setup
```bash
# Install dependencies using uv (preferred)
uv sync --group test --group dev

# Alternative: install only test dependencies
uv sync --group test
```

### Testing
```bash
# Run all tests
uv run pytest

# Run tests with coverage
uv run pytest --cov --cov-report=html

# Run specific test file
uv run pytest tests/test_df_in.py

# Run tests matching pattern
uv run pytest -k "test_missing_columns"

# Run with verbose output
uv run pytest -v
```

### Linting and Type Checking
```bash
# Run Ruff formatter
uv run ruff format

# Run Ruff linter
uv run ruff check

# Run Ruff linter with auto-fix
uv run ruff check --fix

# Run type checker (Pyrefly)
uv run pyrefly check .
```

### Pre-commit Hooks
```bash
# Install pre-commit hooks (runs ruff format + ruff check on each commit)
pre-commit install
```

### Building
```bash
# Build wheel package
uv build --wheel

# Build both wheel and sdist
uv build
```

### Testing Optional Dependencies

Daffy supports optional dependencies (pandas-only, polars-only, or both). See `TESTING_OPTIONAL_DEPS.md` for details.

```bash
# Build wheel first
uv build --wheel

# Test with pandas only
WHEEL=$(ls dist/daffy-*.whl | head -n1)
uv run --no-project --with "pandas>=1.5.1" --with "$WHEEL" python scripts/test_isolated_deps.py pandas

# Test with polars only
uv run --no-project --with "polars>=1.7.0" --with "$WHEEL" python scripts/test_isolated_deps.py polars

# Test with both libraries
uv run --no-project --with "pandas>=1.5.1" --with "polars>=1.7.0" --with "$WHEEL" python scripts/test_isolated_deps.py both
```

## Architecture

### Core Module Responsibilities

**decorators.py** - Public API and orchestration
- Exports `df_in`, `df_out`, `df_log` decorators
- Orchestrates validation by calling validation.py and utils.py
- Manages configuration precedence (decorator param > config file > default)
- Preserves type information using TypeVar for static type checking

**validation.py** - Core validation logic
- `validate_dataframe()` is the central validation engine
- Supports two modes: list-based (columns only) or dict-based (columns + dtypes)
- Handles regex pattern matching via patterns.py
- Accumulates all validation errors before raising single AssertionError
- Performs strictness checking (no extra columns when strict=True)

**patterns.py** - Regex pattern handling
- Recognizes `r/pattern/` syntax for regex column matching
- Compiles regex patterns and caches them as `RegexColumnDef` tuples
- Provides matching functions used by validation layer
- Example: `"r/Price_[0-9]+/"` matches Price_1, Price_2, etc.

**utils.py** - Cross-cutting utilities
- DataFrame type assertions using `assert_is_dataframe()`
- Parameter extraction from function signatures via `get_parameter()`
- Context formatting for error messages
- DataFrame description for logging
- Logging functions for df_log decorator

**config.py** - Configuration management
- Loads `[tool.daffy]` section from pyproject.toml
- Caches configuration on first access
- Only searches in current working directory (not parent dirs)
- Configuration precedence: decorator parameter > config file > False (default)

**dataframe_types.py** - Optional dependency handling
- Dynamically constructs DataFrame type unions based on installed libraries
- Supports pandas-only, polars-only, both, or neither scenarios
- Separate compile-time (TYPE_CHECKING) and runtime type definitions
- Provides `get_dataframe_types()` for isinstance() checks
- **IMPORTANT**: This file is excluded from coverage (see pyproject.toml:88) because it's tested via isolation scenarios in CI

### Data Flow

```
User calls decorated function
    ↓
@df_in wrapper executes
    ↓
get_parameter() extracts DataFrame from args/kwargs
    ↓
assert_is_dataframe() validates type
    ↓
get_strict() reads config (cached)
    ↓
validate_dataframe() checks columns/dtypes/strictness
    ↓
Original function executes
    ↓
@df_out wrapper validates return value
    ↓
Result returned to caller
```

### Key Design Patterns

1. **Optional Dependency Injection**: dataframe_types.py dynamically builds type unions based on available libraries (pandas/polars)

2. **Lazy Configuration Loading**: Config file read once and cached; expensive operations happen only on first access

3. **Error Context Accumulation**: Validation collects ALL errors before raising, providing complete feedback in single exception

4. **Type-Safe Decorator Composition**: Uses TypeVar to preserve return types through decorator stack for static type checkers

5. **Regex Pattern Abstraction**: Patterns are compiled once and reused; validation layer doesn't handle regex directly

## Configuration

Users can set project-wide defaults in `pyproject.toml`:

```toml
[tool.daffy]
strict = false  # or true to disallow extra columns by default
```

Decorator parameters override config file settings:
```python
@df_in(columns=["A", "B"], strict=True)  # strict=True overrides config
```

## Testing Strategy

**Unit Tests** (tests/test_*.py):
- test_df_in.py - Input validation decorator
- test_df_out.py - Output validation decorator
- test_df_log.py - Logging decorator
- test_decorators.py - Decorator composition
- test_config.py - Configuration loading
- test_optional_dependencies.py - Library detection (always passes)
- test_type_compatibility.py - Type hint compatibility

**Isolation Tests** (CI only via scripts/test_isolated_deps.py):
- Test pandas-only, polars-only, both, and none scenarios in true isolation
- Uses built wheel packages to avoid dev environment contamination
- These tests may "fail" locally since both libraries are typically installed in dev

**Coverage Requirements**:
- Minimum 95% coverage (pyproject.toml:92)
- dataframe_types.py excluded (tested in isolation scenarios)

## Common Patterns

### Adding New Validation Logic

1. Add core validation logic to validation.py
2. Integrate into `validate_dataframe()` function
3. Add error message formatting in utils.py if needed
4. Update decorators.py to pass new parameters
5. Add tests in appropriate test_*.py file

### Supporting New DataFrame Types

1. Update dataframe_types.py to import new library conditionally
2. Add to _available_types list if library is available
3. Update get_dataframe_types() and get_available_library_names()
4. Add tests for new library in test_optional_dependencies.py
5. Add isolation scenario test in scripts/test_isolated_deps.py

### Modifying Configuration Options

1. Update config.py load_config() to parse new option
2. Add accessor function (like get_strict())
3. Update decorators to use new config option
4. Add tests in test_config.py
5. Document in README.md and docs/usage.md

## Important Constraints

- **Python 3.9+ compatibility**: Code must work on Python 3.9-3.14
- **Type hints required**: All functions should have proper type annotations (Ruff ANN rules)
- **No hard dependencies**: DataFrame libraries (Pandas, Polars, Modin, PyArrow) are optional; only tomli and narwhals are required
- **Coverage threshold**: 95% minimum (excluding dataframe_types.py)
- **Import organization**: Use TYPE_CHECKING for static vs runtime type imports

## Version Management

- Version number is in pyproject.toml:3
- Update CHANGELOG.md when making changes
- Follow existing changelog format (see CHANGELOG.md for examples)
- avoid comments that are obvious. aim to improve function or variable names to avoid comments