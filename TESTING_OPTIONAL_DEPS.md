# Testing Optional Dependencies

This document describes how to test daffy's optional dependency support for pandas and polars.

## Background

Daffy now supports optional dependencies - you can install it with just pandas, just polars, or both. This testing setup ensures that all combinations work correctly.

## Automated Testing

### CI Pipeline

The GitHub Actions workflow includes three separate jobs:

1. **Standard tests** - Run with both pandas and polars installed (full functionality)
2. **Pytest optional dependency tests** - Run pytest tests that work with available libraries (always pass locally and in CI)
3. **Isolation scenario tests** - Test each scenario in true isolation using built wheels:
   - `pandas-only` - Only pandas is available
   - `polars-only` - Only polars is available
   - `both` - Both libraries available
   - `none` - No DataFrame libraries (should fail gracefully)

### Simple Pytest Tests

The file `tests/test_optional_dependencies.py` contains tests that:
- Verify library detection flags work correctly
- Test that error messages reflect available libraries
- Ensure decorators work with whatever is installed

These tests are designed to always pass regardless of which DataFrame libraries are installed. They run as part of the regular test suite and should succeed when you run `uv run pytest` locally.

## Manual Testing

### Using the Test Script

The `scripts/test_isolated_deps.py` script allows manual testing of different scenarios:

**Note:** The pandas-only and polars-only tests will likely "fail" in local development environments because both libraries are typically installed. These tests are designed to work in CI environments with truly isolated environments using built wheel packages. The test failure messages will explain this.

```bash
# First build a wheel to avoid dev dependencies
uv build --wheel

# Test with pandas only
WHEEL=$(ls dist/daffy-*.whl | head -n1)
uv run --no-project --with "pandas>=1.5.1" --with "$WHEEL" python scripts/test_isolated_deps.py pandas

# Test with polars only
WHEEL=$(ls dist/daffy-*.whl | head -n1)
uv run --no-project --with "polars>=1.7.0" --with "$WHEEL" python scripts/test_isolated_deps.py polars

# Test with both
WHEEL=$(ls dist/daffy-*.whl | head -n1)
uv run --no-project --with "pandas>=1.5.1" --with "polars>=1.7.0" --with "$WHEEL" python scripts/test_isolated_deps.py both

# Test with neither (should fail gracefully)
WHEEL=$(ls dist/daffy-*.whl | head -n1)
uv run --no-project --with "$WHEEL" python scripts/test_isolated_deps.py none
```

### Expected Behaviors

#### Pandas Only
- `HAS_PANDAS = True`, `HAS_POLARS = False`
- Only pandas DataFrames are accepted
- Error messages mention "Pandas DataFrame"

#### Polars Only
- `HAS_PANDAS = False`, `HAS_POLARS = True`
- Only polars DataFrames are accepted
- Error messages mention "Polars DataFrame"

#### Both Libraries
- `HAS_PANDAS = True`, `HAS_POLARS = True`
- Both DataFrame types work
- Error messages mention "Pandas or Polars DataFrame"

#### No Libraries
- Import should fail with: `ImportError: No DataFrame library found. Please install Pandas or Polars`

## Implementation Details

The optional dependency support works through:

1. **Lazy imports** in `daffy/utils.py` with try/except blocks
2. **Runtime type checking** that builds DataFrame type tuples dynamically
3. **Conditional type hints** using `TYPE_CHECKING` for static analysis
4. **Dynamic error messages** that reflect available libraries

## Adding New Tests

When adding tests for optional dependencies:

1. Use the simple approach in `test_optional_dependencies.py`
2. Check `HAS_PANDAS` and `HAS_POLARS` flags to conditionally run tests
3. Use `pytest.mark.skipif` for tests requiring specific libraries
4. Test error message content to ensure it reflects available libraries

## Development Workflow

When working on optional dependency features:

1. Run standard tests: `uv run pytest`
2. Test specific scenarios: `uv run python scripts/test_isolated_deps.py <scenario>`
3. Verify CI passes with all dependency combinations
4. Ensure mypy type checking works: `uv run mypy daffy`