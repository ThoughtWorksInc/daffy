# Changelog

All notable changes to this project will be documented in this file.

## 1.1.0

### New Features

- **Nullable column validation** - Validate that columns contain no null values
  - Use rich column spec format: `{"column": {"nullable": False}}`
  - Combines with dtype validation: `{"column": {"dtype": "float64", "nullable": False}}`
  - Works with regex patterns: `{"r/Price_\\d+/": {"nullable": False}}`
  - Default behavior unchanged (nullable=True, nulls allowed)

## 1.0.0

### Stable Release

Daffy 1.0.0 marks the first stable release. The public API (`df_in`, `df_out`, `df_log`) is now considered stable and follows semantic versioning.

### Changed

- Development status upgraded from Beta to Production/Stable
- Updated documentation to reflect current tooling (uv, ruff, pyrefly)

### Fixed

- Improved error handling for invalid regex patterns in column specifications
- Better error messages when parameter extraction fails

### Internal

- Extracted duplicate row validation logic into shared helper function
- Added docstrings to public-facing utility functions

### API Stability

As of 1.0.0, Daffy follows semantic versioning:
- Major versions (2.0, 3.0) may contain breaking changes
- Minor versions (1.1, 1.2) add features without breaking changes
- Patch versions (1.0.1, 1.0.2) contain bug fixes only

## 0.19.0

### Performance Improvements

- **Early termination for row validation** - Dramatically faster when validation errors exist
  - Stops scanning after collecting `max_errors` (default behavior)
  - **71-124x speedup** when errors are present (1.2ms vs 140ms for 100k rows with errors)
  - **No overhead** for valid data (maintains 767K rows/sec throughput)
  - Can be disabled with `early_termination=False` parameter for exact error counts
  - Error messages now indicate when scanning stopped early: "stopped scanning early (at least N more row(s) with errors)"

### New Features

- Added `early_termination` parameter to `validate_dataframe_rows()`
  - Default: `True` (stops after `max_errors` for performance)
  - Set to `False` to scan entire DataFrame and get exact error count

## 0.18.0

### Major Performance Improvements

- **2x faster row validation** - Optimized DataFrame conversion and validation pipeline
  - 767K rows/sec on simple validation (was 400K)
  - 165K rows/sec on complex bioinformatics data (32 columns, 5% missing values)
  - Changed from batch TypeAdapter validation to optimized row-by-row with fast DataFrame iteration
  - Use `itertuples()` for efficient row access while preserving None values

### Critical Bug Fix

- **Fixed NaN handling for Optional fields** - NaN values in Optional fields are now properly converted to None
  - Previous implementation failed validation on legitimate missing data
  - Now correctly handles nullable numeric fields in pandas DataFrames
  - Converts numeric columns with NaN to object dtype to preserve None values

### New Features

- **Bioinformatics benchmark** (`scripts/benchmark_bioinformatics.py`) - Realistic validation testing
  - 32-column feature store schema modeling cancer research data
  - Gene expression, clinical measurements, patient demographics, mutations, outcomes
  - Mixed types: floats, ints, strings, bools, Optional fields, Literal enums
  - Missing data patterns (~5%) typical in real-world datasets
  - Cross-field validation (e.g., disease-free survival ≤ follow-up time)

- **Performance benchmarking suite** - Compare Daffy against competing libraries
  - Test multiple scenarios (simple, medium complexity)
  - Multiple dataset sizes (1k, 10k, 100k rows)
  - Compare pandas vs polars implementations

### Internal Improvements

- Removed unused internal functions (`_pandas_to_records_fast`, `_iterate_dataframe_with_index`)
- Simplified error formatting (removed dead code branches)
- Improved test coverage to 98.34%
- Better handling of edge cases in validation error reporting

## 0.17.1

- Improve package discoverability on PyPI and GitHub
  - Add polars, pydantic, decorator to PyPI keywords
  - Add Framework::Pydantic, Testing, and Typing classifiers
  - Add changelog and issues URLs to package metadata
  - Set GitHub topics for better search visibility

## 0.17.0

- Add optional row-level validation using Pydantic models (requires Pydantic >= 2.4.0)
  - New `row_validator` parameter for `@df_in` and `@df_out` decorators
  - Validates actual data values, not just column structure
  - Batch validation for optimal performance (10-100x faster than row-by-row)
  - Informative error messages showing which rows failed and why
  - Configuration via `pyproject.toml`: `row_validation_max_errors` and `row_validation_convert_nans`
  - Works with both Pandas and Polars DataFrames

## 0.16.1

- Internal refactoring: extracted DataFrame type handling to dedicated module for better code organization and maintainability

## 0.16.0

- Removed Pandas and Polars from required dependencies. Daffy will not pull in Polars if your project just uses Pandas
and vice versa. All combinations are dynamically supported and require no changes from existing users.

### Testing & CI

- Added comprehensive CI testing for all dependency combinations
- New test suite validates optional dependency behavior
- Manual testing script for developers (`scripts/test_isolated_deps.py`)
- Updated CI to test pandas-only, polars-only, both, and none scenarios

## 0.15.0

- Exception messages now include function names to improve debugging
  - Input validation: `"Missing columns: ['Col'] in function 'my_func' parameter 'param'. Got columns: ['Other']"`
- Return value validation messages now clearly state "return value" instead of just showing function name
  - Output validation: `"Missing columns: ['Col'] in function 'my_func' return value. Got columns: ['Other']"`

## 0.14.2

- Internal code quality improvements

## 0.14.1

- Internal code quality improvements

## 0.14.0

- Improve df_in error messages to include parameter names

## 0.13.2

- Updated urls for Pypi site compatibility

## 0.13.1

- Update documentation
- Update dependencies

## 0.13.0

- Fix type annotation issues with decorator parameters that could cause type errors in strict type checking
- Use `Sequence` instead of `List` for better type variance compatibility
- Add test case that validates type compatibility

## 0.12.0

- Add support for regex patterns used with column dtype validation

## 0.11.0

- Update function parameter types for better type safety
- Fix missing return statement in df_log decorator
- Added stricter mypy type checking settings

## 0.10.1

- Built and published with UV. No functional changes

## 0.10.0

- Add support for regex patterns in column name validation

## 0.9.4

- Fix to strict flag loading when tool config was missing

## 0.9.3

- Add configuration system to set default strict mode in pyproject.toml
- Improve logging when multiple columns are missing

## 0.9.2

- Add explicit `__all__` export for functions to make Mypy happy

## 0.9.0

- Add marker (`py.typed`) to tell Mypy that the library has type annotations
- Fix bug when using `strict` parameter and no `name` parameter in `@df_in`

## 0.8.0

- Support Polars DataFrames

## 0.7.0

- Support Pandas 2.x
- Drop support for Python 3.7 and 3.8
- Build and test with Python 3.12 also

## 0.6.0

- Make checking columns of multiple function parameters work also with positional arguments (thanks @latvanii)

## 0.5.0

- Added `strict` parameter for `@df_in` and `@df_out`

## 0.4.2

- Added docstrings for the decorators
- Fix import of `@df_log`

## 0.4.1

- Add `include_dtypes` parameter for `@df_log`.
- Fix handling of empty signature with `@df_in`.

## 0.4.0

- Added `@df_log` for logging.
- Improved assertion messages.

## 0.3.0

- Added type hints.

## 0.2.1

- Added Pypi classifiers.

## 0.2.0

- Fixed decorator usage.
- Added functools wraps.

## 0.1.0

- Initial release.