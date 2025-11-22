# Changelog

All notable changes to this project will be documented in this file.

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