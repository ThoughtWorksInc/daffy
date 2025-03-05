# Changelog

All notable changes to this project will be documented in this file.

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