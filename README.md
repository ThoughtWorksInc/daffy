# Daffy - DataFrame Column Validator

[![PyPI](https://img.shields.io/pypi/v/daffy)](https://pypi.org/project/daffy/)
![PyPI - Python Version](https://img.shields.io/pypi/pyversions/daffy)
![test](https://github.com/fourkind/daffy/workflows/test/badge.svg)
[![codecov](https://codecov.io/gh/vertti/daffy/graph/badge.svg?token=00OL75TW4W)](https://codecov.io/gh/vertti/daffy)

## Description

Working with DataFrames often means passing them through multiple transformation functions, making it easy to lose track of their structure over time. Daffy adds runtime validation and documentation to your DataFrame operations through simple decorators. By declaring the expected columns and types in your function definitions, you can:

```python
@df_in(columns=["price", "bedrooms", "location"])
@df_out(columns=["price_per_room", "price_category"])
def analyze_housing(houses_df):
    # Transform raw housing data into price analysis
    return analyzed_df
```

Like type hints for DataFrames, Daffy helps you catch structural mismatches early and keeps your data pipeline documentation synchronized with the code. Compatible with both Pandas and Polars.

## Key Features

- Validate DataFrame columns at function entry and exit points
- Support regex patterns for matching column names (e.g., `"r/column_\d+/"`)
- Check data types of columns
- Control strictness of validation (allow or disallow extra columns)
- Works with both Pandas and Polars DataFrames
- Project-wide configuration via pyproject.toml
- Integrated logging for DataFrame structure inspection
- Enhanced type annotations for improved IDE and type checker support

## Documentation

- [Usage Guide](https://github.com/ThoughtWorksInc/daffy/blob/master/docs/usage.md) - Detailed usage instructions
- [Development Guide](https://github.com/ThoughtWorksInc/daffy/blob/master/docs/development.md) - Guide for contributing to Daffy
- [Changelog](https://github.com/ThoughtWorksInc/daffy/blob/master/CHANGELOG.md) - Version history and release notes

## Installation

Install with your favorite Python dependency manager:

```sh
pip install daffy
```

## Quick Start

```python
from daffy import df_in, df_out

@df_in(columns=["Brand", "Price"])  # Validate input DataFrame columns
@df_out(columns=["Brand", "Price", "Discount"])  # Validate output DataFrame columns
def apply_discount(cars_df):
    cars_df = cars_df.copy()
    cars_df["Discount"] = cars_df["Price"] * 0.1
    return cars_df
```

## License

MIT

