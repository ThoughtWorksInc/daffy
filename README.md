# Daffy - DataFrame Validator

[![PyPI](https://img.shields.io/pypi/v/daffy)](https://pypi.org/project/daffy/)
![PyPI - Python Version](https://img.shields.io/pypi/pyversions/daffy)
![CI](https://github.com/vertti/daffy/actions/workflows/main.yml/badge.svg)
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

Like type hints for DataFrames, Daffy helps you catch structural mismatches early and keeps your data pipeline documentation synchronized with the code. Column validation is lightweight and fast. For deeper validation, Daffy also supports row-level validation using Pydantic models. Compatible with both Pandas and Polars.

## Key Features

**Column Validation** (lightweight, minimal overhead):
- Validate DataFrame columns at function entry and exit points
- Support regex patterns for matching column names (e.g., `"r/column_\d+/"`)
- Check data types of columns
- Control strictness of validation (allow or disallow extra columns)

**Row Validation** (optional, requires Pydantic >= 2.4.0):
- Validate row data using Pydantic models
- Batch validation for optimal performance
- Informative error messages showing which rows failed and why

**General**:
- Works with both Pandas and Polars DataFrames
- Project-wide configuration via pyproject.toml
- Integrated logging for DataFrame structure inspection
- Enhanced type annotations for improved IDE and type checker support

## Documentation

- [Usage Guide](https://github.com/vertti/daffy/blob/master/docs/usage.md) - Detailed usage instructions
- [Development Guide](https://github.com/vertti/daffy/blob/master/docs/development.md) - Guide for contributing to Daffy
- [Changelog](https://github.com/vertti/daffy/blob/master/CHANGELOG.md) - Version history and release notes

## Installation

Install with your favorite Python dependency manager:

```sh
pip install daffy
```

Daffy works with **pandas**, **polars**, or both - install whichever you need:

```sh
pip install pandas   # for pandas support
pip install polars   # for polars support
```

**Python version support:** 3.9 - 3.14

## Quick Start

### Column Validation

```python
from daffy import df_in, df_out

@df_in(columns=["Brand", "Price"])  # Validate input DataFrame columns
@df_out(columns=["Brand", "Price", "Discount"])  # Validate output DataFrame columns
def apply_discount(cars_df):
    cars_df = cars_df.copy()
    cars_df["Discount"] = cars_df["Price"] * 0.1
    return cars_df
```

### Row Validation

For validating actual data values (requires `pip install 'pydantic>=2.4.0'`):

```python
from pydantic import BaseModel, Field
from daffy import df_in

class Product(BaseModel):
    name: str
    price: float = Field(gt=0)  # Price must be positive
    stock: int = Field(ge=0)    # Stock must be non-negative

@df_in(row_validator=Product)
def process_inventory(df):
    # Process inventory data with validated rows
    return df
```

## Performance

**Column validation** is essentially free - it only checks column names and types, adding negligible overhead to your functions.

**Row validation** validates actual data values and is naturally more expensive, but has been optimized to be performant:
- **Simple validation**: ~770K rows/sec (100K rows in 130ms)
- **Complex validation**: ~165K rows/sec (32 columns, missing values, cross-field validation)

*Benchmarked on MacBook Pro M1 Pro. Performance depends on:*
- **Model complexity**: Number of fields, validators, and custom validation logic
- **Data characteristics**: DataFrame size, missing values, data types
- **Hardware**: CPU speed, available memory

For detailed benchmarks and optimization strategies, see [scripts/README_BENCHMARKS.md](https://github.com/vertti/daffy/blob/master/scripts/README_BENCHMARKS.md).

## License

MIT

