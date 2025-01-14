# DAFFY DataFrame Column Validator
[![PyPI](https://img.shields.io/pypi/v/daffy)](https://pypi.org/project/daffy/)
![PyPI - Python Version](https://img.shields.io/pypi/pyversions/daffy)
![test](https://github.com/fourkind/daffy/workflows/test/badge.svg)
[![codecov](https://codecov.io/gh/vertti/daffy/graph/badge.svg?token=00OL75TW4W)](https://codecov.io/gh/vertti/daffy)

## Description 

Working with DataFrames often means passing them through multiple transformation functions, making it easy to lose track of their structure over time. DAFFY adds runtime validation and documentation to your DataFrame operations through simple decorators. By declaring the expected columns and types in your function definitions, you can:

```python
@df_in(columns=["price", "bedrooms", "location"])
@df_out(columns=["price_per_room", "price_category"])
def analyze_housing(houses_df):
    # Transform raw housing data into price analysis
    return analyzed_df
```

Like type hints for DataFrames, DAFFY helps you catch structural mismatches early and keeps your data pipeline documentation synchronized with the code. Compatible with both Pandas and Polars.


## Table of Contents
* [Installation](#installation)
* [Usage](#usage)
* [Contributing](#contributing)
* [Tests](#tests)
* [License](#license)
* [Changelog](#changelog)

## Installation

Install with your favorite Python dependency manager like

```sh
pip install daffy
```

## Usage 

Start by importing the needed decorators:

```python
from daffy import df_in, df_out
```

To check a DataFrame input to a function, annotate the function with `@df_in`. For example the following function expects to get
a DataFrame with columns `Brand` and `Price`:

```python
@df_in(columns=["Brand", "Price"])
def process_cars(car_df):
    # do stuff with cars
```

If your function takes multiple arguments, specify the field to be checked with it's `name`:

```python
@df_in(name="car_df", columns=["Brand", "Price"])
def process_cars(year, style, car_df):
    # do stuff with cars
```

You can also check columns of multiple arguments if you specify the names
```python
@df_in(name="car_df", columns=["Brand", "Price"])
@df_in(name="brand_df", columns=["Brand", "BrandName"])
def process_cars(car_df, brand_df):
    # do stuff with cars
```

To check that a function returns a DataFrame with specific columns, use `@df_out` decorator:

```python
@df_out(columns=["Brand", "Price"])
def get_all_cars():
    # get those cars
    return all_cars_df
```

In case one of the listed columns is missing from the DataFrame, a helpful assertion error is thrown:

```python
AssertionError("Column Price missing from DataFrame. Got columns: ['Brand']")
```

To check both input and output, just use both annotations on the same function:

```python
@df_in(columns=["Brand", "Price"])
@df_out(columns=["Brand", "Price"])
def filter_cars(car_df):
    # filter some cars
    return filtered_cars_df
```

If you want to also check the data types of each column, you can replace the column array:

```python
columns=["Brand", "Price"]
```

with a dict:

```python
columns={"Brand": "object", "Price": "int64"}
```

This will not only check that the specified columns are found from the DataFrame but also that their `dtype` is the expected.
In case of a wrong `dtype`, an error message similar to following will explain the mismatch:

```
AssertionError("Column Price has wrong dtype. Was int64, expected float64")
```

You can enable strict-mode for both `@df_in` and `@df_out`. This will raise an error if the DataFrame contains columns
not defined in the annotation:

```python
@df_in(columns=["Brand"], strict=True)
def process_cars(car_df):
    # do stuff with cars
```

will, when `car_df` contains columns `["Brand", "Price"]` raise an error:

```
AssertionError: DataFrame contained unexpected column(s): Price
```

To quickly check what the incoming and outgoing dataframes contain, you can add a `@df_log` annotation to the function. For
example adding `@df_log` to the above `filter_cars` function will product log lines:

```
Function filter_cars parameters contained a DataFrame: columns: ['Brand', 'Price']
Function filter_cars returned a DataFrame: columns: ['Brand', 'Price']
```

or with `@df_log(include_dtypes=True)` you get:

```
Function filter_cars parameters contained a DataFrame: columns: ['Brand', 'Price'] with dtypes ['object', 'int64']
Function filter_cars returned a DataFrame: columns: ['Brand', 'Price'] with dtypes ['object', 'int64']
```

## Contributing

Contributions are accepted. Include tests in PR's.

## Development

To run the tests, clone the repository, install dependencies with Poetry and run tests with PyTest:

```sh
poetry install
poetry shell
pytest
```

To enable linting on each commit, run `pre-commit install`. After that, your every commit will be checked with `isort`, `black` and `flake8`.

## License

MIT

## Changelog

### 0.9.0

- Add marker (`py.typed`) to tell Mypy that the library has type annotations
- Fix bug when using `strict` parameter and no `name` parameter in `@df_in`

### 0.8.0

- Support Polars DataFrames

### 0.7.0

- Support Pandas 2.x
- Drop support for Python 3.7 and 3.8
- Build and test with Python 3.12 also

### 0.6.0

- Make checking columns of multiple function parameters work also with positional arguments (thanks @latvanii)

### 0.5.0

- Added `strict` parameter for `@df_in` and `@df_out`

### 0.4.2

- Added docstrings for the decorators
- Fix import of `@df_log`

### 0.4.1

- Add `include_dtypes` parameter for `@df_log`.
- Fix handling of empty signature with `@df_in`.

### 0.4.0

- Added `@df_log` for logging.
- Improved assertion messages.

### 0.3.0

- Added type hints.

### 0.2.1

- Added Pypi classifiers. 

### 0.2.0

- Fixed decorator usage.
- Added functools wraps.

### 0.1.0

- Initial release.

