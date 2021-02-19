# DAFFY DataFrame Column Validator
![PyPI](https://img.shields.io/pypi/v/daffy)
![PyPI - Python Version](https://img.shields.io/pypi/pyversions/daffy)
![test](https://github.com/fourkind/daffy/workflows/test/badge.svg)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

## Description 

In projects using Pandas, it's very common to have functions that take Pandas DataFrames as input or produce them as output.
It's hard to figure out quickly what these DataFrames contain. This library offers simple decorators to annotate your functions
so that they document themselves and that documentation is kept up-to-date by validating the input and output on runtime.

## Table of Contents
* [Installation](#installation)
* [Usage](#usage)
* [Contributing](#contributing)
* [Tests](#tests)
* [License](#license)

## Installation

Install with your favorite Python dependency manager like

```sh
pip install daffy
```

or

```sh
poetry add daffy
```


## Usage 

Start by importing the needed decorators:

```
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

To quickly check what the incoming and outgoing dataframes contain, you can add a `@df_log` annotation to the function. For
example adding `@df_log` to the above `filter_cars` function will product log lines:

```
Function filter_cars parameters contained a DataFrame:columns: ['Brand', 'Price']
Function filter_cars returned a DataFrame: columns: ['Brand', 'Price']
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

### 0.4.0

Added `@df_log` for logging.
Improved assertion messages.

### 0.3.0

Added type hints.

### 0.2.1

Added Pypi classifiers. 

### 0.2.0

Fixed decorator usage.
Added functools wraps.

### 0.1.0

Initial release.

