# DAFFY Usage Guide

## Basic Usage

Start by importing the needed decorators:

```python
from daffy import df_in, df_out
```

## Input Validation

To check a DataFrame input to a function, annotate the function with `@df_in`. For example the following function expects to get a DataFrame with columns `Brand` and `Price`:

```python
@df_in(columns=["Brand", "Price"])
def process_cars(car_df):
    # do stuff with cars
```

If your function takes multiple arguments, specify the field to be checked with its `name`:

```python
@df_in(name="car_df", columns=["Brand", "Price"])
def process_cars(year, style, car_df):
    # do stuff with cars
```

You can also check columns of multiple arguments if you specify the names:
```python
@df_in(name="car_df", columns=["Brand", "Price"])
@df_in(name="brand_df", columns=["Brand", "BrandName"])
def process_cars(car_df, brand_df):
    # do stuff with cars
```

## Output Validation

To check that a function returns a DataFrame with specific columns, use `@df_out` decorator:

```python
@df_out(columns=["Brand", "Price"])
def get_all_cars():
    # get those cars
    return all_cars_df
```

In case one of the listed columns is missing from the DataFrame, a helpful assertion error is thrown:

```python
AssertionError("Missing columns: ['Price'] in function 'get_all_cars' return value. Got columns: ['Brand']")
```

The error message clearly indicates that this is a **return value** validation failure in the function `get_all_cars`.

## Combined Validation

To check both input and output, just use both annotations on the same function:

```python
@df_in(columns=["Brand", "Price"])
@df_out(columns=["Brand", "Price"])
def filter_cars(car_df):
    # filter some cars
    return filtered_cars_df
```

Note that error messages will clearly distinguish between input and output validation failures:
- Input validation: `"Missing columns: ['Price'] in function 'filter_cars' parameter 'car_df'. Got columns: ['Brand']"`
- Output validation: `"Missing columns: ['Price'] in function 'filter_cars' return value. Got columns: ['Brand']"`

## Column Pattern Matching with Regex

You can use regex patterns to match column names that follow a specific pattern. This is useful when working with dynamic column names or when dealing with many similar columns.

Define a regex pattern by using the format `"r/pattern/"`:

```python
@df_in(columns=["Brand", "r/Price_\d+/"])
def process_data(df):
    # This will accept DataFrames with columns like "Brand", "Price_1", "Price_2", etc.
    ...
```

In this example:
- The DataFrame must have a column named exactly "Brand"
- The DataFrame must have at least one column matching the pattern "Price_\d+" (e.g., "Price_1", "Price_2", etc.)

If no columns match a regex pattern, an error is raised:

```
AssertionError: Missing columns: ['r/Price_\d+/'] in function 'process_data' parameter 'df'. Got columns: ['Brand', 'Model']
```

Regex patterns are also considered in strict mode. Any column matching a regex pattern is considered valid.

## Data Type Validation

If you want to also check the data types of each column, you can replace the column array:

```python
columns=["Brand", "Price"]
```

with a dict:

```python
columns={"Brand": "object", "Price": "int64"}
```

This will not only check that the specified columns are found from the DataFrame but also that their `dtype` is the expected. In case of a wrong `dtype`, an error message similar to following will explain the mismatch:

```
AssertionError("Column Price in function 'process_cars' parameter 'car_df' has wrong dtype. Was int64, expected float64")
```

### Combining Regex Patterns with Data Type Validation

You can use regex patterns in dictionaries that specify data types as well:

```python
@df_in(columns={"Brand": "object", "r/Price_\d+/": "int64"})
def process_data(df):
    # This will check that all columns matching "Price_\d+" have int64 dtype
    ...
```

In this example:
- The DataFrame must have a column named exactly "Brand" with dtype "object"
- Any columns matching the pattern "Price_\d+" (e.g., "Price_1", "Price_2") must have dtype "int64"

If a column matches the regex pattern but has the wrong dtype, an error is raised:

```
AssertionError: Column Price_2 in function 'process_data' parameter 'df' has wrong dtype. Was float64, expected int64
```

## Strict Mode

You can enable strict-mode for both `@df_in` and `@df_out`. This will raise an error if the DataFrame contains columns not defined in the annotation:

```python
@df_in(columns=["Brand"], strict=True)
def process_cars(car_df):
    # do stuff with cars
```

will, when `car_df` contains columns `["Brand", "Price"]` raise an error:

```
AssertionError: DataFrame in function 'process_cars' parameter 'car_df' contained unexpected column(s): Price
```

## Project-wide Configuration

You can set the default value for strict mode at the project level by adding a `[tool.daffy]` section to your `pyproject.toml` file:

```toml
[tool.daffy]
strict = true
```

When this configuration is present, all `@df_in` and `@df_out` decorators will use strict mode by default. You can still override this setting on individual decorators:

```python
# Uses strict=true from project config
@df_in(columns=["Brand"])
# Explicitly disable strict mode for this decorator
@df_out(columns=["Brand", "FilteredPrice"], strict=False)
def filter_cars(car_df):
    # filter some cars
    return filtered_cars_df
```

## Row Validation

In addition to column validation, Daffy supports row-level validation using Pydantic models. This allows you to validate actual data values, not just column structure.

**Performance consideration:** Column validation is lightweight with minimal overhead. Row validation validates data values and will impact performance, especially on large DataFrames. Use row validation when data quality is critical.

### Basic Row Validation

First, install Pydantic if you haven't already:

```bash
pip install 'pydantic>=2.4.0'
```

Define a Pydantic model describing your expected row structure:

```python
from pydantic import BaseModel, Field
from daffy import df_in

class Product(BaseModel):
    name: str
    price: float = Field(gt=0)  # Price must be positive
    stock: int = Field(ge=0)    # Stock cannot be negative

@df_in(row_validator=Product)
def process_inventory(df):
    return df
```

If any rows fail validation, you'll get a detailed error message:

```python
AssertionError: Row validation failed for 2 out of 100 rows:

  Row 5:
    - price: Input should be greater than 0

  Row 12:
    - stock: Input should be greater than or equal to 0

 in function 'process_inventory' parameter 'df'
```

### Combined Column and Row Validation

You can validate both columns and row data:

```python
@df_in(columns=["name", "price", "stock"], row_validator=Product)
def process_inventory(df):
    return df
```

Column validation runs first (fast check), then row validation runs if columns are valid.

### Validating Return Values

Use `row_validator` with `@df_out` to validate returned DataFrames:

```python
@df_out(row_validator=Product)
def load_products():
    return products_df
```

### Configuration

Row validation behavior can be configured in `pyproject.toml`:

```toml
[tool.daffy]
row_validation_max_errors = 5  # Show up to 5 failed rows (default: 5)
row_validation_convert_nans = true  # Convert NaN to None (default: true)
```

### Performance Optimization

Row validation includes an early termination optimization. When validation errors are found, scanning stops after collecting `max_errors` failures instead of processing the entire DataFrame.

**Performance impact:**
- **DataFrames with errors:** 71-124x faster (stops early)
- **DataFrames without errors:** No overhead (must scan all rows)

For example, validating 100k rows with early errors takes ~1.2ms instead of ~140ms.

This optimization is enabled by default. Error messages will show "X out of Y+ rows" when early termination occurs, indicating there may be additional errors beyond those displayed.

### Advanced Features

Pydantic's validation features work seamlessly:

```python
from pydantic import BaseModel, Field, ConfigDict, field_validator

class Employee(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True)

    name: str
    email: str
    age: int = Field(ge=18, le=120)
    salary: float = Field(gt=0)

    @field_validator('email')
    @classmethod
    def validate_email(cls, v):
        if '@' not in v:
            raise ValueError('must contain @')
        return v

@df_in(row_validator=Employee)
def process_employees(df):
    return df
```

## Logging

To quickly check what the incoming and outgoing dataframes contain, you can add a `@df_log` annotation to the function. For example adding `@df_log` to the above `filter_cars` function will produce log lines:

```
Function filter_cars parameters contained a DataFrame: columns: ['Brand', 'Price']
Function filter_cars returned a DataFrame: columns: ['Brand', 'Price']
```

or with `@df_log(include_dtypes=True)` you get:

```
Function filter_cars parameters contained a DataFrame: columns: ['Brand', 'Price'] with dtypes ['object', 'int64']
Function filter_cars returned a DataFrame: columns: ['Brand', 'Price'] with dtypes ['object', 'int64']
```