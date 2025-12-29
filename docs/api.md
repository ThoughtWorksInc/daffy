# API Reference

## Decorators

### @df_in

Validates DataFrame parameters passed to a function.

```python
@df_in(
    name: str | None = None,
    columns: list[str] | dict[str, str | dict] | None = None,
    strict: bool | None = None,
    row_validator: type[BaseModel] | None = None
)
```

**Parameters:**

| Parameter       | Type                      | Description                                                                                   |
| --------------- | ------------------------- | --------------------------------------------------------------------------------------------- |
| `name`          | `str \| None`             | Name of the parameter to validate. If not specified, validates the first DataFrame parameter. |
| `columns`       | `list \| dict \| None`    | Column specification. See [Column Specifications](#column-specifications).                    |
| `strict`        | `bool \| None`            | If `True`, raises error for unexpected columns. Defaults to project config or `False`.        |
| `row_validator` | `type[BaseModel] \| None` | Pydantic model for row-level validation.                                                      |

**Examples:**

```python
# Simple column list
@df_in(columns=["a", "b", "c"])

# With dtypes
@df_in(columns={"a": "int64", "b": "object"})

# With constraints
@df_in(columns={"price": {"dtype": "float64", "nullable": False, "checks": {"gt": 0}}})

# Multiple DataFrames
@df_in(name="orders", columns=["id", "total"])
@df_in(name="customers", columns=["id", "name"])

# Row validation
@df_in(row_validator=OrderModel)
```

---

### @df_out

Validates the DataFrame returned by a function.

```python
@df_out(
    columns: list[str] | dict[str, str | dict] | None = None,
    strict: bool | None = None,
    row_validator: type[BaseModel] | None = None
)
```

**Parameters:**

| Parameter       | Type                      | Description                                                                            |
| --------------- | ------------------------- | -------------------------------------------------------------------------------------- |
| `columns`       | `list \| dict \| None`    | Column specification. See [Column Specifications](#column-specifications).             |
| `strict`        | `bool \| None`            | If `True`, raises error for unexpected columns. Defaults to project config or `False`. |
| `row_validator` | `type[BaseModel] \| None` | Pydantic model for row-level validation.                                               |

**Examples:**

```python
@df_out(columns=["result", "score"])

@df_out(columns={"score": "float64"}, strict=True)

@df_out(row_validator=ResultModel)
```

---

### @df_log

Logs DataFrame structure when entering and exiting a function.

```python
@df_log(
    include_dtypes: bool = False
)
```

**Parameters:**

| Parameter        | Type   | Description                                                        |
| ---------------- | ------ | ------------------------------------------------------------------ |
| `include_dtypes` | `bool` | If `True`, includes column dtypes in log output. Default: `False`. |

**Examples:**

```python
@df_log()
def process(df):
    return df
# Logs: Function process parameters contained a DataFrame: columns: ['a', 'b']
# Logs: Function process returned a DataFrame: columns: ['a', 'b']

@df_log(include_dtypes=True)
def process(df):
    return df
# Logs: ... columns: ['a', 'b'] with dtypes ['int64', 'object']
```

---

## Column Specifications

Columns can be specified in several formats:

### List Format

Simple list of required column names:

```python
columns=["a", "b", "c"]
```

### Dict with Dtypes

Map column names to expected dtypes:

```python
columns={"a": "int64", "b": "object", "c": "float64"}
```

### Rich Column Spec

Full control over column validation:

```python
columns={
    "column_name": {
        "dtype": "float64",      # Expected dtype (optional)
        "nullable": False,       # Allow null values? Default: True
        "unique": True,          # Require unique values? Default: False
        "required": True,        # Is column required? Default: True
        "checks": {              # Value checks (optional)
            "gt": 0,
            "lt": 100
        }
    }
}
```

### Regex Patterns

Match multiple columns with regex:

```python
columns=["id", "r/feature_\\d+/"]  # Matches feature_1, feature_2, etc.

columns={"r/score_\\d+/": {"dtype": "float64", "checks": {"between": (0, 100)}}}
```

---

## Value Checks

Available checks for the `checks` parameter:

| Check       | Argument   | Description                |
| ----------- | ---------- | -------------------------- |
| `gt`        | `number`   | Greater than               |
| `ge`        | `number`   | Greater than or equal      |
| `lt`        | `number`   | Less than                  |
| `le`        | `number`   | Less than or equal         |
| `eq`        | `value`    | Equal to                   |
| `ne`        | `value`    | Not equal to               |
| `between`   | `(lo, hi)` | Value in range (inclusive) |
| `isin`      | `list`     | Value in set               |
| `notnull`   | `True`     | No null values             |
| `str_regex` | `pattern`  | String matches regex       |

**Examples:**

```python
columns={
    "price": {"checks": {"gt": 0, "lt": 10000}},
    "score": {"checks": {"between": (0, 100)}},
    "status": {"checks": {"isin": ["active", "pending"]}},
    "email": {"checks": {"str_regex": r"^[^@]+@[^@]+\.[^@]+$"}}
}
```

---

## Configuration

Configure Daffy in `pyproject.toml`:

```toml
[tool.daffy]
strict = false                 # Default strict mode (default: false)
row_validation_max_errors = 5  # Max errors shown in row validation (default: 5)
checks_max_samples = 5         # Max sample values in check errors (default: 5)
```

Configuration is read from the `pyproject.toml` in the current working directory or any parent directory.
