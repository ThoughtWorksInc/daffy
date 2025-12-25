# Daffy

**Validate pandas and Polars DataFrames with Python decorators.**

Daffy catches missing columns, wrong data types, and invalid values at runtime — before they cause errors downstream in your data pipeline. Just add decorators to your functions.

Also supports Modin and PyArrow DataFrames.

<div class="grid cards" markdown>

- :material-lightning-bolt: **Lightweight**

    Column & dtype validation with minimal overhead

- :material-check-all: **Value Constraints**

    Nullability, uniqueness, range checks

- :material-shield-check: **Row Validation**

    Deep validation with Pydantic models

- :material-swap-horizontal: **Multi-Backend**

    Works with pandas, Polars, Modin, PyArrow

</div>

## Quick Example

```python
from daffy import df_in, df_out

@df_in(columns=["price", "bedrooms", "location"])
@df_out(columns=["price_per_room", "price_category"])
def analyze_housing(houses_df):
    # Transform raw housing data into price analysis
    return analyzed_df
```

If a column is missing, has wrong dtype, or violates a constraint — **Daffy fails fast** with a clear error message at the function boundary.

## Installation

=== "pip"

    ```bash
    pip install daffy
    ```

=== "conda"

    ```bash
    conda install -c conda-forge daffy
    ```

Works with whatever DataFrame library you already have installed. Python 3.9–3.14.

## Why Daffy?

Most DataFrame validation tools are schema-first (define schemas separately) or pipeline-wide (run suites over datasets). **Daffy is decorator-first:** validate inputs and outputs where transformations happen.

| | |
|---|---|
| **Non-intrusive** | Just add decorators — no refactoring, no custom DataFrame types, no schema files |
| **Easy to adopt** | Add in 30 seconds, remove just as fast if needed |
| **In-process** | No external stores, orchestrators, or infrastructure |
| **Pay for what you use** | Column validation is essentially free; opt into row validation when needed |

## Next Steps

<div class="grid cards" markdown>

- :material-rocket-launch: **[Getting Started](getting-started.md)**

    Quick introduction to Daffy's core features

- :material-book-open-variant: **[Usage Guide](usage.md)**

    Comprehensive reference for all features

- :material-chef-hat: **[Recipes & Patterns](recipes.md)**

    Real-world examples and best practices

- :material-api: **[API Reference](api.md)**

    Decorator signatures and parameters

</div>
