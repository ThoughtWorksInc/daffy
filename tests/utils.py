"""Test utilities for cross-backend assertions."""

from typing import Any, Callable

DataFrameFactory = Callable[[dict[str, Any]], Any]


def to_list(column: Any) -> list[Any]:
    """Convert a DataFrame column to a Python list regardless of backend."""
    if hasattr(column, "to_pylist"):
        return column.to_pylist()  # PyArrow
    return list(column)


def get_column_names(df: Any) -> list[str]:
    """Get column names from a DataFrame regardless of backend."""
    if hasattr(df, "column_names"):
        return df.column_names  # PyArrow
    return list(df.columns)
