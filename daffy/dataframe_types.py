"""DataFrame type handling for DAFFY - supports Pandas, Polars, Modin, and PyArrow."""

from __future__ import annotations

from typing import Any

import narwhals as nw
from narwhals.typing import IntoDataFrame, IntoDataFrameT

# Re-export narwhals types for use throughout daffy
__all__ = ["IntoDataFrame", "IntoDataFrameT", "get_available_library_names"]

# Lazy imports - only import what's available (for error messages)
try:
    from pandas import DataFrame as PandasDataFrame

    HAS_PANDAS = True
except ImportError:  # pragma: no cover
    PandasDataFrame = None  # type: ignore
    HAS_PANDAS = False

try:
    from polars import DataFrame as PolarsDataFrame

    HAS_POLARS = True
except ImportError:  # pragma: no cover
    PolarsDataFrame = None  # type: ignore
    HAS_POLARS = False


def get_available_library_names() -> list[str]:
    """
    Get list of available DataFrame library names for error messages.

    Returns:
        list[str]: List of available library names (e.g., ["Pandas", "Polars"])
    """
    available_libs = []
    if HAS_PANDAS:
        available_libs.append("Pandas")
    if HAS_POLARS:
        available_libs.append("Polars")
    return available_libs


def count_null_values(df: Any, column: str) -> int:
    """Count null values in a DataFrame column."""
    return int(nw.from_native(df, eager_only=True)[column].is_null().sum())


def count_duplicate_values(df: Any, column: str) -> int:
    """Count duplicate values in a DataFrame column (excludes first occurrence)."""
    nw_df = nw.from_native(df, eager_only=True)
    return len(nw_df) - nw_df[column].n_unique()


def count_duplicate_rows(df: Any, columns: list[str]) -> int:
    """Count rows with duplicate values across a column combination (excludes first occurrence)."""
    nw_df = nw.from_native(df, eager_only=True)
    total = len(nw_df)
    unique = nw_df.select(columns).unique().shape[0]
    return total - unique
