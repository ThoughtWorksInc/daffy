"""Narwhals compatibility layer for unified DataFrame operations.

This module provides a centralized interface to Narwhals, enabling
library-agnostic DataFrame operations across pandas, polars, and other
supported backends.
"""

from __future__ import annotations

from typing import Any

import narwhals as nw


def wrap_dataframe(df: Any) -> nw.DataFrame[Any]:
    """Wrap a native DataFrame in Narwhals.

    Args:
        df: Native DataFrame (pandas, polars, etc.)

    Returns:
        Narwhals DataFrame wrapper
    """
    return nw.from_native(df, eager_only=True)


def get_columns(df: Any) -> list[str]:
    """Get column names from any supported DataFrame.

    Args:
        df: Native DataFrame (pandas, polars, etc.)

    Returns:
        List of column names
    """
    return wrap_dataframe(df).columns


def count_nulls(df: Any, column: str) -> int:
    """Count null values in a DataFrame column.

    Args:
        df: Native DataFrame (pandas, polars, etc.)
        column: Column name to check

    Returns:
        Number of null values in the column
    """
    return int(wrap_dataframe(df)[column].is_null().sum())


def count_duplicates(df: Any, column: str) -> int:
    """Count duplicate values in a DataFrame column.

    Args:
        df: Native DataFrame (pandas, polars, etc.)
        column: Column name to check

    Returns:
        Number of duplicate values (excludes first occurrence)
    """
    nw_df = wrap_dataframe(df)
    return len(nw_df) - nw_df[column].n_unique()


def is_pandas_backend(df: Any) -> bool:
    """Check if the DataFrame is backed by pandas.

    Args:
        df: Native DataFrame or Narwhals DataFrame

    Returns:
        True if the DataFrame is pandas-backed
    """
    return wrap_dataframe(df).implementation.is_pandas()


def is_polars_backend(df: Any) -> bool:
    """Check if the DataFrame is backed by polars.

    Args:
        df: Native DataFrame or Narwhals DataFrame

    Returns:
        True if the DataFrame is polars-backed
    """
    return wrap_dataframe(df).implementation.is_polars()


def wrap_series(series: Any) -> nw.Series[Any]:
    """Wrap a native Series in Narwhals.

    Args:
        series: Native Series (pandas, polars, etc.)

    Returns:
        Narwhals Series wrapper
    """
    return nw.from_native(series, series_only=True)


def series_is_in(series: Any, values: Any) -> Any:
    """Check if series values are in the given set.

    Args:
        series: Native Series (pandas, polars, etc.)
        values: Values to check membership against

    Returns:
        Native boolean Series
    """
    nw_series = wrap_series(series)
    result = nw_series.is_in(values)
    return nw.to_native(result)


def series_is_null(series: Any) -> Any:
    """Get null mask for a series.

    Args:
        series: Native Series (pandas, polars, etc.)

    Returns:
        Native boolean Series indicating null positions
    """
    nw_series = wrap_series(series)
    result = nw_series.is_null()
    return nw.to_native(result)


def series_fill_null(series: Any, value: Any) -> Any:
    """Fill null values in a series.

    Args:
        series: Native Series (pandas, polars, etc.)
        value: Value to fill nulls with

    Returns:
        Native Series with nulls filled
    """
    nw_series = wrap_series(series)
    result = nw_series.fill_null(value)
    return nw.to_native(result)


def get_dtypes(df: Any) -> list[Any]:
    """Get list of column dtypes from a DataFrame.

    Args:
        df: Native DataFrame (pandas, polars, etc.)

    Returns:
        List of Narwhals dtype objects
    """
    return list(wrap_dataframe(df).schema.values())


def iter_rows(df: Any, named: bool = True) -> Any:
    """Iterate over DataFrame rows.

    Args:
        df: Native DataFrame (pandas, polars, etc.)
        named: If True, return dictionaries with column names as keys.
               If False, return tuples.

    Returns:
        Iterator of row dictionaries or tuples
    """
    return wrap_dataframe(df).iter_rows(named=named)
