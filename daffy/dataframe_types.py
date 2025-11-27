"""DataFrame type handling for DAFFY - supports Pandas and Polars."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Union

# Lazy imports - only import what's available
try:
    import pandas as pd
    from pandas import DataFrame as PandasDataFrame

    HAS_PANDAS = True
except ImportError:  # pragma: no cover
    pd = None  # type: ignore
    PandasDataFrame = None  # type: ignore
    HAS_PANDAS = False

try:
    import polars as pl
    from polars import DataFrame as PolarsDataFrame

    HAS_POLARS = True
except ImportError:  # pragma: no cover
    pl = None  # type: ignore
    PolarsDataFrame = None  # type: ignore
    HAS_POLARS = False

# Build DataFrame type dynamically based on what's available
if TYPE_CHECKING:
    # For static type checking, assume both are available
    from typing import TypeGuard

    from pandas import DataFrame as PandasDataFrame
    from polars import DataFrame as PolarsDataFrame

    DataFrameType = Union[PandasDataFrame, PolarsDataFrame]
else:
    # For runtime, build type tuple from available libraries
    _available_types = []
    if HAS_PANDAS:
        _available_types.append(PandasDataFrame)
    if HAS_POLARS:
        _available_types.append(PolarsDataFrame)

    if not _available_types:
        raise ImportError(
            "No DataFrame library found. Please install Pandas or Polars: pip install pandas  OR  pip install polars"
        )

    DataFrameType = Union[tuple(_available_types)]


def get_dataframe_types() -> tuple[Any, ...]:
    """
    Get tuple of available DataFrame types for isinstance checks.

    Returns:
        tuple: Tuple of available DataFrame classes (pd.DataFrame, pl.DataFrame, or both)
    """
    dataframe_types: list[Any] = []
    if HAS_PANDAS and pd is not None:
        dataframe_types.append(pd.DataFrame)
    if HAS_POLARS and pl is not None:
        dataframe_types.append(pl.DataFrame)
    return tuple(dataframe_types)


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


def is_pandas_dataframe(df: Any) -> TypeGuard[PandasDataFrame]:
    return HAS_PANDAS and pd is not None and isinstance(df, pd.DataFrame)


def is_polars_dataframe(df: Any) -> TypeGuard[PolarsDataFrame]:
    return HAS_POLARS and pl is not None and isinstance(df, pl.DataFrame)


# Cache the types tuple at module level for efficiency
_DATAFRAME_TYPES = get_dataframe_types()
