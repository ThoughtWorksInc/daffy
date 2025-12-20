"""DataFrame type handling for DAFFY - supports Pandas, Polars, Modin, and PyArrow."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Union

# Lazy imports - only import what's available
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

# Build DataFrame type dynamically based on what's available
if TYPE_CHECKING:
    # For static type checking, assume both are available
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
        raise ImportError("No DataFrame library found. Install a supported library: pip install pandas")

    DataFrameType = Union[tuple(_available_types)]


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
    import narwhals as nw

    return int(nw.from_native(df, eager_only=True)[column].is_null().sum())


def count_duplicate_values(df: Any, column: str) -> int:
    """Count duplicate values in a DataFrame column (excludes first occurrence)."""
    import narwhals as nw

    nw_df = nw.from_native(df, eager_only=True)
    return len(nw_df) - nw_df[column].n_unique()
