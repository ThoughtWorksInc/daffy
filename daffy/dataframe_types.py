"""DataFrame type handling for DAFFY - supports Pandas, Polars, Modin, and PyArrow."""

from __future__ import annotations

from narwhals.typing import IntoDataFrame, IntoDataFrameT

# Re-export narwhals types for use throughout daffy
__all__ = ["IntoDataFrame", "IntoDataFrameT", "get_available_library_names"]

# Check which DataFrame libraries are available (for error messages and early failure)
try:
    import pandas as pd  # noqa: F401

    HAS_PANDAS = True
except ImportError:  # pragma: no cover
    HAS_PANDAS = False

try:
    import polars as pl  # noqa: F401

    HAS_POLARS = True
except ImportError:  # pragma: no cover
    HAS_POLARS = False

# Fail early if no DataFrame library is available
if not HAS_PANDAS and not HAS_POLARS:  # pragma: no cover
    raise ImportError("No DataFrame library found. Install a supported library: pip install pandas")


def get_available_library_names() -> list[str]:
    """Get list of available DataFrame library names for error messages.

    Returns:
        list[str]: List of available library names (e.g., ["Pandas", "Polars"])

    """
    available_libs = []
    if HAS_PANDAS:
        available_libs.append("Pandas")
    if HAS_POLARS:
        available_libs.append("Polars")
    return available_libs
