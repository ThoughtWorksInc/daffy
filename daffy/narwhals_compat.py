"""Narwhals utilities for DataFrame type checking."""

from __future__ import annotations

from typing import Any

import narwhals as nw


def is_supported_dataframe(obj: Any) -> bool:
    """Check if object is a supported eager DataFrame type."""
    try:
        nw.from_native(obj, eager_only=True)
        return True
    except TypeError:
        return False


def is_pandas_backend(df: Any) -> bool:
    """Check if DataFrame is pandas-backed (needed for NaN handling)."""
    return nw.from_native(df, eager_only=True).implementation.is_pandas()
