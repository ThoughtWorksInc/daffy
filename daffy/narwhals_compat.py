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
