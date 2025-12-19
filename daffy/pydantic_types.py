"""Optional Pydantic dependency support.

This module handles optional Pydantic imports similar to how dataframe_types.py
handles optional pandas/polars imports. It supports runtime detection and
graceful fallback when Pydantic is not available.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

# Runtime import with availability flag
try:
    from pydantic import BaseModel, ValidationError

    HAS_PYDANTIC = True
except ImportError:  # pragma: no cover
    BaseModel = None  # type: ignore[assignment, misc]
    ValidationError = None  # type: ignore[assignment, misc]
    HAS_PYDANTIC = False

# Compile-time types for type checkers
if TYPE_CHECKING:
    from pydantic import BaseModel, ValidationError  # noqa: F401


def require_pydantic() -> None:
    """Raise ImportError if Pydantic is not available."""
    if not HAS_PYDANTIC:
        raise ImportError("Pydantic is required for row validation. Install it with: pip install daffy[pydantic]")
