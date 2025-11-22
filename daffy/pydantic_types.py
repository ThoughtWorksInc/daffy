"""Optional Pydantic dependency support.

This module handles optional Pydantic imports similar to how dataframe_types.py
handles optional pandas/polars imports. It supports runtime detection and
graceful fallback when Pydantic is not available.

Requires Pydantic >= 2.4.0 for TypeAdapter support.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

# Runtime import with availability flag
try:
    from pydantic import BaseModel, ConfigDict, TypeAdapter, ValidationError
    from pydantic import __version__ as PYDANTIC_VERSION  # noqa: N812

    # Check for minimum version (2.4.0 for TypeAdapter)
    major, minor = map(int, PYDANTIC_VERSION.split(".")[:2])
    if major < 2 or (major == 2 and minor < 4):
        raise ImportError(f"Pydantic {PYDANTIC_VERSION} is too old. Daffy requires Pydantic >= 2.4.0")

    HAS_PYDANTIC = True
except ImportError:  # pragma: no cover
    BaseModel = None  # type: ignore[assignment, misc]
    ValidationError = None  # type: ignore[assignment, misc]
    ConfigDict = None  # type: ignore[assignment, misc]
    TypeAdapter = None  # type: ignore[assignment, misc]
    HAS_PYDANTIC = False
    PYDANTIC_VERSION = None

# Compile-time types for type checkers
if TYPE_CHECKING:
    from pydantic import BaseModel, ConfigDict, TypeAdapter, ValidationError  # noqa: F401


def get_pydantic_available() -> bool:
    """
    Check if Pydantic is available.

    Returns:
        True if Pydantic >= 2.4.0 is installed, False otherwise
    """
    return HAS_PYDANTIC


def require_pydantic() -> None:
    """
    Raise ImportError if Pydantic is not available or version is too old.

    Raises:
        ImportError: If Pydantic is not installed or version < 2.4.0
    """
    if not HAS_PYDANTIC:
        raise ImportError(
            "Pydantic >= 2.4.0 is required for row validation. Install it with: pip install 'pydantic>=2.4.0'"
        )
