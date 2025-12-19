"""Tests for optional Pydantic dependency support."""

from daffy.pydantic_types import (
    HAS_PYDANTIC,
    BaseModel,
    ValidationError,
    require_pydantic,
)


def test_pydantic_availability() -> None:
    assert HAS_PYDANTIC is True


def test_require_pydantic() -> None:
    require_pydantic()


def test_pydantic_imports_available() -> None:
    assert BaseModel is not None
    assert ValidationError is not None
