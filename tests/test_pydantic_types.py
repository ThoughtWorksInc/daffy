"""Tests for optional Pydantic dependency support."""

from daffy.pydantic_types import (
    HAS_PYDANTIC,
    PYDANTIC_VERSION,
    BaseModel,
    ConfigDict,
    TypeAdapter,
    ValidationError,
    get_pydantic_available,
    require_pydantic,
)


def test_pydantic_availability() -> None:
    assert HAS_PYDANTIC is True
    assert get_pydantic_available() is True


def test_require_pydantic() -> None:
    require_pydantic()


def test_pydantic_imports_available() -> None:
    assert BaseModel is not None
    assert ValidationError is not None
    assert ConfigDict is not None
    assert TypeAdapter is not None


def test_pydantic_version() -> None:
    assert PYDANTIC_VERSION is not None
    assert isinstance(PYDANTIC_VERSION, str)
    major, minor = map(int, PYDANTIC_VERSION.split(".")[:2])
    assert major >= 2
    if major == 2:
        assert minor >= 4
