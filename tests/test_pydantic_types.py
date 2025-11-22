"""Tests for optional Pydantic dependency support."""

import pytest


def test_pydantic_availability() -> None:
    """Test that HAS_PYDANTIC reflects actual availability."""
    from daffy.pydantic_types import HAS_PYDANTIC, get_pydantic_available

    try:
        import pydantic

        # Check version
        major, minor = map(int, pydantic.__version__.split(".")[:2])
        if major >= 2 and minor >= 4:
            assert HAS_PYDANTIC is True
            assert get_pydantic_available() is True
        else:
            # Pydantic too old
            assert HAS_PYDANTIC is False
            assert get_pydantic_available() is False
    except ImportError:
        # Pydantic not installed
        assert HAS_PYDANTIC is False
        assert get_pydantic_available() is False


def test_require_pydantic_when_available() -> None:
    """Test require_pydantic when Pydantic >= 2.4.0 is installed."""
    from daffy.pydantic_types import HAS_PYDANTIC, require_pydantic

    if HAS_PYDANTIC:
        # Should not raise
        require_pydantic()
    else:
        pytest.skip("Pydantic not installed or version < 2.4.0")


def test_require_pydantic_when_unavailable() -> None:
    """Test require_pydantic raises when Pydantic is not available."""
    from daffy.pydantic_types import HAS_PYDANTIC, require_pydantic

    if not HAS_PYDANTIC:
        with pytest.raises(ImportError, match="Pydantic >= 2.4.0 is required"):
            require_pydantic()
    else:
        pytest.skip("Pydantic is installed")


def test_pydantic_imports_available_when_installed() -> None:
    """Test that Pydantic types are available when installed."""
    from daffy.pydantic_types import HAS_PYDANTIC

    if HAS_PYDANTIC:
        from daffy.pydantic_types import BaseModel, ConfigDict, TypeAdapter, ValidationError

        # All should be importable
        assert BaseModel is not None
        assert ValidationError is not None
        assert ConfigDict is not None
        assert TypeAdapter is not None
    else:
        pytest.skip("Pydantic not installed or version < 2.4.0")


def test_pydantic_version_check() -> None:
    """Test that PYDANTIC_VERSION is set correctly."""
    from daffy.pydantic_types import HAS_PYDANTIC, PYDANTIC_VERSION

    if HAS_PYDANTIC:
        assert PYDANTIC_VERSION is not None
        assert isinstance(PYDANTIC_VERSION, str)
        # Should be at least 2.4.0
        major, minor = map(int, PYDANTIC_VERSION.split(".")[:2])
        assert major >= 2
        if major == 2:
            assert minor >= 4
    else:
        assert PYDANTIC_VERSION is None
