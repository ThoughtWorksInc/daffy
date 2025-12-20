"""Tests for utility functions."""

import pytest

from daffy.utils import get_parameter


def test_get_parameter_name_not_in_signature() -> None:
    def func(a: int, b: int) -> None:
        pass

    with pytest.raises(ValueError, match="not found in function signature"):
        get_parameter(func, "nonexistent", 1, 2)


def test_get_parameter_not_provided() -> None:
    def func(a: int, b: int, c: int) -> None:
        pass

    with pytest.raises(ValueError, match="not found in function arguments"):
        get_parameter(func, "c", 1, 2)
