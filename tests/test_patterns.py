"""Tests for pattern matching utilities."""

import pytest

from daffy.patterns import compile_regex_pattern


def test_invalid_regex_pattern_raises_error() -> None:
    with pytest.raises(ValueError, match="Invalid regex pattern"):
        compile_regex_pattern("r/[invalid/")


def test_empty_regex_pattern_raises_error() -> None:
    with pytest.raises(ValueError, match="cannot be empty"):
        compile_regex_pattern("r//")
