"""Tests for pattern matching utilities."""

import pytest

from daffy.patterns import compile_regex_pattern


def test_invalid_regex_pattern_raises_error() -> None:
    with pytest.raises(ValueError, match="Invalid regex pattern"):
        compile_regex_pattern("r/[invalid/")
