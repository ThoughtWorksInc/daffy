"""Tests for duplicate value detection utility."""

from typing import Any, Callable

from daffy.dataframe_types import count_duplicate_values


class TestCountDuplicateValues:
    def test_detects_duplicates(self, make_df: Callable[[dict[str, Any]], Any]) -> None:
        df = make_df({"a": [1, 2, 2, 3]})
        assert count_duplicate_values(df, "a") == 1

    def test_no_duplicates(self, make_df: Callable[[dict[str, Any]], Any]) -> None:
        df = make_df({"a": [1, 2, 3, 4]})
        assert count_duplicate_values(df, "a") == 0

    def test_multiple_duplicates(self, make_df: Callable[[dict[str, Any]], Any]) -> None:
        df = make_df({"a": [1, 1, 1, 2, 2]})
        assert count_duplicate_values(df, "a") == 3
