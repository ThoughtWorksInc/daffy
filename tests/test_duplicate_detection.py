"""Tests for duplicate value detection utility."""

from daffy.dataframe_types import count_duplicate_values
from tests.utils import DataFrameFactory


class TestCountDuplicateValues:
    def test_detects_duplicates(self, make_df: DataFrameFactory) -> None:
        df = make_df({"a": [1, 2, 2, 3]})
        assert count_duplicate_values(df, "a") == 1

    def test_no_duplicates(self, make_df: DataFrameFactory) -> None:
        df = make_df({"a": [1, 2, 3, 4]})
        assert count_duplicate_values(df, "a") == 0

    def test_multiple_duplicates(self, make_df: DataFrameFactory) -> None:
        df = make_df({"a": [1, 1, 1, 2, 2]})
        assert count_duplicate_values(df, "a") == 3
