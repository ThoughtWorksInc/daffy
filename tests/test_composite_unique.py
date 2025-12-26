"""Tests for composite uniqueness validation."""

import pandas as pd
import pytest

from daffy import df_in, df_out
from daffy.dataframe_types import count_duplicate_rows


class TestCountDuplicateRows:
    def test_no_duplicates(self) -> None:
        df = pd.DataFrame({
            "first": ["A", "A", "B"],
            "last": ["X", "Y", "X"],
        })
        assert count_duplicate_rows(df, ["first", "last"]) == 0

    def test_with_duplicates(self) -> None:
        df = pd.DataFrame({
            "first": ["A", "A", "A"],
            "last": ["X", "X", "Y"],
        })
        # Row 0 and 1 are duplicates (A, X)
        assert count_duplicate_rows(df, ["first", "last"]) == 1

    def test_all_duplicates(self) -> None:
        df = pd.DataFrame({
            "a": [1, 1, 1],
            "b": [2, 2, 2],
        })
        # All rows are duplicates, 2 extra after first
        assert count_duplicate_rows(df, ["a", "b"]) == 2

    def test_single_column(self) -> None:
        df = pd.DataFrame({"a": [1, 1, 2]})
        assert count_duplicate_rows(df, ["a"]) == 1

    def test_empty_dataframe(self) -> None:
        df = pd.DataFrame({"a": [], "b": []})
        assert count_duplicate_rows(df, ["a", "b"]) == 0


class TestCompositeUniqueDecorators:
    def test_df_in_composite_unique_passes(self) -> None:
        @df_in(composite_unique=[["first", "last"]])
        def process(df: pd.DataFrame) -> pd.DataFrame:
            return df

        df = pd.DataFrame({
            "first": ["A", "A", "B"],
            "last": ["X", "Y", "X"],
        })
        result = process(df)
        assert len(result) == 3

    def test_df_in_composite_unique_fails(self) -> None:
        @df_in(composite_unique=[["first", "last"]])
        def process(df: pd.DataFrame) -> pd.DataFrame:
            return df

        df = pd.DataFrame({
            "first": ["A", "A", "B"],
            "last": ["X", "X", "X"],  # A+X appears twice
        })
        with pytest.raises(AssertionError) as exc_info:
            process(df)
        assert "composite_unique" in str(exc_info.value)
        assert "'first' + 'last'" in str(exc_info.value)

    def test_df_out_composite_unique_passes(self) -> None:
        @df_out(composite_unique=[["a", "b"]])
        def create() -> pd.DataFrame:
            return pd.DataFrame({"a": [1, 2], "b": [3, 4]})

        result = create()
        assert len(result) == 2

    def test_df_out_composite_unique_fails(self) -> None:
        @df_out(composite_unique=[["a", "b"]])
        def create() -> pd.DataFrame:
            return pd.DataFrame({"a": [1, 1], "b": [2, 2]})

        with pytest.raises(AssertionError) as exc_info:
            create()
        assert "composite_unique" in str(exc_info.value)

    def test_multiple_composite_unique_constraints(self) -> None:
        @df_in(composite_unique=[["a", "b"], ["c", "d"]])
        def process(df: pd.DataFrame) -> pd.DataFrame:
            return df

        # First constraint passes, second fails
        df = pd.DataFrame({
            "a": [1, 2],
            "b": [3, 4],
            "c": [1, 1],
            "d": [2, 2],
        })
        with pytest.raises(AssertionError) as exc_info:
            process(df)
        assert "'c' + 'd'" in str(exc_info.value)

    def test_composite_unique_with_columns(self) -> None:
        @df_in(
            columns={"a": "int64", "b": "int64"},
            composite_unique=[["a", "b"]],
        )
        def process(df: pd.DataFrame) -> pd.DataFrame:
            return df

        df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
        result = process(df)
        assert len(result) == 2

    def test_composite_unique_only_no_columns(self) -> None:
        @df_in(composite_unique=[["x", "y"]])
        def process(df: pd.DataFrame) -> pd.DataFrame:
            return df

        df = pd.DataFrame({"x": [1, 2], "y": [3, 4], "z": [5, 6]})
        result = process(df)
        assert len(result) == 2

    def test_error_message_includes_count(self) -> None:
        @df_in(composite_unique=[["a", "b"]])
        def process(df: pd.DataFrame) -> pd.DataFrame:
            return df

        df = pd.DataFrame({
            "a": [1, 1, 1, 2],
            "b": [2, 2, 2, 3],
        })  # 2 duplicate rows
        with pytest.raises(AssertionError) as exc_info:
            process(df)
        assert "2 duplicate combinations" in str(exc_info.value)


class TestCompositeUniqueWithLazy:
    def test_lazy_collects_composite_unique_errors(self) -> None:
        @df_in(
            columns={"missing": "int64"},
            composite_unique=[["a", "b"]],
            lazy=True,
        )
        def process(df: pd.DataFrame) -> pd.DataFrame:
            return df

        df = pd.DataFrame({
            "a": [1, 1],
            "b": [2, 2],
        })
        with pytest.raises(AssertionError) as exc_info:
            process(df)
        error = str(exc_info.value)
        assert "Missing columns" in error
        assert "composite_unique" in error
