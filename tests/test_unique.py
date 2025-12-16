"""Tests for unique column validation."""

import pandas as pd
import polars as pl
import pytest

from daffy import df_in


class TestUniqueBasic:
    def test_unique_true_rejects_duplicates_pandas(self) -> None:
        @df_in(columns={"user_id": {"unique": True}})
        def f(df: pd.DataFrame) -> pd.DataFrame:
            return df

        df = pd.DataFrame({"user_id": [1, 2, 2, 3]})
        with pytest.raises(AssertionError, match="duplicate value"):
            f(df)

    def test_unique_true_rejects_duplicates_polars(self) -> None:
        @df_in(columns={"user_id": {"unique": True}})
        def f(df: pl.DataFrame) -> pl.DataFrame:
            return df

        df = pl.DataFrame({"user_id": [1, 2, 2, 3]})
        with pytest.raises(AssertionError, match="duplicate value"):
            f(df)

    def test_unique_true_passes_when_all_unique_pandas(self) -> None:
        @df_in(columns={"user_id": {"unique": True}})
        def f(df: pd.DataFrame) -> pd.DataFrame:
            return df

        df = pd.DataFrame({"user_id": [1, 2, 3, 4]})
        result = f(df)
        assert len(result) == 4

    def test_unique_true_passes_when_all_unique_polars(self) -> None:
        @df_in(columns={"user_id": {"unique": True}})
        def f(df: pl.DataFrame) -> pl.DataFrame:
            return df

        df = pl.DataFrame({"user_id": [1, 2, 3, 4]})
        result = f(df)
        assert len(result) == 4

    def test_default_allows_duplicates_pandas(self) -> None:
        @df_in(columns={"user_id": {"dtype": "int64"}})
        def f(df: pd.DataFrame) -> pd.DataFrame:
            return df

        df = pd.DataFrame({"user_id": [1, 2, 2, 3]})
        result = f(df)
        assert len(result) == 4

    def test_default_allows_duplicates_polars(self) -> None:
        @df_in(columns={"user_id": {"dtype": pl.Int64}})
        def f(df: pl.DataFrame) -> pl.DataFrame:
            return df

        df = pl.DataFrame({"user_id": [1, 2, 2, 3]})
        result = f(df)
        assert len(result) == 4

    def test_unique_false_allows_duplicates_pandas(self) -> None:
        @df_in(columns={"user_id": {"unique": False}})
        def f(df: pd.DataFrame) -> pd.DataFrame:
            return df

        df = pd.DataFrame({"user_id": [1, 2, 2, 3]})
        result = f(df)
        assert len(result) == 4

    def test_unique_false_allows_duplicates_polars(self) -> None:
        @df_in(columns={"user_id": {"unique": False}})
        def f(df: pl.DataFrame) -> pl.DataFrame:
            return df

        df = pl.DataFrame({"user_id": [1, 2, 2, 3]})
        result = f(df)
        assert len(result) == 4


class TestUniqueWithDtype:
    def test_unique_with_dtype_validates_both_pandas(self) -> None:
        @df_in(columns={"user_id": {"dtype": "int64", "unique": True}})
        def f(df: pd.DataFrame) -> pd.DataFrame:
            return df

        # Wrong dtype
        df_wrong_dtype = pd.DataFrame({"user_id": ["a", "b", "c"]})
        with pytest.raises(AssertionError, match="wrong dtype"):
            f(df_wrong_dtype)

        # Duplicates
        df_dups = pd.DataFrame({"user_id": [1, 2, 2, 3]})
        with pytest.raises(AssertionError, match="duplicate value"):
            f(df_dups)

        # Valid
        df_valid = pd.DataFrame({"user_id": [1, 2, 3, 4]})
        result = f(df_valid)
        assert len(result) == 4

    def test_unique_with_dtype_validates_both_polars(self) -> None:
        @df_in(columns={"user_id": {"dtype": pl.Int64, "unique": True}})
        def f(df: pl.DataFrame) -> pl.DataFrame:
            return df

        # Wrong dtype
        df_wrong_dtype = pl.DataFrame({"user_id": ["a", "b", "c"]})
        with pytest.raises(AssertionError, match="wrong dtype"):
            f(df_wrong_dtype)

        # Duplicates
        df_dups = pl.DataFrame({"user_id": [1, 2, 2, 3]})
        with pytest.raises(AssertionError, match="duplicate value"):
            f(df_dups)

        # Valid
        df_valid = pl.DataFrame({"user_id": [1, 2, 3, 4]})
        result = f(df_valid)
        assert len(result) == 4


class TestUniqueWithNullable:
    def test_unique_and_nullable_false_pandas(self) -> None:
        @df_in(columns={"user_id": {"unique": True, "nullable": False}})
        def f(df: pd.DataFrame) -> pd.DataFrame:
            return df

        # Has nulls
        df_nulls = pd.DataFrame({"user_id": [1.0, None, 3.0]})
        with pytest.raises(AssertionError, match="null value"):
            f(df_nulls)

        # Has duplicates
        df_dups = pd.DataFrame({"user_id": [1, 2, 2, 3]})
        with pytest.raises(AssertionError, match="duplicate value"):
            f(df_dups)

        # Valid
        df_valid = pd.DataFrame({"user_id": [1, 2, 3, 4]})
        result = f(df_valid)
        assert len(result) == 4

    def test_unique_and_nullable_false_polars(self) -> None:
        @df_in(columns={"user_id": {"unique": True, "nullable": False}})
        def f(df: pl.DataFrame) -> pl.DataFrame:
            return df

        # Has nulls
        df_nulls = pl.DataFrame({"user_id": [1.0, None, 3.0]})
        with pytest.raises(AssertionError, match="null value"):
            f(df_nulls)

        # Has duplicates
        df_dups = pl.DataFrame({"user_id": [1, 2, 2, 3]})
        with pytest.raises(AssertionError, match="duplicate value"):
            f(df_dups)

        # Valid
        df_valid = pl.DataFrame({"user_id": [1, 2, 3, 4]})
        result = f(df_valid)
        assert len(result) == 4
