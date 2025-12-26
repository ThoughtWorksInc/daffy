"""Tests for unique column validation."""

from typing import Any

import pandas as pd
import polars as pl
import pytest

from daffy import df_in, df_out
from tests.utils import DataFrameFactory


class TestUniqueBasic:
    def test_unique_true_rejects_duplicates(self, make_df: DataFrameFactory) -> None:
        @df_in(columns={"user_id": {"unique": True}})
        def f(df: Any) -> Any:
            return df

        df = make_df({"user_id": [1, 2, 2, 3]})
        with pytest.raises(AssertionError, match="duplicate value"):
            f(df)

    def test_unique_true_passes_when_all_unique(self, make_df: DataFrameFactory) -> None:
        @df_in(columns={"user_id": {"unique": True}})
        def f(df: Any) -> Any:
            return df

        df = make_df({"user_id": [1, 2, 3, 4]})
        result = f(df)
        assert len(result) == 4

    def test_unique_false_allows_duplicates(self, make_df: DataFrameFactory) -> None:
        @df_in(columns={"user_id": {"unique": False}})
        def f(df: Any) -> Any:
            return df

        df = make_df({"user_id": [1, 2, 2, 3]})
        result = f(df)
        assert len(result) == 4


class TestUniqueDefaultWithDtype:
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


class TestUniqueWithDtype:
    def test_unique_with_dtype_validates_both_pandas(self) -> None:
        @df_in(columns={"user_id": {"dtype": "int64", "unique": True}})
        def f(df: pd.DataFrame) -> pd.DataFrame:
            return df

        df_wrong_dtype = pd.DataFrame({"user_id": ["a", "b", "c"]})
        with pytest.raises(AssertionError, match="wrong dtype"):
            f(df_wrong_dtype)

        df_dups = pd.DataFrame({"user_id": [1, 2, 2, 3]})
        with pytest.raises(AssertionError, match="duplicate value"):
            f(df_dups)

        df_valid = pd.DataFrame({"user_id": [1, 2, 3, 4]})
        result = f(df_valid)
        assert len(result) == 4

    def test_unique_with_dtype_validates_both_polars(self) -> None:
        @df_in(columns={"user_id": {"dtype": pl.Int64, "unique": True}})
        def f(df: pl.DataFrame) -> pl.DataFrame:
            return df

        df_wrong_dtype = pl.DataFrame({"user_id": ["a", "b", "c"]})
        with pytest.raises(AssertionError, match="wrong dtype"):
            f(df_wrong_dtype)

        df_dups = pl.DataFrame({"user_id": [1, 2, 2, 3]})
        with pytest.raises(AssertionError, match="duplicate value"):
            f(df_dups)

        df_valid = pl.DataFrame({"user_id": [1, 2, 3, 4]})
        result = f(df_valid)
        assert len(result) == 4


class TestUniqueWithNullable:
    def test_unique_and_nullable_false(self, make_df: DataFrameFactory) -> None:
        @df_in(columns={"user_id": {"unique": True, "nullable": False}})
        def f(df: Any) -> Any:
            return df

        df_nulls = make_df({"user_id": [1.0, None, 3.0]})
        with pytest.raises(AssertionError, match="null value"):
            f(df_nulls)

        df_dups = make_df({"user_id": [1, 2, 2, 3]})
        with pytest.raises(AssertionError, match="duplicate value"):
            f(df_dups)

        df_valid = make_df({"user_id": [1, 2, 3, 4]})
        result = f(df_valid)
        assert len(result) == 4


class TestUniqueWithRegex:
    def test_regex_unique_applies_to_all_matched(self, make_df: DataFrameFactory) -> None:
        @df_in(columns={"r/ID_\\d+/": {"unique": True}})
        def f(df: Any) -> Any:
            return df

        df_dups = make_df({"ID_1": [1, 1, 3], "ID_2": [1, 2, 3]})
        with pytest.raises(AssertionError, match="duplicate value"):
            f(df_dups)

        df_valid = make_df({"ID_1": [1, 2, 3], "ID_2": [4, 5, 6]})
        result = f(df_valid)
        assert len(result) == 3


class TestUniqueWithDfOut:
    def test_df_out_unique_rejects_duplicates(self, make_df: DataFrameFactory) -> None:
        @df_out(columns={"id": {"unique": True}})
        def f() -> Any:
            return make_df({"id": [1, 2, 2, 3]})

        with pytest.raises(AssertionError, match="duplicate value"):
            f()

    def test_df_out_unique_passes_when_valid(self, make_df: DataFrameFactory) -> None:
        @df_out(columns={"id": {"unique": True}})
        def f() -> Any:
            return make_df({"id": [1, 2, 3, 4]})

        result = f()
        assert len(result) == 4


class TestUniqueBackwardsCompatibility:
    def test_list_format_still_works(self, make_df: DataFrameFactory) -> None:
        @df_in(columns=["user_id", "name"])
        def f(df: Any) -> Any:
            return df

        df = make_df({"user_id": [1, 1, 2], "name": ["a", "b", "c"]})
        result = f(df)
        assert len(result) == 3


class TestUniqueBackwardsCompatibilityWithDtype:
    def test_simple_dtype_dict_still_works_pandas(self) -> None:
        @df_in(columns={"user_id": "int64"})
        def f(df: pd.DataFrame) -> pd.DataFrame:
            return df

        df = pd.DataFrame({"user_id": [1, 1, 2]})
        result = f(df)
        assert len(result) == 3

    def test_simple_dtype_dict_still_works_polars(self) -> None:
        @df_in(columns={"user_id": pl.Int64})
        def f(df: pl.DataFrame) -> pl.DataFrame:
            return df

        df = pl.DataFrame({"user_id": [1, 1, 2]})
        result = f(df)
        assert len(result) == 3
