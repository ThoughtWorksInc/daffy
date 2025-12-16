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
