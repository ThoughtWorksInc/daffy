"""Tests for nullable column validation."""

import pandas as pd
import polars as pl
import pytest

from daffy import df_in


class TestNullableBasic:
    def test_nullable_false_rejects_nulls_pandas(self) -> None:
        @df_in(columns={"price": {"nullable": False}})
        def f(df: pd.DataFrame) -> pd.DataFrame:
            return df

        df = pd.DataFrame({"price": [1.0, None, 3.0]})
        with pytest.raises(AssertionError, match="null value"):
            f(df)

    def test_nullable_false_rejects_nulls_polars(self) -> None:
        @df_in(columns={"price": {"nullable": False}})
        def f(df: pl.DataFrame) -> pl.DataFrame:
            return df

        df = pl.DataFrame({"price": [1.0, None, 3.0]})
        with pytest.raises(AssertionError, match="null value"):
            f(df)

    def test_nullable_false_passes_without_nulls_pandas(self) -> None:
        @df_in(columns={"price": {"nullable": False}})
        def f(df: pd.DataFrame) -> pd.DataFrame:
            return df

        df = pd.DataFrame({"price": [1.0, 2.0, 3.0]})
        result = f(df)
        assert len(result) == 3

    def test_nullable_false_passes_without_nulls_polars(self) -> None:
        @df_in(columns={"price": {"nullable": False}})
        def f(df: pl.DataFrame) -> pl.DataFrame:
            return df

        df = pl.DataFrame({"price": [1.0, 2.0, 3.0]})
        result = f(df)
        assert len(result) == 3


class TestNullableWithDtype:
    def test_nullable_with_dtype_pandas(self) -> None:
        @df_in(columns={"price": {"dtype": "float64", "nullable": False}})
        def f(df: pd.DataFrame) -> pd.DataFrame:
            return df

        # Should check both dtype AND nullable
        df = pd.DataFrame({"price": [1.0, None, 3.0]})
        with pytest.raises(AssertionError, match="null value"):
            f(df)

    def test_nullable_with_dtype_polars(self) -> None:
        @df_in(columns={"price": {"dtype": pl.Float64, "nullable": False}})
        def f(df: pl.DataFrame) -> pl.DataFrame:
            return df

        df = pl.DataFrame({"price": [1.0, None, 3.0]})
        with pytest.raises(AssertionError, match="null value"):
            f(df)

    def test_dtype_only_rich_spec_pandas(self) -> None:
        @df_in(columns={"price": {"dtype": "float64"}})
        def f(df: pd.DataFrame) -> pd.DataFrame:
            return df

        # Should still validate dtype
        df = pd.DataFrame({"price": [1.0, 2.0, 3.0]})
        result = f(df)
        assert len(result) == 3

    def test_dtype_mismatch_rich_spec(self) -> None:
        @df_in(columns={"price": {"dtype": "int64"}})
        def f(df: pd.DataFrame) -> pd.DataFrame:
            return df

        df = pd.DataFrame({"price": [1.0, 2.0, 3.0]})  # float64, not int64
        with pytest.raises(AssertionError, match="wrong dtype"):
            f(df)


class TestNullableDefault:
    def test_nullable_default_allows_nulls_pandas(self) -> None:
        # When nullable is not specified, it defaults to True (allow nulls)
        @df_in(columns={"price": {"dtype": "float64"}})
        def f(df: pd.DataFrame) -> pd.DataFrame:
            return df

        df = pd.DataFrame({"price": [1.0, None, 3.0]})
        result = f(df)  # Should NOT raise
        assert len(result) == 3

    def test_nullable_default_allows_nulls_polars(self) -> None:
        @df_in(columns={"price": {"dtype": pl.Float64}})
        def f(df: pl.DataFrame) -> pl.DataFrame:
            return df

        df = pl.DataFrame({"price": [1.0, None, 3.0]})
        result = f(df)  # Should NOT raise
        assert len(result) == 3

    def test_nullable_true_explicit_pandas(self) -> None:
        @df_in(columns={"price": {"nullable": True}})
        def f(df: pd.DataFrame) -> pd.DataFrame:
            return df

        df = pd.DataFrame({"price": [1.0, None, 3.0]})
        result = f(df)  # Should NOT raise
        assert len(result) == 3

    def test_nullable_true_explicit_polars(self) -> None:
        @df_in(columns={"price": {"nullable": True}})
        def f(df: pl.DataFrame) -> pl.DataFrame:
            return df

        df = pl.DataFrame({"price": [1.0, None, 3.0]})
        result = f(df)  # Should NOT raise
        assert len(result) == 3
