"""Tests for nullable column validation."""

from typing import Any

import pandas as pd
import polars as pl
import pytest

from daffy import df_in, df_out
from tests.utils import DataFrameFactory


class TestNullableBasic:
    def test_nullable_false_rejects_nulls(self, make_df: DataFrameFactory) -> None:
        @df_in(columns={"price": {"nullable": False}})
        def f(df: Any) -> Any:
            return df

        df = make_df({"price": [1.0, None, 3.0]})
        with pytest.raises(AssertionError, match="null value"):
            f(df)

    def test_nullable_false_passes_without_nulls(self, make_df: DataFrameFactory) -> None:
        @df_in(columns={"price": {"nullable": False}})
        def f(df: Any) -> Any:
            return df

        df = make_df({"price": [1.0, 2.0, 3.0]})
        result = f(df)
        assert len(result) == 3


class TestNullableWithDtype:
    def test_nullable_with_dtype_pandas(self) -> None:
        @df_in(columns={"price": {"dtype": "float64", "nullable": False}})
        def f(df: pd.DataFrame) -> pd.DataFrame:
            return df

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
    def test_nullable_true_explicit_allows_nulls(self, make_df: DataFrameFactory) -> None:
        @df_in(columns={"price": {"nullable": True}})
        def f(df: Any) -> Any:
            return df

        df = make_df({"price": [1.0, None, 3.0]})
        result = f(df)
        assert len(result) == 3


class TestNullableDefaultWithDtype:
    def test_nullable_default_allows_nulls_pandas(self) -> None:
        @df_in(columns={"price": {"dtype": "float64"}})
        def f(df: pd.DataFrame) -> pd.DataFrame:
            return df

        df = pd.DataFrame({"price": [1.0, None, 3.0]})
        result = f(df)
        assert len(result) == 3

    def test_nullable_default_allows_nulls_polars(self) -> None:
        @df_in(columns={"price": {"dtype": pl.Float64}})
        def f(df: pl.DataFrame) -> pl.DataFrame:
            return df

        df = pl.DataFrame({"price": [1.0, None, 3.0]})
        result = f(df)
        assert len(result) == 3


class TestNullableWithRegex:
    def test_regex_pattern_nullable_false(self, make_df: DataFrameFactory) -> None:
        @df_in(columns={"r/Price_\\d+/": {"nullable": False}})
        def f(df: Any) -> Any:
            return df

        df = make_df({"Price_1": [1.0, None], "Price_2": [2.0, 3.0]})
        with pytest.raises(AssertionError, match="Price_1"):
            f(df)

    def test_regex_pattern_nullable_passes(self, make_df: DataFrameFactory) -> None:
        @df_in(columns={"r/Price_\\d+/": {"nullable": False}})
        def f(df: Any) -> Any:
            return df

        df = make_df({"Price_1": [1.0, 2.0], "Price_2": [2.0, 3.0]})
        result = f(df)
        assert len(result) == 2


class TestNullableWithRegexPandasSpecific:
    def test_regex_pattern_all_columns_checked(self) -> None:
        @df_in(columns={"r/Price_\\d+/": {"nullable": False}})
        def f(df: pd.DataFrame) -> pd.DataFrame:
            return df

        df = pd.DataFrame({"Price_1": [1.0, None], "Price_2": [None, 3.0]})
        with pytest.raises(AssertionError, match="Null violations"):
            f(df)

    def test_regex_with_dtype_and_nullable(self) -> None:
        @df_in(columns={"r/Price_\\d+/": {"dtype": "float64", "nullable": False}})
        def f(df: pd.DataFrame) -> pd.DataFrame:
            return df

        df = pd.DataFrame({"Price_1": [1.0, None], "Price_2": [2.0, 3.0]})
        with pytest.raises(AssertionError, match="null value"):
            f(df)


class TestNullableDfOut:
    def test_df_out_nullable_false_rejects_nulls(self, make_df: DataFrameFactory) -> None:
        @df_out(columns={"price": {"nullable": False}})
        def f() -> Any:
            return make_df({"price": [1.0, None, 3.0]})

        with pytest.raises(AssertionError, match="return value"):
            f()

    def test_df_out_nullable_passes(self, make_df: DataFrameFactory) -> None:
        @df_out(columns={"price": {"nullable": False}})
        def f() -> Any:
            return make_df({"price": [1.0, 2.0, 3.0]})

        result = f()
        assert len(result) == 3


class TestNullableDfOutPandasSpecific:
    def test_df_out_with_dtype_and_nullable(self) -> None:
        @df_out(columns={"price": {"dtype": "float64", "nullable": False}})
        def f() -> pd.DataFrame:
            return pd.DataFrame({"price": [1.0, None, 3.0]})

        with pytest.raises(AssertionError, match="null value"):
            f()


class TestBackwardsCompatibility:
    def test_list_columns_spec(self, make_df: DataFrameFactory) -> None:
        @df_in(columns=["price", "qty"])
        def f(df: Any) -> Any:
            return df

        df = make_df({"price": [1.0], "qty": [1]})
        result = f(df)
        assert len(result) == 1


class TestBackwardsCompatibilityPandasSpecific:
    def test_string_dtype_spec_pandas(self) -> None:
        @df_in(columns={"price": "float64"})
        def f(df: pd.DataFrame) -> pd.DataFrame:
            return df

        df = pd.DataFrame({"price": [1.0, 2.0, 3.0]})
        result = f(df)
        assert len(result) == 3

    def test_string_dtype_spec_polars(self) -> None:
        @df_in(columns={"price": pl.Float64})
        def f(df: pl.DataFrame) -> pl.DataFrame:
            return df

        df = pl.DataFrame({"price": [1.0, 2.0, 3.0]})
        result = f(df)
        assert len(result) == 3

    def test_string_dtype_with_nulls_allowed(self) -> None:
        @df_in(columns={"price": "float64"})
        def f(df: pd.DataFrame) -> pd.DataFrame:
            return df

        df = pd.DataFrame({"price": [1.0, None, 3.0]})
        result = f(df)
        assert len(result) == 3

    def test_list_columns_with_nulls_allowed(self) -> None:
        @df_in(columns=["price"])
        def f(df: pd.DataFrame) -> pd.DataFrame:
            return df

        df = pd.DataFrame({"price": [1.0, None, 3.0]})
        result = f(df)
        assert len(result) == 3

    def test_regex_dtype_spec(self) -> None:
        @df_in(columns={"r/Price_\\d+/": "float64"})
        def f(df: pd.DataFrame) -> pd.DataFrame:
            return df

        df = pd.DataFrame({"Price_1": [1.0, 2.0], "Price_2": [3.0, 4.0]})
        result = f(df)
        assert len(result) == 2

    def test_strict_mode_with_rich_spec(self) -> None:
        @df_in(columns={"price": {"dtype": "float64"}}, strict=True)
        def f(df: pd.DataFrame) -> pd.DataFrame:
            return df

        df = pd.DataFrame({"price": [1.0, 2.0], "extra": [1, 2]})
        with pytest.raises(AssertionError, match="unexpected column"):
            f(df)
