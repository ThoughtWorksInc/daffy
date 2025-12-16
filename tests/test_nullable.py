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
