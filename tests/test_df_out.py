from typing import Any

import pandas as pd
import polars as pl
import pytest

from daffy import df_out
from tests.conftest import DataFrameType, cars


def test_wrong_return_type() -> None:
    @df_out()
    def test_fn() -> int:
        return 1

    with pytest.raises(AssertionError) as excinfo:
        test_fn()

    assert "Wrong return type" in str(excinfo.value)


@pytest.mark.parametrize(("df"), [pd.DataFrame(cars), pl.DataFrame(cars)])
def test_correct_return_type_and_no_column_constraints(df: DataFrameType) -> None:
    @df_out()
    def test_fn() -> DataFrameType:
        return df

    test_fn()


@pytest.mark.parametrize(("df"), [pd.DataFrame(cars), pl.DataFrame(cars)])
def test_correct_return_type_and_columns(df: DataFrameType) -> None:
    @df_out(columns=["Brand", "Price"])
    def test_fn() -> DataFrameType:
        return df

    test_fn()


@pytest.mark.parametrize(("df"), [pd.DataFrame(cars), pl.DataFrame(cars)])
def test_allow_extra_columns_out(df: DataFrameType) -> None:
    @df_out(columns=["Brand"])
    def test_fn() -> DataFrameType:
        return df

    test_fn()


@pytest.mark.parametrize(("df"), [pd.DataFrame(cars), pl.DataFrame(cars)])
def test_correct_return_type_and_columns_strict(df: DataFrameType) -> None:
    @df_out(columns=["Brand", "Price"], strict=True)
    def test_fn() -> DataFrameType:
        return df

    test_fn()


@pytest.mark.parametrize(("df"), [pd.DataFrame(cars), pl.DataFrame(cars)])
def test_extra_column_in_return_strict(df: DataFrameType) -> None:
    @df_out(columns=["Brand"], strict=True)
    def test_fn() -> DataFrameType:
        return df

    with pytest.raises(AssertionError) as excinfo:
        test_fn()

    assert "DataFrame contained unexpected column(s): Price" in str(excinfo.value)


@pytest.mark.parametrize(("df"), [pd.DataFrame(cars), pl.DataFrame(cars)])
def test_missing_column_in_return(df: DataFrameType) -> None:
    @df_out(columns=["Brand", "FooColumn"])
    def test_fn() -> DataFrameType:
        return df

    with pytest.raises(AssertionError) as excinfo:
        test_fn()

    assert "Missing columns: ['FooColumn']. Got columns: ['Brand', 'Price']" in str(excinfo.value)


def test_df_out_with_df_modification(basic_pandas_df: pd.DataFrame, extended_pandas_df: pd.DataFrame) -> None:
    @df_out(columns=["Brand", "Price", "Year"])
    def test_fn(my_input: Any) -> Any:
        my_input["Year"] = list(extended_pandas_df["Year"])
        return my_input

    assert list(basic_pandas_df.columns) == ["Brand", "Price"]  # For sanity
    pd.testing.assert_frame_equal(extended_pandas_df, test_fn(basic_pandas_df.copy()))
