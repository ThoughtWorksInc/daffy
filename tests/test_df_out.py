from typing import Any

import pandas as pd
import polars as pl
import pytest

from daffy import df_out
from tests.conftest import DataFrameType, cars


def test_wrong_return_type() -> None:
    @df_out()  # type: ignore[arg-type]
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
    result = test_fn(basic_pandas_df.copy())
    # Type check to ensure we get a pandas DataFrame before comparing
    assert isinstance(result, pd.DataFrame)
    pd.testing.assert_frame_equal(extended_pandas_df, result)


def test_regex_column_pattern_in_output(basic_pandas_df: pd.DataFrame) -> None:
    # Create a function that adds numbered price columns
    @df_out(columns=["Brand", "r/Price_[0-9]/"])
    def test_fn() -> pd.DataFrame:
        df = basic_pandas_df.copy()
        df["Price_1"] = df["Price"] * 1
        df["Price_2"] = df["Price"] * 2
        return df

    # This should pass since the output has Brand and Price_1, Price_2 columns
    result = test_fn()
    assert "Price_1" in result.columns
    assert "Price_2" in result.columns


def test_regex_column_pattern_missing_in_output(basic_pandas_df: pd.DataFrame) -> None:
    @df_out(columns=["Brand", "r/NonExistent_[0-9]/"])
    def test_fn() -> pd.DataFrame:
        return basic_pandas_df.copy()

    # This should fail since the output doesn't have columns matching the pattern
    with pytest.raises(AssertionError) as excinfo:
        test_fn()

    assert "Missing columns: ['r/NonExistent_[0-9]/']" in str(excinfo.value)


def test_regex_column_pattern_with_strict_in_output(basic_pandas_df: pd.DataFrame) -> None:
    @df_out(columns=["Brand", "r/Price_[0-9]/"], strict=True)
    def test_fn() -> pd.DataFrame:
        df = basic_pandas_df.copy()
        df["Price_1"] = df["Price"] * 1
        return df

    # This should raise an error because Price is unexpected
    with pytest.raises(AssertionError) as excinfo:
        test_fn()

    assert "DataFrame contained unexpected column(s): Price" in str(excinfo.value)


def test_regex_column_with_dtype_in_output_pandas(basic_pandas_df: pd.DataFrame) -> None:
    # Create a function that adds numbered price columns
    @df_out(columns={"Brand": "object", "r/Price_[0-9]/": "int64"})
    def test_fn() -> pd.DataFrame:
        df = basic_pandas_df.copy()
        df["Price_1"] = df["Price"] * 1
        df["Price_2"] = df["Price"] * 2
        return df

    # This should pass since Price_1 and Price_2 are int64
    result = test_fn()
    assert "Price_1" in result.columns
    assert "Price_2" in result.columns


def test_regex_column_with_dtype_mismatch_in_output_pandas(basic_pandas_df: pd.DataFrame) -> None:
    # Create a function that adds numbered price columns with one wrong dtype
    @df_out(columns={"Brand": "object", "r/Price_[0-9]/": "int64"})
    def test_fn() -> pd.DataFrame:
        df = basic_pandas_df.copy()
        df["Price_1"] = df["Price"] * 1
        df["Price_2"] = df["Price"] * 2.0  # Make this a float
        return df

    # This should fail since Price_2 is float64, not int64
    with pytest.raises(AssertionError) as excinfo:
        test_fn()

    assert "Column Price_2 has wrong dtype. Was float64, expected int64" in str(excinfo.value)


def test_regex_column_with_dtype_in_output_polars(basic_polars_df: pl.DataFrame) -> None:
    # Create a function that adds numbered price columns
    @df_out(columns={"Brand": pl.datatypes.String, "r/Price_[0-9]/": pl.datatypes.Int64})
    def test_fn() -> pl.DataFrame:
        # Polars DataFrames are immutable, so we build a new one
        return basic_polars_df.with_columns([pl.col("Price").alias("Price_1"), pl.col("Price").alias("Price_2")])

    # This should pass since Price_1 and Price_2 are Int64
    result = test_fn()
    assert "Price_1" in result.columns
    assert "Price_2" in result.columns


def test_regex_column_with_dtype_strict_in_output_pandas(basic_pandas_df: pd.DataFrame) -> None:
    # Create a function that adds numbered price columns
    @df_out(columns={"Brand": "object", "r/Price_[0-9]/": "int64"}, strict=True)
    def test_fn() -> pd.DataFrame:
        df = basic_pandas_df.copy()
        df["Price_1"] = df["Price"] * 1
        df["Price_2"] = df["Price"] * 2
        return df

    # This should fail because Price is unexpected
    with pytest.raises(AssertionError) as excinfo:
        test_fn()

    assert "DataFrame contained unexpected column(s): Price" in str(excinfo.value)
