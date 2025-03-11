from typing import Any

import pandas as pd
import polars as pl
import pytest

from daffy import df_in
from tests.conftest import DataFrameType, cars, extended_cars


def test_wrong_input_type_unnamed() -> None:
    @df_in()
    def test_fn(my_input: Any) -> Any:
        return my_input

    with pytest.raises(AssertionError) as excinfo:
        test_fn("foobar")

    assert "Wrong parameter type" in str(excinfo.value)


def test_wrong_input_type_named() -> None:
    @df_in(name="my_input")
    def test_fn(my_input: Any) -> Any:
        return my_input

    with pytest.raises(AssertionError) as excinfo:
        test_fn(my_input="foobar")

    assert "Wrong parameter type. Expected DataFrame, got str instead." in str(excinfo.value)


@pytest.mark.parametrize(("df"), [pd.DataFrame(cars), pl.DataFrame(cars)])
def test_correct_input_with_columns(df: DataFrameType) -> None:
    @df_in(columns=["Brand", "Price"])
    def test_fn(my_input: Any) -> Any:
        return my_input

    test_fn(df)


@pytest.mark.parametrize(("df"), [pd.DataFrame(cars), pl.DataFrame(cars)])
def test_correct_input_with_no_column_constraints(df: DataFrameType) -> None:
    @df_in()
    def test_fn(my_input: Any) -> Any:
        return my_input

    test_fn(df)


def test_dfin_with_no_inputs() -> None:
    @df_in()
    def test_fn() -> Any:
        return

    with pytest.raises(AssertionError) as excinfo:
        test_fn()

    assert "Wrong parameter type. Expected DataFrame, got NoneType instead." in str(excinfo.value)


@pytest.mark.parametrize(("df"), [pd.DataFrame(cars), pl.DataFrame(cars)])
def test_correct_named_input_with_columns(df: DataFrameType) -> None:
    @df_in(name="_df", columns=["Brand", "Price"])
    def test_fn(my_input: Any, _df: DataFrameType) -> DataFrameType:
        return _df

    test_fn("foo", _df=df)


@pytest.mark.parametrize(("df"), [pd.DataFrame(cars), pl.DataFrame(cars)])
def test_correct_named_input_with_columns_strict(df: DataFrameType) -> None:
    @df_in(name="_df", columns=["Brand", "Price"], strict=True)
    def test_fn(my_input: Any, _df: DataFrameType) -> DataFrameType:
        return _df

    test_fn("foo", _df=df)


@pytest.mark.parametrize(("df"), [pd.DataFrame(cars), pl.DataFrame(cars)])
def test_correct_named_input_with_columns_strict_no_name(df: DataFrameType) -> None:
    @df_in(columns=["Brand", "Price"], strict=True)
    def test_fn(_df: DataFrameType) -> DataFrameType:
        return _df

    test_fn(_df=df)


@pytest.mark.parametrize(("df"), [pd.DataFrame(cars), pl.DataFrame(cars)])
def test_in_allow_extra_columns(df: DataFrameType) -> None:
    @df_in(name="_df", columns=["Brand"])
    def test_fn(my_input: Any, _df: DataFrameType) -> DataFrameType:
        return _df

    test_fn("foo", _df=df)


@pytest.mark.parametrize(("df"), [pd.DataFrame(cars), pl.DataFrame(cars)])
def test_in_strict_extra_columns(df: DataFrameType) -> None:
    @df_in(name="_df", columns=["Brand"], strict=True)
    def test_fn(my_input: Any, _df: DataFrameType) -> DataFrameType:
        return _df

    with pytest.raises(AssertionError) as excinfo:
        test_fn("foo", _df=df)

    assert "DataFrame contained unexpected column(s): Price" in str(excinfo.value)


def test_correct_input_with_columns_and_dtypes_pandas(basic_pandas_df: pd.DataFrame) -> None:
    @df_in(columns={"Brand": "object", "Price": "int64"})
    def test_fn(my_input: Any) -> Any:
        return my_input

    test_fn(basic_pandas_df)


def test_correct_input_with_columns_and_dtypes_polars(basic_polars_df: pl.DataFrame) -> None:
    @df_in(columns={"Brand": pl.datatypes.String, "Price": pl.datatypes.Int64})
    def test_fn(my_input: Any) -> Any:
        return my_input

    test_fn(basic_polars_df)


def test_dtype_mismatch_pandas(basic_pandas_df: pd.DataFrame) -> None:
    @df_in(columns={"Brand": "object", "Price": "float64"})
    def test_fn(my_input: Any) -> Any:
        return my_input

    with pytest.raises(AssertionError) as excinfo:
        test_fn(basic_pandas_df)

    assert "Column Price has wrong dtype. Was int64, expected float64" in str(excinfo.value)


def test_dtype_mismatch_polars(basic_polars_df: pl.DataFrame) -> None:
    @df_in(columns={"Brand": pl.datatypes.String, "Price": pl.datatypes.Float64})
    def test_fn(my_input: Any) -> Any:
        return my_input

    with pytest.raises(AssertionError) as excinfo:
        test_fn(basic_polars_df)

    assert "Column Price has wrong dtype. Was Int64, expected Float64" in str(excinfo.value)


@pytest.mark.parametrize(("df"), [pd.DataFrame(cars), pl.DataFrame(cars)])
def test_df_in_missing_column(df: DataFrameType) -> None:
    @df_in(columns=["Brand", "Price"])
    def test_fn(my_input: Any) -> Any:
        return my_input

    with pytest.raises(AssertionError) as excinfo:
        test_fn(df[["Brand"]])
    assert "Missing columns: ['Price']. Got columns: ['Brand']" in str(excinfo.value)


@pytest.mark.parametrize(("df"), [pd.DataFrame(cars), pl.DataFrame(cars)])
def test_df_in_missing_multiple_columns(df: DataFrameType) -> None:
    @df_in(columns=["Brand", "Price", "Extra"])
    def test_fn(my_input: Any) -> Any:
        return my_input

    with pytest.raises(AssertionError) as excinfo:
        test_fn(df[["Brand"]])
    assert "Missing columns: ['Price', 'Extra']. Got columns: ['Brand']" in str(excinfo.value)


@pytest.mark.parametrize(
    ("basic_df,extended_df"),
    [(pd.DataFrame(cars), pd.DataFrame(extended_cars)), (pl.DataFrame(cars), pl.DataFrame(extended_cars))],
)
def test_multiple_named_inputs_with_names_in_function_call(basic_df: DataFrameType, extended_df: DataFrameType) -> None:
    @df_in(name="cars", columns=["Brand", "Price"], strict=True)
    @df_in(name="ext_cars", columns=["Brand", "Price", "Year"], strict=True)
    def test_fn(cars: DataFrameType, ext_cars: DataFrameType) -> int:
        return len(cars) + len(ext_cars)

    test_fn(cars=basic_df, ext_cars=extended_df)


@pytest.mark.parametrize(
    ("basic_df,extended_df"),
    [(pd.DataFrame(cars), pd.DataFrame(extended_cars)), (pl.DataFrame(cars), pl.DataFrame(extended_cars))],
)
def test_multiple_named_inputs_without_names_in_function_call(
    basic_df: DataFrameType, extended_df: DataFrameType
) -> None:
    @df_in(name="cars", columns=["Brand", "Price"], strict=True)
    @df_in(name="ext_cars", columns=["Brand", "Price", "Year"], strict=True)
    def test_fn(cars: pd.DataFrame, ext_cars: pd.DataFrame) -> int:
        return len(cars) + len(ext_cars)

    test_fn(basic_df, extended_df)


@pytest.mark.parametrize(
    ("basic_df,extended_df"),
    [(pd.DataFrame(cars), pd.DataFrame(extended_cars)), (pl.DataFrame(cars), pl.DataFrame(extended_cars))],
)
def test_multiple_named_inputs_with_some_of_names_in_function_call(
    basic_df: DataFrameType, extended_df: DataFrameType
) -> None:
    @df_in(name="cars", columns=["Brand", "Price"], strict=True)
    @df_in(name="ext_cars", columns=["Brand", "Price", "Year"], strict=True)
    def test_fn(cars: DataFrameType, ext_cars: DataFrameType) -> int:
        return len(cars) + len(ext_cars)

    test_fn(basic_df, ext_cars=extended_df)


def test_regex_column_pattern(basic_pandas_df: pd.DataFrame) -> None:
    # Create a DataFrame with numbered price columns
    df = basic_pandas_df.copy()
    df["Price_1"] = df["Price"] * 1
    df["Price_2"] = df["Price"] * 2
    df["Price_3"] = df["Price"] * 3

    @df_in(columns=["Brand", "r/Price_[0-9]/"])
    def test_fn(my_input: Any) -> Any:
        return my_input

    # This should pass since we have Price_1, Price_2, and Price_3 columns
    result = test_fn(df)
    assert "Price_1" in result.columns
    assert "Price_2" in result.columns
    assert "Price_3" in result.columns


def test_regex_column_pattern_missing(basic_pandas_df: pd.DataFrame) -> None:
    @df_in(columns=["Brand", "r/NonExistent_[0-9]/"])
    def test_fn(my_input: Any) -> Any:
        return my_input

    # This should fail since we don't have any columns matching the pattern
    with pytest.raises(AssertionError) as excinfo:
        test_fn(basic_pandas_df)

    assert "Missing columns: ['r/NonExistent_[0-9]/']" in str(excinfo.value)


def test_regex_column_pattern_with_strict(basic_pandas_df: pd.DataFrame) -> None:
    # Create a DataFrame with numbered price columns
    df = basic_pandas_df.copy()
    df["Price_1"] = df["Price"] * 1
    df["Price_2"] = df["Price"] * 2

    @df_in(columns=["Brand", "r/Price_[0-9]/"], strict=True)
    def test_fn(my_input: Any) -> Any:
        return my_input

    # This should pass, because "Price" is unexpected but "Price_1" and "Price_2" match the regex
    with pytest.raises(AssertionError) as excinfo:
        test_fn(df)

    assert "DataFrame contained unexpected column(s): Price" in str(excinfo.value)


def test_regex_column_with_dtype_pandas(basic_pandas_df: pd.DataFrame) -> None:
    # Create a DataFrame with numbered price columns
    df = basic_pandas_df.copy()
    df["Price_1"] = df["Price"] * 1
    df["Price_2"] = df["Price"] * 2

    @df_in(columns={"Brand": "object", "r/Price_[0-9]/": "int64"})
    def test_fn(my_input: Any) -> Any:
        return my_input

    # This should pass since Price_1 and Price_2 are int64
    result = test_fn(df)
    assert "Price_1" in result.columns
    assert "Price_2" in result.columns


def test_regex_column_with_dtype_mismatch_pandas(basic_pandas_df: pd.DataFrame) -> None:
    # Create a DataFrame with numbered price columns
    df = basic_pandas_df.copy()
    df["Price_1"] = df["Price"] * 1
    df["Price_2"] = df["Price"] * 2.0  # Make this a float

    @df_in(columns={"Brand": "object", "r/Price_[0-9]/": "int64"})
    def test_fn(my_input: Any) -> Any:
        return my_input

    # This should fail since Price_2 is float64, not int64
    with pytest.raises(AssertionError) as excinfo:
        test_fn(df)

    assert "Column Price_2 has wrong dtype. Was float64, expected int64" in str(excinfo.value)


def test_regex_column_with_dtype_polars(basic_polars_df: pl.DataFrame) -> None:
    # Create a DataFrame with numbered price columns
    # Polars DataFrames are immutable, so we don't need to copy
    df = basic_polars_df.with_columns([pl.col("Price").alias("Price_1"), pl.col("Price").alias("Price_2")])

    @df_in(columns={"Brand": pl.datatypes.String, "r/Price_[0-9]/": pl.datatypes.Int64})
    def test_fn(my_input: Any) -> Any:
        return my_input

    # This should pass since Price_1 and Price_2 are Int64
    result = test_fn(df)
    assert "Price_1" in result.columns
    assert "Price_2" in result.columns
