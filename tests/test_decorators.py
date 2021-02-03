from typing import Any

import pandas as pd
import pytest

from daffy import df_in, df_out


@pytest.fixture
def basic_df() -> pd.DataFrame:
    cars = {
        "Brand": ["Honda Civic", "Toyota Corolla", "Ford Focus", "Audi A4"],
        "Price": [22000, 25000, 27000, 35000],
    }
    return pd.DataFrame(cars, columns=["Brand", "Price"])


@pytest.fixture
def extended_df() -> pd.DataFrame:
    cars = {
        "Brand": ["Honda Civic", "Toyota Corolla", "Ford Focus", "Audi A4"],
        "Price": [22000, 25000, 27000, 35000],
        "Year": [2020, 1998, 2001, 2021],
    }
    return pd.DataFrame(cars, columns=["Brand", "Price", "Year"])


def test_wrong_return_type() -> None:
    @df_out()
    def test_fn() -> int:
        return 1

    with pytest.raises(AssertionError) as excinfo:
        test_fn()

    assert "Wrong return type" in str(excinfo.value)


def test_correct_return_type_and_columns(basic_df: pd.DataFrame) -> None:
    @df_out(columns=["Brand", "Price"])
    def test_fn() -> pd.DataFrame:
        return basic_df

    test_fn()


def test_missing_column_in_return(basic_df: pd.DataFrame) -> None:
    @df_out(columns=["Brand", "FooColumn"])
    def test_fn() -> pd.DataFrame:
        return basic_df

    with pytest.raises(AssertionError) as excinfo:
        test_fn()

    assert "Column FooColumn missing" in str(excinfo.value)


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

    assert "Wrong parameter type" in str(excinfo.value)


def test_correct_input_with_columns(basic_df: pd.DataFrame) -> None:
    @df_in(columns=["Brand", "Price"])
    def test_fn(my_input: Any) -> Any:
        return my_input

    test_fn(basic_df)


def test_correct_named_input_with_columns(basic_df: pd.DataFrame) -> None:
    @df_in(name="df", columns=["Brand", "Price"])
    def test_fn(my_input: Any, df: pd.DataFrame) -> pd.DataFrame:
        return df

    test_fn("foo", df=basic_df)


def test_correct_input_with_columns_and_dtypes(basic_df: pd.DataFrame) -> None:
    @df_in(columns={"Brand": "object", "Price": "int64"})
    def test_fn(my_input: Any) -> Any:
        return my_input

    test_fn(basic_df)


def test_dtype_mismatch(basic_df: pd.DataFrame) -> None:
    @df_in(columns={"Brand": "object", "Price": "float64"})
    def test_fn(my_input: Any) -> Any:
        return my_input

    with pytest.raises(AssertionError) as excinfo:
        test_fn(basic_df)

    assert "Column Price has wrong dtype" in str(excinfo.value)


def test_df_in_incorrect_input(basic_df: pd.DataFrame) -> None:
    @df_in(columns=["Brand", "Price"])
    def test_fn(my_input: Any) -> Any:
        return my_input

    with pytest.raises(AssertionError) as excinfo:
        test_fn(basic_df[["Brand"]])
    assert "Column Price missing" in str(excinfo.value)


def test_df_out_with_df_modification(
    basic_df: pd.DataFrame, extended_df: pd.DataFrame
) -> None:
    @df_out(columns=["Brand", "Price", "Year"])
    def test_fn(my_input: Any) -> Any:
        my_input["Year"] = list(extended_df["Year"])
        return my_input

    assert list(basic_df.columns) == ["Brand", "Price"]  # For sanity
    pd.testing.assert_frame_equal(extended_df, test_fn(basic_df.copy()))


def test_decorator_combinations(
    basic_df: pd.DataFrame, extended_df: pd.DataFrame
) -> None:
    @df_in(columns=["Brand", "Price"])
    @df_out(columns=["Brand", "Price", "Year"])
    def test_fn(my_input: Any) -> Any:
        my_input["Year"] = list(extended_df["Year"])
        return my_input

    pd.testing.assert_frame_equal(extended_df, test_fn(basic_df.copy()))
