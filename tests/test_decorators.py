import logging
from typing import Any
from unittest.mock import call

import pandas as pd
import polars as pl
import pytest
from pytest_mock import MockerFixture

from daffy import df_in, df_log, df_out

cars = {
    "Brand": ["Honda Civic", "Toyota Corolla", "Ford Focus", "Audi A4"],
    "Price": [22000, 25000, 27000, 35000],
}


@pytest.fixture
def basic_df() -> pd.DataFrame:
    return pd.DataFrame(cars)


@pytest.fixture
def basic_polars_df() -> pd.DataFrame:
    return pl.DataFrame(cars)


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


def test_correct_return_type_and_no_column_constraints(basic_df: pd.DataFrame) -> None:
    @df_out()
    def test_fn() -> pd.DataFrame:
        return basic_df

    test_fn()


def test_correct_return_type_and_columns(basic_df: pd.DataFrame) -> None:
    @df_out(columns=["Brand", "Price"])
    def test_fn() -> pd.DataFrame:
        return basic_df

    test_fn()


def test_correct_return_type_and_columns_with_polars(basic_polars_df: pl.DataFrame) -> None:
    @df_out(columns=["Brand", "Price"])
    def test_fn() -> pl.DataFrame:
        return basic_polars_df

    test_fn()


def test_allow_extra_columns_out(basic_df: pd.DataFrame) -> None:
    @df_out(columns=["Brand"])
    def test_fn() -> pd.DataFrame:
        return basic_df

    test_fn()


def test_correct_return_type_and_columns_strict(basic_df: pd.DataFrame) -> None:
    @df_out(columns=["Brand", "Price"], strict=True)
    def test_fn() -> pd.DataFrame:
        return basic_df

    test_fn()


def test_extra_column_in_return_strict(basic_df: pd.DataFrame) -> None:
    @df_out(columns=["Brand"], strict=True)
    def test_fn() -> pd.DataFrame:
        return basic_df

    with pytest.raises(AssertionError) as excinfo:
        test_fn()

    assert "DataFrame contained unexpected column(s): Price" in str(excinfo.value)


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

    assert "Wrong parameter type. Expected DataFrame, got str instead." in str(excinfo.value)


def test_correct_input_with_columns(basic_df: pd.DataFrame) -> None:
    @df_in(columns=["Brand", "Price"])
    def test_fn(my_input: Any) -> Any:
        return my_input

    test_fn(basic_df)


def test_correct_input_with_no_column_constraints(basic_df: pd.DataFrame) -> None:
    @df_in()
    def test_fn(my_input: Any) -> Any:
        return my_input

    test_fn(basic_df)


def test_dfin_with_no_inputs() -> None:
    @df_in()
    def test_fn() -> Any:
        return

    with pytest.raises(AssertionError) as excinfo:
        test_fn()

    assert "Wrong parameter type. Expected DataFrame, got NoneType instead." in str(excinfo.value)


def test_correct_named_input_with_columns(basic_df: pd.DataFrame) -> None:
    @df_in(name="df", columns=["Brand", "Price"])
    def test_fn(my_input: Any, df: pd.DataFrame) -> pd.DataFrame:
        return df

    test_fn("foo", df=basic_df)


def test_correct_named_input_with_columns_strict(basic_df: pd.DataFrame) -> None:
    @df_in(name="df", columns=["Brand", "Price"], strict=True)
    def test_fn(my_input: Any, df: pd.DataFrame) -> pd.DataFrame:
        return df

    test_fn("foo", df=basic_df)


def test_in_allow_extra_columns(basic_df: pd.DataFrame) -> None:
    @df_in(name="df", columns=["Brand"])
    def test_fn(my_input: Any, df: pd.DataFrame) -> pd.DataFrame:
        return df

    test_fn("foo", df=basic_df)


def test_in_strict_extra_columns(basic_df: pd.DataFrame) -> None:
    @df_in(name="df", columns=["Brand"], strict=True)
    def test_fn(my_input: Any, df: pd.DataFrame) -> pd.DataFrame:
        return df

    with pytest.raises(AssertionError) as excinfo:
        test_fn("foo", df=basic_df)

    assert "DataFrame contained unexpected column(s): Price" in str(excinfo.value)


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

    assert "Column Price has wrong dtype. Was int64, expected float64" in str(excinfo.value)


def test_df_in_incorrect_input(basic_df: pd.DataFrame) -> None:
    @df_in(columns=["Brand", "Price"])
    def test_fn(my_input: Any) -> Any:
        return my_input

    with pytest.raises(AssertionError) as excinfo:
        test_fn(basic_df[["Brand"]])
    assert "Column Price missing" in str(excinfo.value)


def test_df_out_with_df_modification(basic_df: pd.DataFrame, extended_df: pd.DataFrame) -> None:
    @df_out(columns=["Brand", "Price", "Year"])
    def test_fn(my_input: Any) -> Any:
        my_input["Year"] = list(extended_df["Year"])
        return my_input

    assert list(basic_df.columns) == ["Brand", "Price"]  # For sanity
    pd.testing.assert_frame_equal(extended_df, test_fn(basic_df.copy()))


def test_decorator_combinations(basic_df: pd.DataFrame, extended_df: pd.DataFrame) -> None:
    @df_in(columns=["Brand", "Price"])
    @df_out(columns=["Brand", "Price", "Year"])
    def test_fn(my_input: Any) -> Any:
        my_input["Year"] = list(extended_df["Year"])
        return my_input

    pd.testing.assert_frame_equal(extended_df, test_fn(basic_df.copy()))


def test_multiple_named_inputs_with_names_in_function_call(basic_df: pd.DataFrame, extended_df: pd.DataFrame) -> None:
    @df_in(name="cars", columns=["Brand", "Price"], strict=True)
    @df_in(name="ext_cars", columns=["Brand", "Price", "Year"], strict=True)
    def test_fn(cars: pd.DataFrame, ext_cars: pd.DataFrame) -> int:
        return len(cars) + len(ext_cars)

    test_fn(cars=basic_df, ext_cars=extended_df)


def test_multiple_named_inputs_without_names_in_function_call(
    basic_df: pd.DataFrame, extended_df: pd.DataFrame
) -> None:
    @df_in(name="cars", columns=["Brand", "Price"], strict=True)
    @df_in(name="ext_cars", columns=["Brand", "Price", "Year"], strict=True)
    def test_fn(cars: pd.DataFrame, ext_cars: pd.DataFrame) -> int:
        return len(cars) + len(ext_cars)

    test_fn(basic_df, extended_df)


def test_multiple_named_inputs_with_some_of_names_in_function_call(
    basic_df: pd.DataFrame, extended_df: pd.DataFrame
) -> None:
    @df_in(name="cars", columns=["Brand", "Price"], strict=True)
    @df_in(name="ext_cars", columns=["Brand", "Price", "Year"], strict=True)
    def test_fn(cars: pd.DataFrame, ext_cars: pd.DataFrame) -> int:
        return len(cars) + len(ext_cars)

    test_fn(basic_df, ext_cars=extended_df)


def test_log_df(basic_df: pd.DataFrame, mocker: MockerFixture) -> None:
    @df_log()
    def test_fn(foo_df: pd.DataFrame) -> pd.DataFrame:
        return basic_df

    mock_log = mocker.patch("daffy.decorators.logging.log")
    test_fn(basic_df)

    mock_log.assert_has_calls(
        [
            call(
                logging.DEBUG,
                ("Function test_fn parameters contained a DataFrame: columns: ['Brand', 'Price']"),
            ),
            call(
                logging.DEBUG,
                "Function test_fn returned a DataFrame: columns: ['Brand', 'Price']",
            ),
        ]
    )


def test_log_df_with_dtypes(basic_df: pd.DataFrame, mocker: MockerFixture) -> None:
    @df_log(include_dtypes=True)
    def test_fn(foo_df: pd.DataFrame) -> pd.DataFrame:
        return basic_df

    mock_log = mocker.patch("daffy.decorators.logging.log")
    test_fn(basic_df)

    mock_log.assert_has_calls(
        [
            call(
                logging.DEBUG,
                (
                    "Function test_fn parameters contained a DataFrame: "
                    "columns: ['Brand', 'Price'] with dtypes ['object', 'int64']"
                ),
            ),
            call(
                logging.DEBUG,
                "Function test_fn returned a DataFrame: columns: ['Brand', 'Price'] with dtypes ['object', 'int64']",
            ),
        ]
    )


def test_log_non_df(mocker: MockerFixture) -> None:
    @df_log()
    def test_fn(foo: str) -> int:
        return 123

    mock_log = mocker.patch("daffy.decorators.logging.log")
    test_fn("foo")

    mock_log.assert_not_called()
