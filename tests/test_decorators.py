import pandas as pd
import pytest

from daffy import df_in, df_out


@pytest.fixture
def basic_df():
    cars = {
        "Brand": ["Honda Civic", "Toyota Corolla", "Ford Focus", "Audi A4"],
        "Price": [22000, 25000, 27000, 35000],
    }
    return pd.DataFrame(cars, columns=["Brand", "Price"])


def test_wrong_return_type():
    def test_fn():
        return 1

    wrapped_test_fn = df_out(test_fn)

    with pytest.raises(AssertionError) as excinfo:
        wrapped_test_fn()

    assert "Wrong return type" in str(excinfo.value)


def test_correct_return_type_and_columns(basic_df):
    def test_fn():
        return basic_df

    wrapped_test_fn = df_out(test_fn, columns=["Brand", "Price"])
    wrapped_test_fn()


def test_missing_column_in_return(basic_df):
    def test_fn():
        return basic_df

    wrapped_test_fn = df_out(test_fn, columns=["Brand", "FooColumn"])

    with pytest.raises(AssertionError) as excinfo:
        wrapped_test_fn()

    assert "Column FooColumn missing" in str(excinfo.value)


def test_wrong_input_type_unnamed():
    def test_fn(my_input):
        return my_input

    wrapped_test_fn = df_in(test_fn)

    with pytest.raises(AssertionError) as excinfo:
        wrapped_test_fn("foobar")

    assert "Wrong parameter type" in str(excinfo.value)


def test_wrong_input_type_named():
    def test_fn(my_input):
        return my_input

    wrapped_test_fn = df_in(test_fn, name="my_input")

    with pytest.raises(AssertionError) as excinfo:
        wrapped_test_fn(my_input="foobar")

    assert "Wrong parameter type" in str(excinfo.value)
