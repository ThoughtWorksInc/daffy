import pandas as pd
import pytest

from daffy.decorators import df_out


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


def test_correct_return_type(basic_df):
    def test_fn():
        return basic_df

    wrapped_test_fn = df_out(test_fn)
    wrapped_test_fn()
