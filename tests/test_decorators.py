import pandas as pd

from daffy import df_in, df_out


def test_decorator_combinations(basic_pandas_df: pd.DataFrame, extended_pandas_df: pd.DataFrame) -> None:
    @df_in(columns=["Brand", "Price"])
    @df_out(columns=["Brand", "Price", "Year"])
    def test_fn(my_input: pd.DataFrame) -> pd.DataFrame:
        my_input["Year"] = list(extended_pandas_df["Year"])
        return my_input

    result = test_fn(basic_pandas_df.copy())
    assert isinstance(result, pd.DataFrame)  # Confirm the type for mypy
    pd.testing.assert_frame_equal(extended_pandas_df, result)
