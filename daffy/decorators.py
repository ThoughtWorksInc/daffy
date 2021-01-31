import pandas as pd


def df_out(func):
    def wrapper_df_out(*args, **kwargs):
        result = func(*args, **kwargs)
        assert isinstance(
            result, pd.DataFrame
        ), f"Wrong return type. Expected pandas dataframe, got {type(result)}"

    return wrapper_df_out
