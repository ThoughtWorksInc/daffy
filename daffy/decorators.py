import pandas as pd


def _check_columns(df, columns):
    for column in columns:
        assert column in df.columns, f"Column {column} missing from DataFrame"


def df_out(func, columns=None):
    def wrapper_df_out(*args, **kwargs):
        result = func(*args, **kwargs)
        assert isinstance(
            result, pd.DataFrame
        ), f"Wrong return type. Expected pandas dataframe, got {type(result)}"
        if columns:
            _check_columns(result, columns)

    return wrapper_df_out
