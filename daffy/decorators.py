from functools import wraps

import pandas as pd


def _check_columns(df, columns):
    if isinstance(columns, list):
        for column in columns:
            assert column in df.columns, f"Column {column} missing from DataFrame"
    if isinstance(columns, dict):
        for column, dtype in columns.items():
            assert column in df.columns, f"Column {column} missing from DataFrame"
            assert df[column].dtype == dtype, (
                f"Column {column} has wrong dtype. "
                f"Was {df[column].dtype}, expected {dtype}"
            )


def df_out(columns=None):
    def wrapper_df_out(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            result = func(*args, **kwargs)
            assert isinstance(
                result, pd.DataFrame
            ), f"Wrong return type. Expected pandas dataframe, got {type(result)}"
            if columns:
                _check_columns(result, columns)
            return result

        return wrapper

    return wrapper_df_out


def _get_parameter(name=None, *args, **kwargs):
    if not name:
        return args[0]
    return kwargs[name]


def df_in(name=None, columns=None):
    def wrapper_df_out(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            df = _get_parameter(name, *args, **kwargs)
            assert isinstance(
                df, pd.DataFrame
            ), f"Wrong parameter type. Expected pandas dataframe, got {type(df)}"
            if columns:
                _check_columns(df, columns)
            return func(*args, **kwargs)

        return wrapper

    return wrapper_df_out
