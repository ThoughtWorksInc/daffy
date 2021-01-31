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


def _get_parameter(name=None, *args, **kwargs):
    if not name:
        return args[0]
    return kwargs[name]


def df_in(func, name=None, columns=None):
    def wrapper_df_out(*args, **kwargs):
        df = _get_parameter(name, *args, **kwargs)
        assert isinstance(
            df, pd.DataFrame
        ), f"Wrong parameter type. Expected pandas dataframe, got {type(df)}"
        if columns:
            _check_columns(df, columns)
        return func(*args, **kwargs)

    return wrapper_df_out