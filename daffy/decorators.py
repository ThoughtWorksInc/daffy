from functools import wraps
from typing import Any, Callable, Dict, List, Optional, Union

import pandas as pd

ColumnsDef = Optional[Union[List, Dict]]


def _check_columns(df: pd.DataFrame, columns: ColumnsDef) -> None:
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


def df_out(columns: ColumnsDef = None) -> Callable:
    def wrapper_df_out(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args: str, **kwargs: Any) -> Any:
            result = func(*args, **kwargs)
            assert isinstance(
                result, pd.DataFrame
            ), f"Wrong return type. Expected pandas dataframe, got {type(result)}"
            if columns:
                _check_columns(result, columns)
            return result

        return wrapper

    return wrapper_df_out


def _get_parameter(
    name: Optional[str] = None, *args: str, **kwargs: Any
) -> pd.DataFrame:
    if not name:
        return args[0]
    return kwargs[name]


def df_in(name: Optional[str] = None, columns: ColumnsDef = None) -> Callable:
    def wrapper_df_out(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args: str, **kwargs: Any) -> Any:
            df = _get_parameter(name, *args, **kwargs)
            assert isinstance(
                df, pd.DataFrame
            ), f"Wrong parameter type. Expected pandas dataframe, got {type(df)}"
            if columns:
                _check_columns(df, columns)
            return func(*args, **kwargs)

        return wrapper

    return wrapper_df_out
