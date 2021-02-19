import logging
from functools import wraps
from typing import Any, Callable, Dict, List, Optional, Union

import pandas as pd

ColumnsDef = Optional[Union[List, Dict]]


def _check_columns(df: pd.DataFrame, columns: ColumnsDef) -> None:
    if isinstance(columns, list):
        for column in columns:
            assert column in df.columns, f"Column {column} missing from DataFrame. Got {_describe_pd(df)}"
    if isinstance(columns, dict):
        for column, dtype in columns.items():
            assert column in df.columns, f"Column {column} missing from DataFrame. Got {_describe_pd(df)}"
            assert (
                df[column].dtype == dtype
            ), f"Column {column} has wrong dtype. Was {df[column].dtype}, expected {dtype}"


def df_out(columns: ColumnsDef = None) -> Callable:
    def wrapper_df_out(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args: str, **kwargs: Any) -> Any:
            result = func(*args, **kwargs)
            assert isinstance(result, pd.DataFrame), f"Wrong return type. Expected pandas dataframe, got {type(result)}"
            if columns:
                _check_columns(result, columns)
            return result

        return wrapper

    return wrapper_df_out


def _get_parameter(name: Optional[str] = None, *args: str, **kwargs: Any) -> pd.DataFrame:
    if not name:
        if len(args) == 0:
            return None
        return args[0]
    return kwargs[name]


def df_in(name: Optional[str] = None, columns: ColumnsDef = None) -> Callable:
    def wrapper_df_out(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args: str, **kwargs: Any) -> Any:
            df = _get_parameter(name, *args, **kwargs)
            assert isinstance(df, pd.DataFrame), f"Wrong parameter type. Expected pandas dataframe, got {type(df)}"
            if columns:
                _check_columns(df, columns)
            return func(*args, **kwargs)

        return wrapper

    return wrapper_df_out


def _describe_pd(df: pd.DataFrame, list_columns: Optional[bool] = True):
    result = ""
    if list_columns:
        result += f"columns: {list(df.columns)}"
    return result


def _log_input(level: int, func_name: str, df: Any) -> None:
    if isinstance(df, pd.DataFrame):
        logging.log(
            level,
            f"Function {func_name} parameters contained a DataFrame: {_describe_pd(df)}",
        )


def _log_output(level: int, func_name: str, df: Any) -> None:
    if isinstance(df, pd.DataFrame):
        logging.log(
            level,
            f"Function {func_name} returned a DataFrame: {_describe_pd(df)}",
        )


def df_log(level: Optional[int] = logging.DEBUG) -> Callable:
    def wrapper_df_log(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args: str, **kwargs: Any) -> Any:
            _log_input(level, func.__name__, _get_parameter(None, *args, **kwargs))
            result = func(*args, **kwargs)
            _log_output(level, func.__name__, result)

        return wrapper

    return wrapper_df_log
