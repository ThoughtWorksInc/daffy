"""Decorators for DAFFY DataFrame Column Validator."""

import logging
from functools import wraps
from typing import Any, Callable, Optional, TypeVar, Union

# Import fully qualified types to satisfy disallow_any_unimported
from pandas import DataFrame as PandasDataFrame
from polars import DataFrame as PolarsDataFrame

from daffy.config import get_strict
from daffy.utils import (
    assert_is_dataframe,
    get_parameter,
    get_parameter_name,
    log_dataframe_input,
    log_dataframe_output,
)
from daffy.validation import ColumnsDef, validate_dataframe

# Type variables for preserving return types
T = TypeVar("T")  # Generic type var for df_log
DF = TypeVar("DF", bound=Union[PandasDataFrame, PolarsDataFrame])
R = TypeVar("R")  # Return type for df_in


def df_out(
    columns: ColumnsDef = None, strict: Optional[bool] = None
) -> Callable[[Callable[..., DF]], Callable[..., DF]]:
    """Decorate a function that returns a Pandas or Polars DataFrame.

    Document the return value of a function. The return value will be validated in runtime.

    Args:
        columns (Union[Sequence[str], Dict[str, Any]], optional): Sequence or dict that describes expected columns
            of the DataFrame.
            Sequence can contain regex patterns in format "r/pattern/" (e.g., "r/Col[0-9]+/").
            Dict can use regex patterns as keys in format "r/pattern/" to validate dtypes for matching columns.
            Defaults to None.
        strict (bool, optional): If True, columns must match exactly with no extra columns.
            If None, uses the value from [tool.daffy] strict setting in pyproject.toml.

    Returns:
        Callable: Decorated function with preserved DataFrame return type
    """

    def wrapper_df_out(func: Callable[..., DF]) -> Callable[..., DF]:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> DF:
            result = func(*args, **kwargs)
            assert_is_dataframe(result, "return type")
            if columns:
                validate_dataframe(result, columns, get_strict(strict))
            return result

        return wrapper

    return wrapper_df_out


def df_in(
    name: Optional[str] = None, columns: ColumnsDef = None, strict: Optional[bool] = None
) -> Callable[[Callable[..., R]], Callable[..., R]]:
    """Decorate a function parameter that is a Pandas or Polars DataFrame.

    Document the contents of an input parameter. The parameter will be validated in runtime.

    Args:
        name (Optional[str], optional): Name of the parameter that contains a DataFrame. Defaults to None.
        columns (Union[Sequence[str], Dict[str, Any]], optional): Sequence or dict that describes expected columns
            of the DataFrame.
            Sequence can contain regex patterns in format "r/pattern/" (e.g., "r/Col[0-9]+/").
            Dict can use regex patterns as keys in format "r/pattern/" to validate dtypes for matching columns.
            Defaults to None.
        strict (bool, optional): If True, columns must match exactly with no extra columns.
            If None, uses the value from [tool.daffy] strict setting in pyproject.toml.

    Returns:
        Callable: Decorated function with preserved return type
    """

    def wrapper_df_in(func: Callable[..., R]) -> Callable[..., R]:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> R:
            df = get_parameter(func, name, *args, **kwargs)
            param_name = get_parameter_name(func, name, *args, **kwargs)
            assert_is_dataframe(df, "parameter type")
            if columns:
                validate_dataframe(df, columns, get_strict(strict), param_name)
            return func(*args, **kwargs)

        return wrapper

    return wrapper_df_in


def df_log(level: int = logging.DEBUG, include_dtypes: bool = False) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """Decorate a function that consumes or produces a Pandas DataFrame or both.

    Logs the columns of the consumed and/or produced DataFrame.

    Args:
        level (int, optional): Level of the logging messages produced. Defaults to logging.DEBUG.
        include_dtypes (bool, optional): When set to True, will log also the dtypes of each column. Defaults to False.

    Returns:
        Callable: Decorated function with preserved return type.
    """

    def wrapper_df_log(func: Callable[..., T]) -> Callable[..., T]:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> T:
            log_dataframe_input(level, func.__name__, get_parameter(func, None, *args, **kwargs), include_dtypes)
            result = func(*args, **kwargs)
            log_dataframe_output(level, func.__name__, result, include_dtypes)
            return result

        return wrapper

    return wrapper_df_log
