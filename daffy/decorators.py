"""Decorators for DAFFY DataFrame Column Validator."""

import inspect
import logging
import re
from functools import wraps
from typing import Any, Callable, Dict, List, Optional, Pattern, Tuple, TypeVar, Union

import pandas as pd
import polars as pl

# Import fully qualified types to satisfy disallow_any_unimported
from pandas import DataFrame as PandasDataFrame
from polars import DataFrame as PolarsDataFrame

from daffy.config import get_strict

# Type variables for preserving return types
T = TypeVar("T")
R = TypeVar("R")


# Improved type definitions to support regex patterns
RegexColumnDef = Tuple[str, Pattern[str]]  # Tuple of (pattern_str, compiled_pattern)
ColumnsDef = Union[List[Union[str, RegexColumnDef]], Dict[str, Any]]
DataFrameType = Union[PandasDataFrame, PolarsDataFrame]


def _is_regex_pattern(column: Any) -> bool:
    """Check if the column definition is a regex pattern tuple."""
    return (
        isinstance(column, tuple) and len(column) == 2 and isinstance(column[0], str) and isinstance(column[1], Pattern)
    )


def _match_column_with_regex(column_pattern: RegexColumnDef, df_columns: List[str]) -> List[str]:
    """Find all column names that match the regex pattern."""
    _, pattern = column_pattern
    return [col for col in df_columns if pattern.match(col)]


def _compile_regex_patterns(columns: List[Any]) -> List[Union[str, RegexColumnDef]]:
    """Compile regex patterns in the column list."""
    result: List[Union[str, RegexColumnDef]] = []
    for col in columns:
        if isinstance(col, str) and col.startswith("r/") and col.endswith("/"):
            # Pattern is in the format "r/pattern/"
            pattern_str = col[2:-1]  # Remove "r/" prefix and "/" suffix
            compiled_pattern = re.compile(pattern_str)
            result.append((col, compiled_pattern))
        else:
            result.append(col)
    return result


def _check_columns(df: DataFrameType, columns: ColumnsDef, strict: bool) -> None:
    missing_columns = []
    dtype_mismatches = []
    matched_by_regex = set()

    # Handle list of column names/patterns
    if isinstance(columns, list):
        # First, compile any regex patterns
        processed_columns = _compile_regex_patterns(columns)

        for column in processed_columns:
            if isinstance(column, str):
                # Direct column name match
                if column not in df.columns:
                    missing_columns.append(column)
            elif _is_regex_pattern(column):
                # Regex pattern match
                matches = _match_column_with_regex(column, list(df.columns))
                if not matches:
                    missing_columns.append(column[0])  # Add the original pattern string
                else:
                    matched_by_regex.update(matches)

    # Handle dictionary of column names/types
    elif isinstance(columns, dict):
        for column, dtype in columns.items():
            if column not in df.columns:
                missing_columns.append(column)
            elif df[column].dtype != dtype:
                dtype_mismatches.append((column, df[column].dtype, dtype))

    if missing_columns:
        raise AssertionError(f"Missing columns: {missing_columns}. Got {_describe_pd(df)}")

    if dtype_mismatches:
        mismatches = ", ".join(
            [f"Column {col} has wrong dtype. Was {was}, expected {expected}" for col, was, expected in dtype_mismatches]
        )
        raise AssertionError(mismatches)

    if strict:
        if isinstance(columns, list):
            # For regex matches, we need to consider all matched columns
            explicit_columns = {col for col in columns if isinstance(col, str)}
            allowed_columns = explicit_columns.union(matched_by_regex)
            extra_columns = set(df.columns) - allowed_columns
        else:
            extra_columns = set(df.columns) - set(columns)

        if extra_columns:
            raise AssertionError(f"DataFrame contained unexpected column(s): {', '.join(extra_columns)}")


def df_out(
    columns: Optional[ColumnsDef] = None, strict: Optional[bool] = None
) -> Callable[[Callable[..., DataFrameType]], Callable[..., DataFrameType]]:
    """Decorate a function that returns a Pandas or Polars DataFrame.

    Document the return value of a function. The return value will be validated in runtime.

    Args:
        columns (ColumnsDef, optional): List or dict that describes expected columns of the DataFrame.
            List can contain regex patterns in format "r/pattern/" (e.g., "r/Col[0-9]+/"). Defaults to None.
        strict (bool, optional): If True, columns must match exactly with no extra columns.
            If None, uses the value from [tool.daffy] strict setting in pyproject.toml.

    Returns:
        Callable: Decorated function with preserved DataFrame return type
    """

    def wrapper_df_out(func: Callable[..., DataFrameType]) -> Callable[..., DataFrameType]:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> DataFrameType:
            result = func(*args, **kwargs)
            assert isinstance(result, pd.DataFrame) or isinstance(result, pl.DataFrame), (
                f"Wrong return type. Expected DataFrame, got {type(result)}"
            )
            if columns:
                _check_columns(result, columns, get_strict(strict))
            return result

        return wrapper

    return wrapper_df_out


def _get_parameter(func: Callable[..., Any], name: Optional[str] = None, *args: Any, **kwargs: Any) -> Any:
    if not name:
        if len(args) > 0:
            return args[0]
        if kwargs:
            return next(iter(kwargs.values()))
        return None

    if name and (name not in kwargs):
        func_params_in_order = list(inspect.signature(func).parameters.keys())
        parameter_location = func_params_in_order.index(name)
        return args[parameter_location]

    return kwargs[name]


def df_in(
    name: Optional[str] = None, columns: Optional[ColumnsDef] = None, strict: Optional[bool] = None
) -> Callable[[Callable[..., R]], Callable[..., R]]:
    """Decorate a function parameter that is a Pandas or Polars DataFrame.

    Document the contents of an input parameter. The parameter will be validated in runtime.

    Args:
        name (Optional[str], optional): Name of the parameter that contains a DataFrame. Defaults to None.
        columns (ColumnsDef, optional): List or dict that describes expected columns of the DataFrame.
            List can contain regex patterns in format "r/pattern/" (e.g., "r/Col[0-9]+/"). Defaults to None.
        strict (bool, optional): If True, columns must match exactly with no extra columns.
            If None, uses the value from [tool.daffy] strict setting in pyproject.toml.

    Returns:
        Callable: Decorated function with preserved return type
    """

    def wrapper_df_in(func: Callable[..., R]) -> Callable[..., R]:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> R:
            df = _get_parameter(func, name, *args, **kwargs)
            assert isinstance(df, pd.DataFrame) or isinstance(df, pl.DataFrame), (
                f"Wrong parameter type. Expected DataFrame, got {type(df).__name__} instead."
            )
            if columns:
                _check_columns(df, columns, get_strict(strict))
            return func(*args, **kwargs)

        return wrapper

    return wrapper_df_in


def _describe_pd(df: DataFrameType, include_dtypes: bool = False) -> str:
    result = f"columns: {list(df.columns)}"
    if include_dtypes:
        if isinstance(df, pd.DataFrame):
            readable_dtypes = [dtype.name for dtype in df.dtypes]
            result += f" with dtypes {readable_dtypes}"
        if isinstance(df, pl.DataFrame):
            result += f" with dtypes {df.dtypes}"
    return result


def _log_input(level: int, func_name: str, df: Any, include_dtypes: bool) -> None:
    if isinstance(df, pd.DataFrame) or isinstance(df, pl.DataFrame):
        logging.log(
            level,
            f"Function {func_name} parameters contained a DataFrame: {_describe_pd(df, include_dtypes)}",
        )


def _log_output(level: int, func_name: str, df: Any, include_dtypes: bool) -> None:
    if isinstance(df, pd.DataFrame) or isinstance(df, pl.DataFrame):
        logging.log(
            level,
            f"Function {func_name} returned a DataFrame: {_describe_pd(df, include_dtypes)}",
        )


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
            _log_input(level, func.__name__, _get_parameter(func, None, *args, **kwargs), include_dtypes)
            result = func(*args, **kwargs)
            _log_output(level, func.__name__, result, include_dtypes)
            return result  # Added missing return statement

        return wrapper

    return wrapper_df_log
