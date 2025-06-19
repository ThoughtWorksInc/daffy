"""Decorators for DAFFY DataFrame Column Validator."""

import inspect
import logging
import re
from functools import wraps
from typing import Any, Callable, Dict, List, Optional, Pattern, Set, Tuple, TypeVar, Union
from typing import Sequence as Seq  # Renamed to avoid collision

import pandas as pd
import polars as pl

# Import fully qualified types to satisfy disallow_any_unimported
from pandas import DataFrame as PandasDataFrame
from polars import DataFrame as PolarsDataFrame

from daffy.config import get_strict

# Type variables for preserving return types
T = TypeVar("T")  # Generic type var for df_log
DF = TypeVar("DF", bound=Union[PandasDataFrame, PolarsDataFrame])
R = TypeVar("R")  # Return type for df_in

RegexColumnDef = Tuple[str, Pattern[str]]

ColumnsList = Seq[Union[str, RegexColumnDef]]
ColumnsDict = Dict[Union[str, RegexColumnDef], Any]
ColumnsDef = Union[ColumnsList, ColumnsDict, None]
DataFrameType = Union[PandasDataFrame, PolarsDataFrame]


def _is_regex_pattern(column: Any) -> bool:
    return (
        isinstance(column, tuple) and len(column) == 2 and isinstance(column[0], str) and isinstance(column[1], Pattern)
    )


def _as_regex_pattern(column: Union[str, RegexColumnDef]) -> Optional[RegexColumnDef]:
    """Convert column to RegexColumnDef if it is a regex pattern, otherwise return None."""
    if _is_regex_pattern(column):
        return column  # type: ignore[return-value]  # We know it's the right type after the check
    return None


def _assert_is_dataframe(obj: Any, context: str) -> None:
    if not isinstance(obj, (pd.DataFrame, pl.DataFrame)):
        raise AssertionError(f"Wrong {context}. Expected DataFrame, got {type(obj).__name__} instead.")


def _make_param_info(param_name: Optional[str]) -> str:
    return f" in parameter '{param_name}'" if param_name else ""


def _find_missing_columns(column_spec: Union[str, RegexColumnDef], df_columns: List[str]) -> List[str]:
    """Find missing columns for a single column specification."""
    if isinstance(column_spec, str):
        return [column_spec] if column_spec not in df_columns else []
    elif _is_regex_pattern(column_spec):
        pattern_str, _ = column_spec
        matches = _match_column_with_regex(column_spec, df_columns)
        return [pattern_str] if not matches else []
    return []


def _find_dtype_mismatches(
    column_spec: Union[str, RegexColumnDef], df: DataFrameType, expected_dtype: Any, df_columns: List[str]
) -> List[Tuple[str, Any, Any]]:
    """Find dtype mismatches for a single column specification."""
    mismatches = []
    if isinstance(column_spec, str):
        if column_spec in df_columns and df[column_spec].dtype != expected_dtype:
            mismatches.append((column_spec, df[column_spec].dtype, expected_dtype))
    elif _is_regex_pattern(column_spec):
        matches = _match_column_with_regex(column_spec, df_columns)
        for matched_col in matches:
            if df[matched_col].dtype != expected_dtype:
                mismatches.append((matched_col, df[matched_col].dtype, expected_dtype))
    return mismatches


def _find_regex_matches(column_spec: Union[str, RegexColumnDef], df_columns: List[str]) -> Set[str]:
    """Find regex matches for a single column specification."""
    regex_pattern = _as_regex_pattern(column_spec)
    if regex_pattern:
        return set(_match_column_with_regex(regex_pattern, df_columns))
    return set()


def _match_column_with_regex(column_pattern: RegexColumnDef, df_columns: List[str]) -> List[str]:
    _, pattern = column_pattern
    return [col for col in df_columns if pattern.match(col)]


def _compile_regex_pattern(pattern_string: str) -> RegexColumnDef:
    pattern_str = pattern_string[2:-1]
    compiled_pattern = re.compile(pattern_str)
    return (pattern_string, compiled_pattern)


def _is_regex_string(column: str) -> bool:
    return column.startswith("r/") and column.endswith("/")


def _compile_regex_patterns(columns: Seq[Any]) -> List[Union[str, RegexColumnDef]]:
    return [_compile_regex_pattern(col) if isinstance(col, str) and _is_regex_string(col) else col for col in columns]


def _check_columns(
    df: DataFrameType, columns: Union[ColumnsList, ColumnsDict], strict: bool, param_name: Optional[str] = None
) -> None:
    df_columns = list(df.columns)  # Cache the column list conversion
    all_missing_columns = []
    all_dtype_mismatches = []
    all_matched_by_regex = set()

    if isinstance(columns, list):
        processed_columns = _compile_regex_patterns(columns)
        for column_spec in processed_columns:
            all_missing_columns.extend(_find_missing_columns(column_spec, df_columns))
            all_matched_by_regex.update(_find_regex_matches(column_spec, df_columns))
    else:  # isinstance(columns, dict)
        assert isinstance(columns, dict)
        for column, expected_dtype in columns.items():
            column_spec = (
                _compile_regex_pattern(column) if isinstance(column, str) and _is_regex_string(column) else column
            )
            all_missing_columns.extend(_find_missing_columns(column_spec, df_columns))
            all_dtype_mismatches.extend(_find_dtype_mismatches(column_spec, df, expected_dtype, df_columns))
            all_matched_by_regex.update(_find_regex_matches(column_spec, df_columns))

    param_info = _make_param_info(param_name)

    if all_missing_columns:
        raise AssertionError(f"Missing columns: {all_missing_columns}{param_info}. Got {_describe_pd(df)}")

    if all_dtype_mismatches:
        mismatch_descriptions = ", ".join(
            f"Column {col}{param_info} has wrong dtype. Was {was}, expected {expected}"
            for col, was, expected in all_dtype_mismatches
        )
        raise AssertionError(mismatch_descriptions)

    if strict:
        explicit_columns = {col for col in columns if isinstance(col, str)}
        allowed_columns = explicit_columns.union(all_matched_by_regex)
        extra_columns = set(df_columns) - allowed_columns
        if extra_columns:
            raise AssertionError(f"DataFrame{param_info} contained unexpected column(s): {', '.join(extra_columns)}")


def df_out(
    columns: Union[ColumnsList, ColumnsDict, None] = None, strict: Optional[bool] = None
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
            _assert_is_dataframe(result, "return type")
            if columns:
                _check_columns(result, columns, get_strict(strict))
            return result

        return wrapper

    return wrapper_df_out


def _get_parameter(func: Callable[..., Any], name: Optional[str] = None, *args: Any, **kwargs: Any) -> Any:
    if not name:
        return args[0] if args else next(iter(kwargs.values()), None)

    if name in kwargs:
        return kwargs[name]

    func_params_in_order = list(inspect.signature(func).parameters.keys())
    parameter_location = func_params_in_order.index(name)
    return args[parameter_location]


def _get_parameter_name(
    func: Callable[..., Any], name: Optional[str] = None, *args: Any, **kwargs: Any
) -> Optional[str]:
    if name:
        return name

    if args:
        func_params_in_order = list(inspect.signature(func).parameters.keys())
        return func_params_in_order[0]

    return next(iter(kwargs.keys()), None)


def df_in(
    name: Optional[str] = None, columns: Union[ColumnsList, ColumnsDict, None] = None, strict: Optional[bool] = None
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
            df = _get_parameter(func, name, *args, **kwargs)
            param_name = _get_parameter_name(func, name, *args, **kwargs)
            _assert_is_dataframe(df, "parameter type")
            if columns:
                _check_columns(df, columns, get_strict(strict), param_name)
            return func(*args, **kwargs)

        return wrapper

    return wrapper_df_in


def _describe_pd(df: DataFrameType, include_dtypes: bool = False) -> str:
    result = f"columns: {list(df.columns)}"
    if include_dtypes:
        if isinstance(df, pd.DataFrame):
            readable_dtypes = [dtype.name for dtype in df.dtypes]
            result += f" with dtypes {readable_dtypes}"
        else:
            result += f" with dtypes {df.dtypes}"
    return result


def _log_input(level: int, func_name: str, df: Any, include_dtypes: bool) -> None:
    if isinstance(df, (pd.DataFrame, pl.DataFrame)):
        logging.log(level, f"Function {func_name} parameters contained a DataFrame: {_describe_pd(df, include_dtypes)}")


def _log_output(level: int, func_name: str, df: Any, include_dtypes: bool) -> None:
    if isinstance(df, (pd.DataFrame, pl.DataFrame)):
        logging.log(level, f"Function {func_name} returned a DataFrame: {_describe_pd(df, include_dtypes)}")


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
