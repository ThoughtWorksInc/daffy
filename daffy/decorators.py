"""Decorators for DAFFY DataFrame Column Validator."""

from __future__ import annotations

import logging
from collections.abc import Callable
from functools import wraps
from typing import TYPE_CHECKING, Any, TypeVar

if TYPE_CHECKING:
    from pydantic import BaseModel

from daffy.config import get_lazy, get_row_validation_max_errors, get_strict
from daffy.dataframe_types import IntoDataFrameT
from daffy.row_validation import validate_dataframe_rows
from daffy.utils import (
    assert_is_dataframe,
    format_param_context,
    get_parameter,
    get_parameter_name,
    log_dataframe_input,
    log_dataframe_output,
)
from daffy.validation import ColumnsDef, validate_dataframe


def _validate_composite_unique(composite_unique: list[list[str]] | None) -> None:
    """Validate composite_unique parameter structure at decorator time."""
    if composite_unique is None:
        return

    if not isinstance(composite_unique, list):
        raise TypeError(f"composite_unique must be a list, got {type(composite_unique).__name__}")

    for i, combo in enumerate(composite_unique):
        if not isinstance(combo, list):
            raise TypeError(f"composite_unique[{i}] must be a list, got {type(combo).__name__}")
        if len(combo) < 2:
            raise ValueError(f"composite_unique[{i}] must have at least 2 columns, got {len(combo)}")
        for j, col in enumerate(combo):
            if not isinstance(col, str):
                raise TypeError(f"composite_unique[{i}][{j}] must be a string, got {type(col).__name__}")


# Type variables for preserving return types
LogReturnT = TypeVar("LogReturnT")  # Return type for df_log
InReturnT = TypeVar("InReturnT")  # Return type for df_in


def _validate_rows_with_context(
    df: Any,
    row_validator: "type[BaseModel]",
    func_name: str,
    param_name: str | None,
    is_return_value: bool,
) -> None:
    """Validate DataFrame rows with Pydantic model and add context to errors."""
    try:
        validate_dataframe_rows(df, row_validator, max_errors=get_row_validation_max_errors())
    except AssertionError as e:
        context = format_param_context(param_name, func_name, is_return_value)
        raise AssertionError(f"{str(e)}{context}") from e


def df_out(
    columns: ColumnsDef = None,
    strict: bool | None = None,
    lazy: bool | None = None,
    composite_unique: list[list[str]] | None = None,
    row_validator: "type[BaseModel] | None" = None,
) -> Callable[[Callable[..., IntoDataFrameT]], Callable[..., IntoDataFrameT]]:
    """Decorate a function that returns a DataFrame (Pandas, Polars, Modin, or PyArrow).

    Document the return value of a function. The return value will be validated in runtime.

    Args:
        columns (Union[Sequence[str], Dict[str, Any]], optional): Sequence or dict that describes expected columns
            of the DataFrame.
            Sequence can contain regex patterns in format "r/pattern/" (e.g., "r/Col[0-9]+/").
            Dict can use regex patterns as keys in format "r/pattern/" to validate dtypes for matching columns.
            Defaults to None.
        strict (bool, optional): If True, columns must match exactly with no extra columns.
            If None, uses the value from [tool.daffy] strict setting in pyproject.toml.
        lazy (bool, optional): If True, collect all validation errors before raising.
            If None, uses the value from [tool.daffy] lazy setting in pyproject.toml.
        composite_unique (list[list[str]], optional): List of column name lists that must be unique together.
            E.g., [["first_name", "last_name"]] ensures the combination is unique.
        row_validator (type[BaseModel], optional): Pydantic model for validating row data.
            Requires pydantic >= 2.4.0. Defaults to None.

    Returns:
        Callable: Decorated function with preserved DataFrame return type
    """
    _validate_composite_unique(composite_unique)

    def wrapper_df_out(func: Callable[..., IntoDataFrameT]) -> Callable[..., IntoDataFrameT]:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> IntoDataFrameT:
            result = func(*args, **kwargs)
            assert_is_dataframe(result, "return type")
            if columns or composite_unique:
                validate_dataframe(
                    df=result,
                    columns=columns or [],
                    strict=get_strict(strict),
                    param_name=None,
                    func_name=func.__name__,
                    is_return_value=True,
                    lazy=get_lazy(lazy),
                    composite_unique=composite_unique,
                )

            if row_validator is not None:
                _validate_rows_with_context(result, row_validator, func.__name__, None, True)

            return result

        return wrapper

    return wrapper_df_out


def df_in(
    name: str | None = None,
    columns: ColumnsDef = None,
    strict: bool | None = None,
    lazy: bool | None = None,
    composite_unique: list[list[str]] | None = None,
    row_validator: "type[BaseModel] | None" = None,
) -> Callable[[Callable[..., InReturnT]], Callable[..., InReturnT]]:
    """Decorate a function parameter that is a DataFrame (Pandas, Polars, Modin, or PyArrow).

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
        lazy (bool, optional): If True, collect all validation errors before raising.
            If None, uses the value from [tool.daffy] lazy setting in pyproject.toml.
        composite_unique (list[list[str]], optional): List of column name lists that must be unique together.
            E.g., [["first_name", "last_name"]] ensures the combination is unique.
        row_validator (type[BaseModel], optional): Pydantic model for validating row data.
            Requires pydantic >= 2.4.0. Defaults to None.

    Returns:
        Callable: Decorated function with preserved return type
    """
    _validate_composite_unique(composite_unique)

    def wrapper_df_in(func: Callable[..., InReturnT]) -> Callable[..., InReturnT]:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> InReturnT:
            df = get_parameter(func, name, *args, **kwargs)
            param_name = get_parameter_name(func, name, *args, **kwargs)
            assert_is_dataframe(df, "parameter type")
            if columns or composite_unique:
                validate_dataframe(
                    df=df,
                    columns=columns or [],
                    strict=get_strict(strict),
                    param_name=param_name,
                    func_name=func.__name__,
                    is_return_value=False,
                    lazy=get_lazy(lazy),
                    composite_unique=composite_unique,
                )

            if row_validator is not None:
                _validate_rows_with_context(df, row_validator, func.__name__, param_name, False)

            return func(*args, **kwargs)

        return wrapper

    return wrapper_df_in


def df_log(
    level: int = logging.DEBUG, include_dtypes: bool = False
) -> Callable[[Callable[..., LogReturnT]], Callable[..., LogReturnT]]:
    """Decorate a function that consumes or produces a Pandas DataFrame or both.

    Logs the columns of the consumed and/or produced DataFrame.

    Args:
        level (int, optional): Level of the logging messages produced. Defaults to logging.DEBUG.
        include_dtypes (bool, optional): When set to True, will log also the dtypes of each column. Defaults to False.

    Returns:
        Callable: Decorated function with preserved return type.
    """

    def wrapper_df_log(func: Callable[..., LogReturnT]) -> Callable[..., LogReturnT]:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> LogReturnT:
            log_dataframe_input(level, func.__name__, get_parameter(func, None, *args, **kwargs), include_dtypes)
            result = func(*args, **kwargs)
            log_dataframe_output(level, func.__name__, result, include_dtypes)
            return result

        return wrapper

    return wrapper_df_log
