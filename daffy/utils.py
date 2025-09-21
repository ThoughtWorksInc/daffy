"""Utility functions for DAFFY DataFrame Column Validator."""

import inspect
import logging
from typing import TYPE_CHECKING, Any, Callable, Optional, Union

# Lazy imports - only import what's available
try:
    import pandas as pd
    from pandas import DataFrame as PandasDataFrame

    HAS_PANDAS = True
except ImportError:  # pragma: no cover
    pd = None  # type: ignore
    PandasDataFrame = None  # type: ignore
    HAS_PANDAS = False

try:
    import polars as pl
    from polars import DataFrame as PolarsDataFrame

    HAS_POLARS = True
except ImportError:  # pragma: no cover
    pl = None  # type: ignore
    PolarsDataFrame = None  # type: ignore
    HAS_POLARS = False

# Build DataFrame type dynamically based on what's available
if TYPE_CHECKING:
    # For static type checking, assume both are available
    from pandas import DataFrame as PandasDataFrame
    from polars import DataFrame as PolarsDataFrame

    DataFrameType = Union[PandasDataFrame, PolarsDataFrame]
else:
    # For runtime, build type tuple from available libraries
    _available_types = []
    if HAS_PANDAS:
        _available_types.append(PandasDataFrame)
    if HAS_POLARS:
        _available_types.append(PolarsDataFrame)

    if not _available_types:  # pragma: no cover
        raise ImportError(
            "No DataFrame library found. Please install Pandas or Polars: pip install pandas  OR  pip install polars"
        )

    DataFrameType = Union[tuple(_available_types)]


def assert_is_dataframe(obj: Any, context: str) -> None:
    # Build type tuple dynamically based on available libraries
    dataframe_types: list[Any] = []
    if HAS_PANDAS and pd is not None:
        dataframe_types.append(pd.DataFrame)
    if HAS_POLARS and pl is not None:
        dataframe_types.append(pl.DataFrame)

    if not isinstance(obj, tuple(dataframe_types)):
        available_libs = []
        if HAS_PANDAS:
            available_libs.append("Pandas")
        if HAS_POLARS:
            available_libs.append("Polars")
        libs_str = " or ".join(available_libs)
        raise AssertionError(f"Wrong {context}. Expected {libs_str} DataFrame, got {type(obj).__name__} instead.")


def format_param_context(
    param_name: Optional[str], func_name: Optional[str] = None, is_return_value: bool = False
) -> str:
    context_parts = []
    if func_name:
        context_parts.append(f"function '{func_name}'")

    if is_return_value:
        context_parts.append("return value")
    elif param_name:
        context_parts.append(f"parameter '{param_name}'")

    if context_parts:
        return f" in {' '.join(context_parts)}"
    return ""


def get_parameter(func: Callable[..., Any], name: Optional[str] = None, *args: Any, **kwargs: Any) -> Any:
    if not name:
        return args[0] if args else next(iter(kwargs.values()), None)

    if name in kwargs:
        return kwargs[name]

    func_params_in_order = list(inspect.signature(func).parameters.keys())
    parameter_location = func_params_in_order.index(name)
    return args[parameter_location]


def get_parameter_name(
    func: Callable[..., Any], name: Optional[str] = None, *args: Any, **kwargs: Any
) -> Optional[str]:
    if name:
        return name

    if args:
        func_params_in_order = list(inspect.signature(func).parameters.keys())
        return func_params_in_order[0]

    return next(iter(kwargs.keys()), None)


def describe_dataframe(df: DataFrameType, include_dtypes: bool = False) -> str:
    result = f"columns: {list(df.columns)}"
    if include_dtypes:
        if HAS_PANDAS and pd is not None and isinstance(df, pd.DataFrame):
            readable_dtypes = [dtype.name for dtype in df.dtypes]
            result += f" with dtypes {readable_dtypes}"
        elif HAS_POLARS and pl is not None and isinstance(df, pl.DataFrame):
            result += f" with dtypes {df.dtypes}"
    return result


def log_dataframe_input(level: int, func_name: str, df: Any, include_dtypes: bool) -> None:
    # Build type tuple dynamically based on available libraries
    dataframe_types: list[Any] = []
    if HAS_PANDAS and pd is not None:
        dataframe_types.append(pd.DataFrame)
    if HAS_POLARS and pl is not None:
        dataframe_types.append(pl.DataFrame)

    if isinstance(df, tuple(dataframe_types)):
        logging.log(
            level, f"Function {func_name} parameters contained a DataFrame: {describe_dataframe(df, include_dtypes)}"
        )


def log_dataframe_output(level: int, func_name: str, df: Any, include_dtypes: bool) -> None:
    # Build type tuple dynamically based on available libraries
    dataframe_types: list[Any] = []
    if HAS_PANDAS and pd is not None:
        dataframe_types.append(pd.DataFrame)
    if HAS_POLARS and pl is not None:
        dataframe_types.append(pl.DataFrame)

    if isinstance(df, tuple(dataframe_types)):
        logging.log(level, f"Function {func_name} returned a DataFrame: {describe_dataframe(df, include_dtypes)}")
