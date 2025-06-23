"""Utility functions for DAFFY DataFrame Column Validator."""

import inspect
import logging
from typing import Any, Callable, Optional, Union

import pandas as pd
import polars as pl

# Import fully qualified types to satisfy disallow_any_unimported
from pandas import DataFrame as PandasDataFrame
from polars import DataFrame as PolarsDataFrame

DataFrameType = Union[PandasDataFrame, PolarsDataFrame]


def _assert_is_dataframe(obj: Any, context: str) -> None:
    if not isinstance(obj, (pd.DataFrame, pl.DataFrame)):
        raise AssertionError(f"Wrong {context}. Expected DataFrame, got {type(obj).__name__} instead.")


def _make_param_info(param_name: Optional[str]) -> str:
    return f" in parameter '{param_name}'" if param_name else ""


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
