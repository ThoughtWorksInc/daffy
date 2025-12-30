from typing import Any, Callable

import pandas as pd
import polars as pl
import pyarrow as pa
import pytest

from daffy.dataframe_types import IntoDataFrame

__all__ = ["IntoDataFrame", "cars", "extended_cars"]


def make_pandas_df(data: dict[str, Any]) -> pd.DataFrame:
    """Create a pandas DataFrame."""
    return pd.DataFrame(data)


def make_polars_df(data: dict[str, Any]) -> pl.DataFrame:
    """Create a polars DataFrame."""
    return pl.DataFrame(data)


def make_pyarrow_table(data: dict[str, Any]) -> pa.Table:
    """Create a PyArrow Table."""
    return pa.table(data)


@pytest.fixture(
    params=[
        pytest.param(make_pandas_df, id="pandas"),
        pytest.param(make_polars_df, id="polars"),
        pytest.param(make_pyarrow_table, id="pyarrow"),
    ]
)
def make_df(request: pytest.FixtureRequest) -> Callable[[dict[str, Any]], Any]:
    """Factory fixture for creating DataFrames across supported libraries."""
    return request.param


cars = {
    "Brand": ["Honda Civic", "Toyota Corolla", "Ford Focus", "Audi A4"],
    "Price": [22000, 25000, 27000, 35000],
}


@pytest.fixture
def basic_pandas_df() -> pd.DataFrame:
    return pd.DataFrame(cars)


@pytest.fixture
def basic_polars_df() -> pl.DataFrame:
    return pl.DataFrame(cars)


extended_cars = {
    "Brand": ["Honda Civic", "Toyota Corolla", "Ford Focus", "Audi A4"],
    "Price": [22000, 25000, 27000, 35000],
    "Year": [2020, 1998, 2001, 2021],
}


@pytest.fixture
def extended_pandas_df() -> pd.DataFrame:
    return pd.DataFrame(extended_cars)


@pytest.fixture
def extended_polars_df() -> pl.DataFrame:
    return pl.DataFrame(extended_cars)
