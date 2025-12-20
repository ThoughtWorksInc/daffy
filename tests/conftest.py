from typing import Any, Callable, Union

import pandas as pd
import polars as pl
import pyarrow as pa
import pytest

# Modin requires ray which only supports Python 3.10-3.13
try:
    import modin.pandas as mpd

    HAS_MODIN = True
except ImportError:
    mpd = None  # type: ignore[assignment]
    HAS_MODIN = False

DataFrameType = Union[pd.DataFrame, pl.DataFrame]


@pytest.fixture(params=[pd, pl], ids=["pandas", "polars"])
def df_lib(request: pytest.FixtureRequest) -> type:
    """Return pd or pl module for creating DataFrames."""
    return request.param


def make_pandas_df(data: dict[str, Any]) -> pd.DataFrame:
    """Create a pandas DataFrame."""
    return pd.DataFrame(data)


def make_polars_df(data: dict[str, Any]) -> pl.DataFrame:
    """Create a polars DataFrame."""
    return pl.DataFrame(data)


def make_modin_df(data: dict[str, Any]) -> Any:
    """Create a modin DataFrame."""
    return mpd.DataFrame(data)  # type: ignore[union-attr]


def make_pyarrow_table(data: dict[str, Any]) -> pa.Table:
    """Create a PyArrow Table."""
    return pa.table(data)


DF_FACTORIES = [
    pytest.param(make_pandas_df, id="pandas"),
    pytest.param(make_polars_df, id="polars"),
    pytest.param(make_pyarrow_table, id="pyarrow"),
]

if HAS_MODIN:
    DF_FACTORIES.insert(2, pytest.param(make_modin_df, id="modin"))


@pytest.fixture(params=DF_FACTORIES)
def make_df(request: pytest.FixtureRequest) -> Callable[[dict[str, Any]], Any]:
    """Factory fixture for creating DataFrames across all supported libraries."""
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
