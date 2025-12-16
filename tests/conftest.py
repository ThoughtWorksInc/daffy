from typing import Union

import pandas as pd
import polars as pl
import pytest

DataFrameType = Union[pd.DataFrame, pl.DataFrame]


@pytest.fixture(params=[pd, pl], ids=["pandas", "polars"])
def df_lib(request: pytest.FixtureRequest) -> type:
    """Return pd or pl module for creating DataFrames."""
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
