"""Test type compatibility issues that might occur in client code."""

from typing import Sequence

import pandas as pd
import polars as pl

from daffy import df_in, df_out


# Pass-through function for testing
@df_in(columns=["Brand", "Price"])
def simple_list_columns(df: pd.DataFrame) -> pd.DataFrame:
    return df


def test_simple_list_columns() -> None:
    """Test with a simple list of string columns."""
    df = pd.DataFrame({"Brand": ["Toyota"], "Price": [25000]})
    result = simple_list_columns(df)
    assert isinstance(result, pd.DataFrame)


# This would test the Union type DataFrameType compatibility
@df_out(columns=["Brand", "Price"])
def return_dataframe() -> pd.DataFrame:
    return pd.DataFrame({"Brand": ["Toyota"], "Price": [25000]})


def function_with_explicit_type_annotations(columns: Sequence[str]) -> None:
    @df_in(columns=columns)
    def inner_function(df: pd.DataFrame) -> pd.DataFrame:
        return df

    df = pd.DataFrame({"Brand": ["Toyota"], "Price": [25000]})
    inner_function(df)


def test_with_polars() -> None:
    df = pl.DataFrame({"Brand": ["Toyota"], "Price": [25000]})

    @df_in(columns=["Brand", "Price"])
    def inner_function(df_param: pl.DataFrame) -> pl.DataFrame:
        return df_param

    inner_function(df)


def test_function_with_explicit_type_annotations() -> None:
    columns = ["Brand", "Price"]
    function_with_explicit_type_annotations(columns)


def test_simple_list_columns_function() -> None:
    df = pd.DataFrame({"Brand": ["Toyota"], "Price": [25000]})
    simple_list_columns(df)


def test_return_dataframe_function() -> None:
    result = return_dataframe()
    assert isinstance(result, pd.DataFrame)


def test_dtype_with_regex_pandas() -> None:
    """Test using both dtype validation and regex patterns with pandas."""
    # Create a DataFrame with numeric columns following a pattern
    df = pd.DataFrame(
        {
            "measure_2020": [10, 20, 30],
            "measure_2021": [15, 25, 35],
            "measure_2022": [18, 28, 38],
            "category": ["A", "B", "C"],
        }
    )

    # Define a function using both regex patterns and dtype validation
    @df_in(
        columns={
            "category": "object",
            "r/measure_\\d{4}/": "int64",  # All measure_YYYY columns should be int64
        }
    )
    def process_measures(data: pd.DataFrame) -> pd.DataFrame:
        return data

    # This should pass type checking and runtime validation
    result = process_measures(df)
    assert "measure_2020" in result.columns
    assert "measure_2021" in result.columns
    assert "measure_2022" in result.columns


def test_dtype_with_regex_polars() -> None:
    """Test using both dtype validation and regex patterns with polars."""
    # Create a Polars DataFrame with numeric columns following a pattern
    df = pl.DataFrame(
        {
            "measure_2020": [10, 20, 30],
            "measure_2021": [15, 25, 35],
            "measure_2022": [18, 28, 38],
            "category": ["A", "B", "C"],
        }
    )

    # Define a function using both regex patterns and dtype validation
    @df_in(
        columns={
            "category": pl.String,
            "r/measure_\\d{4}/": pl.Int64,  # All measure_YYYY columns should be Int64
        }
    )
    def process_measures(data: pl.DataFrame) -> pl.DataFrame:
        return data

    # This should pass type checking and runtime validation
    result = process_measures(df)
    assert "measure_2020" in result.columns
    assert "measure_2021" in result.columns
    assert "measure_2022" in result.columns


def test_type_narrowing_with_df_out_pandas() -> None:
    """Test assigning df_out decorated function result to a specific Pandas DataFrame type."""

    # Define a function that returns a DataFrame with df_out decoration
    @df_out(columns=["name", "value"])
    def get_data() -> pd.DataFrame:
        return pd.DataFrame({"name": ["A", "B", "C"], "value": [1, 2, 3]})

    # The critical test: we should be able to assign the result to a variable
    # explicitly typed as pd.DataFrame without mypy errors
    result: pd.DataFrame = get_data()
    assert "name" in result.columns
    assert "value" in result.columns


def test_type_narrowing_with_df_out_polars() -> None:
    """Test assigning df_out decorated function result to a specific Polars DataFrame type."""

    # Define a function that returns a DataFrame with df_out decoration
    @df_out(columns=["name", "value"])
    def get_data() -> pl.DataFrame:
        return pl.DataFrame({"name": ["A", "B", "C"], "value": [1, 2, 3]})

    # The critical test: we should be able to assign the result to a variable
    # explicitly typed as pl.DataFrame without mypy errors
    result: pl.DataFrame = get_data()
    assert "name" in result.columns
    assert "value" in result.columns


def test_df_out_preserves_specific_return_type() -> None:
    """Test that df_out preserves the specific DataFrame return type annotation."""

    # Function that specifically returns pandas DataFrame with df_out
    @df_out(columns=["col1", "col2"])
    def function_with_pandas_df() -> pd.DataFrame:
        return pd.DataFrame({"col1": [1, 2], "col2": [3, 4]})

    # We should be able to assign to a variable typed as pandas DataFrame
    # without having to cast or getting type errors
    result: pd.DataFrame = function_with_pandas_df()

    # Same with a function returning polars DataFrame
    @df_out(columns=["col1", "col2"])
    def function_with_polars_df() -> pl.DataFrame:
        return pl.DataFrame({"col1": [1, 2], "col2": [3, 4]})

    # Should be assignable to a variable typed as polars DataFrame
    polars_result: pl.DataFrame = function_with_polars_df()

    # Both should work at runtime too
    assert isinstance(result, pd.DataFrame)
    assert isinstance(polars_result, pl.DataFrame)
