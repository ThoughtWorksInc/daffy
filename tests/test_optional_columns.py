"""Tests for optional columns (required=False) feature."""

from typing import Any

import pandas as pd
import polars as pl
import pytest

from daffy import df_in, df_out
from tests.utils import DataFrameFactory, get_column_names


class TestOptionalColumnsBasic:
    def test_optional_column_missing_ok(self, make_df: DataFrameFactory) -> None:
        """Optional column that is missing should not raise an error."""

        @df_in(columns={"A": {"required": True}, "B": {"required": False}})
        def process(df: Any) -> Any:
            return df

        df = make_df({"A": [1, 2, 3]})
        result = process(df)

        assert get_column_names(result) == ["A"]

    def test_optional_column_present_validated(self, make_df: DataFrameFactory) -> None:
        """Optional column that is present should be validated normally."""

        @df_in(columns={"A": {"required": True}, "B": {"required": False}})
        def process(df: Any) -> Any:
            return df

        df = make_df({"A": [1, 2, 3], "B": [1.0, 2.0, 3.0]})
        result = process(df)

        assert get_column_names(result) == ["A", "B"]

    def test_optional_column_nullable_violation(self, make_df: DataFrameFactory) -> None:
        """Optional column with null values when nullable=False should raise an error."""

        @df_in(columns={"A": {"required": True}, "B": {"nullable": False, "required": False}})
        def process(df: Any) -> Any:
            return df

        df = make_df({"A": [1, 2, 3], "B": [1.0, None, 3.0]})

        with pytest.raises(AssertionError, match="null value"):
            process(df)

    def test_optional_column_unique_violation(self, make_df: DataFrameFactory) -> None:
        """Optional column with duplicate values when unique=True should raise an error."""

        @df_in(columns={"A": {"required": True}, "B": {"unique": True, "required": False}})
        def process(df: Any) -> Any:
            return df

        df = make_df({"A": [1, 2, 3], "B": [1.0, 1.0, 3.0]})

        with pytest.raises(AssertionError, match="duplicate value"):
            process(df)

    def test_required_default_true(self, make_df: DataFrameFactory) -> None:
        """Column without required key should be required by default."""

        @df_in(columns={"A": {"required": True}, "B": {}})
        def process(df: Any) -> Any:
            return df

        df = make_df({"A": [1, 2, 3]})

        with pytest.raises(AssertionError, match="Missing columns"):
            process(df)

    def test_multiple_optional_columns(self, make_df: DataFrameFactory) -> None:
        """Multiple optional columns can be missing."""

        @df_in(
            columns={
                "A": {"required": True},
                "B": {"required": False},
                "C": {"required": False},
            }
        )
        def process(df: Any) -> Any:
            return df

        df = make_df({"A": [1, 2, 3]})
        result = process(df)

        assert get_column_names(result) == ["A"]

    def test_df_out_optional_column_missing_ok(self, make_df: DataFrameFactory) -> None:
        """Optional column in df_out that is missing should not raise an error."""

        @df_out(columns={"A": {"required": True}, "B": {"required": False}})
        def process(df: Any) -> Any:
            return df

        df = make_df({"A": [1, 2, 3]})
        result = process(df)

        assert get_column_names(result) == ["A"]


class TestOptionalColumnsWithDtype:
    """Tests that require library-specific dtype specifications."""

    def test_optional_column_dtype_mismatch_pandas(self) -> None:
        """Optional column with wrong dtype should raise an error (pandas)."""

        @df_in(columns={"A": "int64", "B": {"dtype": "float64", "required": False}})
        def process(df: pd.DataFrame) -> pd.DataFrame:
            return df

        df = pd.DataFrame({"A": [1, 2, 3], "B": [1, 2, 3]})

        with pytest.raises(AssertionError, match="wrong dtype"):
            process(df)

    def test_optional_column_dtype_mismatch_polars(self) -> None:
        """Optional column with wrong dtype should raise an error (polars)."""

        @df_in(columns={"A": pl.Int64, "B": {"dtype": pl.Float64, "required": False}})
        def process(df: pl.DataFrame) -> pl.DataFrame:
            return df

        df = pl.DataFrame({"A": [1, 2, 3], "B": [1, 2, 3]})

        with pytest.raises(AssertionError, match="wrong dtype"):
            process(df)

    def test_optional_with_regex_pandas(self) -> None:
        """Regex pattern column with required=False should be optional (pandas)."""

        @df_in(columns={"A": "int64", "r/price_\\d+/": {"dtype": "float64", "required": False}})
        def process(df: pd.DataFrame) -> pd.DataFrame:
            return df

        df = pd.DataFrame({"A": [1, 2, 3]})
        result = process(df)

        assert get_column_names(result) == ["A"]

    def test_optional_with_regex_polars(self) -> None:
        """Regex pattern column with required=False should be optional (polars)."""

        @df_in(columns={"A": pl.Int64, "r/price_\\d+/": {"dtype": pl.Float64, "required": False}})
        def process(df: pl.DataFrame) -> pl.DataFrame:
            return df

        df = pl.DataFrame({"A": [1, 2, 3]})
        result = process(df)

        assert get_column_names(result) == ["A"]

    def test_df_out_optional_column_validated_pandas(self) -> None:
        """Optional column in df_out that is present should be validated (pandas)."""

        @df_out(columns={"A": "int64", "B": {"dtype": "float64", "required": False}})
        def process(df: pd.DataFrame) -> pd.DataFrame:
            return df

        df = pd.DataFrame({"A": [1, 2, 3], "B": [1, 2, 3]})  # B is int, not float

        with pytest.raises(AssertionError, match="wrong dtype"):
            process(df)

    def test_df_out_optional_column_validated_polars(self) -> None:
        """Optional column in df_out that is present should be validated (polars)."""

        @df_out(columns={"A": pl.Int64, "B": {"dtype": pl.Float64, "required": False}})
        def process(df: pl.DataFrame) -> pl.DataFrame:
            return df

        df = pl.DataFrame({"A": [1, 2, 3], "B": [1, 2, 3]})  # B is int, not float

        with pytest.raises(AssertionError, match="wrong dtype"):
            process(df)


class TestBackwardsCompatibility:
    """Test that existing usage without required key still works."""

    def test_list_columns_unchanged(self, make_df: DataFrameFactory) -> None:
        """List-based columns still work as before."""

        @df_in(columns=["A", "B"])
        def process(df: Any) -> Any:
            return df

        # All columns present - works
        df = make_df({"A": [1, 2], "B": [3, 4]})
        result = process(df)
        assert get_column_names(result) == ["A", "B"]

    def test_list_columns_missing_error(self, make_df: DataFrameFactory) -> None:
        """List-based columns still require all columns."""

        @df_in(columns=["A", "B"])
        def process(df: Any) -> Any:
            return df

        df = make_df({"A": [1, 2]})

        with pytest.raises(AssertionError, match="Missing columns"):
            process(df)

    def test_rich_spec_columns_present(self, make_df: DataFrameFactory) -> None:
        """Rich specs with only required key work correctly."""

        @df_in(columns={"A": {"required": True}, "B": {"required": True}})
        def process(df: Any) -> Any:
            return df

        df = make_df({"A": [1, 2], "B": [3.0, 4.0]})
        result = process(df)
        assert get_column_names(result) == ["A", "B"]

    def test_rich_spec_missing_required_column(self, make_df: DataFrameFactory) -> None:
        """Rich specs with required=True raise error on missing columns."""

        @df_in(columns={"A": {"required": True}, "B": {"required": True}})
        def process(df: Any) -> Any:
            return df

        df = make_df({"A": [1, 2]})

        with pytest.raises(AssertionError, match="Missing columns"):
            process(df)

    def test_rich_spec_without_required_defaults_true(self, make_df: DataFrameFactory) -> None:
        """Rich specs without required key default to required=True."""

        @df_in(columns={"A": {}, "B": {"nullable": True}})
        def process(df: Any) -> Any:
            return df

        df = make_df({"A": [1, 2]})

        with pytest.raises(AssertionError, match="Missing columns"):
            process(df)
