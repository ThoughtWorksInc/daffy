"""Tests for optional columns (required=False) feature."""

from typing import Any

import pandas as pd
import polars as pl
import pytest

from daffy import df_in, df_out


@pytest.mark.parametrize("df_lib", [pd, pl], ids=["pandas", "polars"])
class TestOptionalColumnsBasic:
    def test_optional_column_missing_ok(self, df_lib: Any) -> None:
        """Optional column that is missing should not raise an error."""

        @df_in(columns={"A": {"required": True}, "B": {"required": False}})
        def process(df: Any) -> Any:
            return df

        df = df_lib.DataFrame({"A": [1, 2, 3]})
        result = process(df)

        assert list(result.columns) == ["A"]

    def test_optional_column_present_validated(self, df_lib: Any) -> None:
        """Optional column that is present should be validated normally."""

        @df_in(columns={"A": {"required": True}, "B": {"required": False}})
        def process(df: Any) -> Any:
            return df

        df = df_lib.DataFrame({"A": [1, 2, 3], "B": [1.0, 2.0, 3.0]})
        result = process(df)

        assert list(result.columns) == ["A", "B"]

    def test_optional_column_nullable_violation(self, df_lib: Any) -> None:
        """Optional column with null values when nullable=False should raise an error."""

        @df_in(columns={"A": {"required": True}, "B": {"nullable": False, "required": False}})
        def process(df: Any) -> Any:
            return df

        df = df_lib.DataFrame({"A": [1, 2, 3], "B": [1.0, None, 3.0]})

        with pytest.raises(AssertionError, match="null value"):
            process(df)

    def test_optional_column_unique_violation(self, df_lib: Any) -> None:
        """Optional column with duplicate values when unique=True should raise an error."""

        @df_in(columns={"A": {"required": True}, "B": {"unique": True, "required": False}})
        def process(df: Any) -> Any:
            return df

        df = df_lib.DataFrame({"A": [1, 2, 3], "B": [1.0, 1.0, 3.0]})

        with pytest.raises(AssertionError, match="duplicate value"):
            process(df)

    def test_required_default_true(self, df_lib: Any) -> None:
        """Column without required key should be required by default."""

        @df_in(columns={"A": {"required": True}, "B": {}})
        def process(df: Any) -> Any:
            return df

        df = df_lib.DataFrame({"A": [1, 2, 3]})

        with pytest.raises(AssertionError, match="Missing columns"):
            process(df)

    def test_multiple_optional_columns(self, df_lib: Any) -> None:
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

        df = df_lib.DataFrame({"A": [1, 2, 3]})
        result = process(df)

        assert list(result.columns) == ["A"]

    def test_df_out_optional_column_missing_ok(self, df_lib: Any) -> None:
        """Optional column in df_out that is missing should not raise an error."""

        @df_out(columns={"A": {"required": True}, "B": {"required": False}})
        def process(df: Any) -> Any:
            return df

        df = df_lib.DataFrame({"A": [1, 2, 3]})
        result = process(df)

        assert list(result.columns) == ["A"]


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

        assert list(result.columns) == ["A"]

    def test_optional_with_regex_polars(self) -> None:
        """Regex pattern column with required=False should be optional (polars)."""

        @df_in(columns={"A": pl.Int64, "r/price_\\d+/": {"dtype": pl.Float64, "required": False}})
        def process(df: pl.DataFrame) -> pl.DataFrame:
            return df

        df = pl.DataFrame({"A": [1, 2, 3]})
        result = process(df)

        assert list(result.columns) == ["A"]

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
