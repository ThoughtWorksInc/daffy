"""Tests for ValidationContext."""

import pandas as pd
import polars as pl

from daffy.validators.context import ValidationContext


class TestValidationContext:
    def test_caches_narwhals_conversion(self) -> None:
        df = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
        ctx = ValidationContext(df=df)

        assert ctx.nw_df is not None
        assert ctx.columns == ("a", "b")
        assert ctx.column_set == frozenset({"a", "b"})
        assert ctx.row_count == 3

    def test_schema_caches_dtypes(self) -> None:
        df = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
        ctx = ValidationContext(df=df)

        assert "a" in ctx.schema
        assert "b" in ctx.schema

    def test_has_column(self) -> None:
        df = pd.DataFrame({"a": [1], "b": [2]})
        ctx = ValidationContext(df=df)

        assert ctx.has_column("a")
        assert ctx.has_column("b")
        assert not ctx.has_column("c")

    def test_get_series(self) -> None:
        df = pd.DataFrame({"a": [1, 2, 3]})
        ctx = ValidationContext(df=df)

        series = ctx.get_series("a")
        assert list(series.to_list()) == [1, 2, 3]

    def test_get_dtype(self) -> None:
        df = pd.DataFrame({"a": [1, 2, 3]})
        ctx = ValidationContext(df=df)

        dtype = ctx.get_dtype("a")
        assert dtype is not None
        assert ctx.get_dtype("nonexistent") is None


class TestParamInfo:
    def test_return_value(self) -> None:
        ctx = ValidationContext(
            df=pd.DataFrame({"a": [1]}),
            func_name="my_func",
            is_return_value=True,
        )
        assert ctx.param_info == " in function 'my_func' return value"

    def test_parameter(self) -> None:
        ctx = ValidationContext(
            df=pd.DataFrame({"a": [1]}),
            func_name="my_func",
            param_name="data",
        )
        assert ctx.param_info == " in function 'my_func' parameter 'data'"

    def test_no_context(self) -> None:
        ctx = ValidationContext(df=pd.DataFrame({"a": [1]}))
        assert ctx.param_info == ""


class TestPolarsSupport:
    def test_works_with_polars(self) -> None:
        df = pl.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
        ctx = ValidationContext(df=df)

        assert ctx.columns == ("a", "b")
        assert ctx.row_count == 3
        assert ctx.has_column("a")
