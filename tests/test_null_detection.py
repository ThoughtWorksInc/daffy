"""Tests for null value detection utility."""

import pandas as pd
import polars as pl

from daffy.dataframe_types import has_null_values


class TestHasNullValuesPandas:
    def test_detects_nulls_in_pandas(self) -> None:
        df = pd.DataFrame({"a": [1.0, None, 3.0]})
        has_nulls, count = has_null_values(df, "a")
        assert has_nulls is True
        assert count == 1

    def test_no_nulls_in_pandas(self) -> None:
        df = pd.DataFrame({"a": [1.0, 2.0, 3.0]})
        has_nulls, count = has_null_values(df, "a")
        assert has_nulls is False
        assert count == 0

    def test_multiple_nulls_in_pandas(self) -> None:
        df = pd.DataFrame({"a": [None, None, None, 1.0]})
        has_nulls, count = has_null_values(df, "a")
        assert has_nulls is True
        assert count == 3

    def test_nan_in_pandas(self) -> None:
        import numpy as np

        df = pd.DataFrame({"a": [1.0, np.nan, 3.0]})
        has_nulls, count = has_null_values(df, "a")
        assert has_nulls is True
        assert count == 1


class TestHasNullValuesPolars:
    def test_detects_nulls_in_polars(self) -> None:
        df = pl.DataFrame({"a": [1.0, None, 3.0]})
        has_nulls, count = has_null_values(df, "a")
        assert has_nulls is True
        assert count == 1

    def test_no_nulls_in_polars(self) -> None:
        df = pl.DataFrame({"a": [1.0, 2.0, 3.0]})
        has_nulls, count = has_null_values(df, "a")
        assert has_nulls is False
        assert count == 0

    def test_multiple_nulls_in_polars(self) -> None:
        df = pl.DataFrame({"a": [None, None, None, 1.0]})
        has_nulls, count = has_null_values(df, "a")
        assert has_nulls is True
        assert count == 3
