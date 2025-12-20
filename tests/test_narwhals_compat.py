"""Tests for narwhals compatibility utilities."""

from typing import Any

import pandas as pd
import polars as pl
import pytest

from daffy.narwhals_compat import is_pandas_backend, is_supported_dataframe


class TestIsSupportedDataframe:
    @pytest.mark.parametrize("df", [pd.DataFrame({"A": [1, 2, 3]}), pl.DataFrame({"A": [1, 2, 3]})])
    def test_dataframes_supported(self, df: Any) -> None:
        assert is_supported_dataframe(df) is True

    @pytest.mark.parametrize("obj", [[1, 2, 3], {"A": [1, 2, 3]}, "not a dataframe", None, 42])
    def test_non_dataframes_not_supported(self, obj: Any) -> None:
        assert is_supported_dataframe(obj) is False


class TestIsPandasBackend:
    def test_pandas_returns_true(self) -> None:
        assert is_pandas_backend(pd.DataFrame({"A": [1]})) is True

    def test_polars_returns_false(self) -> None:
        assert is_pandas_backend(pl.DataFrame({"A": [1]})) is False
