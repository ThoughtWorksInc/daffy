"""Tests for null value detection utility."""

from typing import Any

import pandas as pd
import polars as pl
import pytest

from daffy.dataframe_types import count_null_values


@pytest.mark.parametrize("df_lib", [pd, pl], ids=["pandas", "polars"])
class TestCountNullValues:
    def test_detects_nulls(self, df_lib: Any) -> None:
        df = df_lib.DataFrame({"a": [1.0, None, 3.0]})
        assert count_null_values(df, "a") == 1

    def test_no_nulls(self, df_lib: Any) -> None:
        df = df_lib.DataFrame({"a": [1.0, 2.0, 3.0]})
        assert count_null_values(df, "a") == 0

    def test_multiple_nulls(self, df_lib: Any) -> None:
        df = df_lib.DataFrame({"a": [None, None, None, 1.0]})
        assert count_null_values(df, "a") == 3


class TestCountNullValuesPandasSpecific:
    def test_nan_in_pandas(self) -> None:
        import numpy as np

        df = pd.DataFrame({"a": [1.0, np.nan, 3.0]})
        assert count_null_values(df, "a") == 1
