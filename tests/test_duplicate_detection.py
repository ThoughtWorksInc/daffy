"""Tests for duplicate value detection utility."""

from typing import Any

import pandas as pd
import polars as pl
import pytest

from daffy.dataframe_types import count_duplicate_values


@pytest.mark.parametrize("df_lib", [pd, pl], ids=["pandas", "polars"])
class TestCountDuplicateValues:
    def test_detects_duplicates(self, df_lib: Any) -> None:
        df = df_lib.DataFrame({"a": [1, 2, 2, 3]})
        assert count_duplicate_values(df, "a") == 1

    def test_no_duplicates(self, df_lib: Any) -> None:
        df = df_lib.DataFrame({"a": [1, 2, 3, 4]})
        assert count_duplicate_values(df, "a") == 0

    def test_multiple_duplicates(self, df_lib: Any) -> None:
        df = df_lib.DataFrame({"a": [1, 1, 1, 2, 2]})
        assert count_duplicate_values(df, "a") == 3
