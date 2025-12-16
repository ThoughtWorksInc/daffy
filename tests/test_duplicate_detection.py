"""Tests for duplicate value detection utility."""

import pandas as pd
import polars as pl

from daffy.dataframe_types import count_duplicate_values


class TestCountDuplicateValuesPandas:
    def test_detects_duplicates_in_pandas(self) -> None:
        df = pd.DataFrame({"a": [1, 2, 2, 3]})
        assert count_duplicate_values(df, "a") == 1

    def test_no_duplicates_in_pandas(self) -> None:
        df = pd.DataFrame({"a": [1, 2, 3, 4]})
        assert count_duplicate_values(df, "a") == 0

    def test_multiple_duplicates_in_pandas(self) -> None:
        df = pd.DataFrame({"a": [1, 1, 1, 2, 2]})
        assert count_duplicate_values(df, "a") == 3


class TestCountDuplicateValuesPolars:
    def test_detects_duplicates_in_polars(self) -> None:
        df = pl.DataFrame({"a": [1, 2, 2, 3]})
        assert count_duplicate_values(df, "a") == 1

    def test_no_duplicates_in_polars(self) -> None:
        df = pl.DataFrame({"a": [1, 2, 3, 4]})
        assert count_duplicate_values(df, "a") == 0

    def test_multiple_duplicates_in_polars(self) -> None:
        df = pl.DataFrame({"a": [1, 1, 1, 2, 2]})
        assert count_duplicate_values(df, "a") == 3
