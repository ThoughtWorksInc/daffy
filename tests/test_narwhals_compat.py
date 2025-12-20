"""Tests for narwhals compatibility layer."""

import pandas as pd
import polars as pl

from daffy.narwhals_compat import (
    count_duplicates,
    count_nulls,
    get_columns,
    is_pandas_backend,
    is_polars_backend,
    series_fill_null,
    series_is_in,
    series_is_null,
    wrap_dataframe,
)


class TestWrapDataframe:
    def test_wrap_pandas(self) -> None:
        df = pd.DataFrame({"A": [1, 2, 3]})
        nw_df = wrap_dataframe(df)
        assert nw_df.columns == ["A"]

    def test_wrap_polars(self) -> None:
        df = pl.DataFrame({"A": [1, 2, 3]})
        nw_df = wrap_dataframe(df)
        assert nw_df.columns == ["A"]


class TestGetColumns:
    def test_pandas(self) -> None:
        df = pd.DataFrame({"A": [1], "B": [2], "C": [3]})
        assert get_columns(df) == ["A", "B", "C"]

    def test_polars(self) -> None:
        df = pl.DataFrame({"A": [1], "B": [2], "C": [3]})
        assert get_columns(df) == ["A", "B", "C"]


class TestCountNulls:
    def test_pandas_no_nulls(self) -> None:
        df = pd.DataFrame({"A": [1, 2, 3]})
        assert count_nulls(df, "A") == 0

    def test_pandas_with_nulls(self) -> None:
        df = pd.DataFrame({"A": [1, None, 3, None]})
        assert count_nulls(df, "A") == 2

    def test_polars_no_nulls(self) -> None:
        df = pl.DataFrame({"A": [1, 2, 3]})
        assert count_nulls(df, "A") == 0

    def test_polars_with_nulls(self) -> None:
        df = pl.DataFrame({"A": [1, None, 3, None]})
        assert count_nulls(df, "A") == 2


class TestCountDuplicates:
    def test_pandas_no_duplicates(self) -> None:
        df = pd.DataFrame({"A": [1, 2, 3]})
        assert count_duplicates(df, "A") == 0

    def test_pandas_with_duplicates(self) -> None:
        df = pd.DataFrame({"A": [1, 1, 2, 2, 2, 3]})
        # 1 appears 2 times (1 dup), 2 appears 3 times (2 dups), 3 appears once (0 dups)
        assert count_duplicates(df, "A") == 3

    def test_polars_no_duplicates(self) -> None:
        df = pl.DataFrame({"A": [1, 2, 3]})
        assert count_duplicates(df, "A") == 0

    def test_polars_with_duplicates(self) -> None:
        df = pl.DataFrame({"A": [1, 1, 2, 2, 2, 3]})
        assert count_duplicates(df, "A") == 3


class TestBackendDetection:
    def test_is_pandas_backend_true(self) -> None:
        df = pd.DataFrame({"A": [1, 2, 3]})
        assert is_pandas_backend(df) is True

    def test_is_pandas_backend_false(self) -> None:
        df = pl.DataFrame({"A": [1, 2, 3]})
        assert is_pandas_backend(df) is False

    def test_is_polars_backend_true(self) -> None:
        df = pl.DataFrame({"A": [1, 2, 3]})
        assert is_polars_backend(df) is True

    def test_is_polars_backend_false(self) -> None:
        df = pd.DataFrame({"A": [1, 2, 3]})
        assert is_polars_backend(df) is False


class TestSeriesIsIn:
    def test_pandas_all_in(self) -> None:
        series = pd.Series([1, 2, 3])
        result = series_is_in(series, [1, 2, 3, 4])
        assert result.tolist() == [True, True, True]

    def test_pandas_some_in(self) -> None:
        series = pd.Series([1, 2, 3])
        result = series_is_in(series, [1, 3])
        assert result.tolist() == [True, False, True]

    def test_polars_all_in(self) -> None:
        series = pl.Series([1, 2, 3])
        result = series_is_in(series, [1, 2, 3, 4])
        assert result.to_list() == [True, True, True]

    def test_polars_some_in(self) -> None:
        series = pl.Series([1, 2, 3])
        result = series_is_in(series, [1, 3])
        assert result.to_list() == [True, False, True]


class TestSeriesIsNull:
    def test_pandas_no_nulls(self) -> None:
        series = pd.Series([1, 2, 3])
        result = series_is_null(series)
        assert result.tolist() == [False, False, False]

    def test_pandas_with_nulls(self) -> None:
        series = pd.Series([1, None, 3])
        result = series_is_null(series)
        assert result.tolist() == [False, True, False]

    def test_polars_no_nulls(self) -> None:
        series = pl.Series([1, 2, 3])
        result = series_is_null(series)
        assert result.to_list() == [False, False, False]

    def test_polars_with_nulls(self) -> None:
        series = pl.Series([1, None, 3])
        result = series_is_null(series)
        assert result.to_list() == [False, True, False]


class TestSeriesFillNull:
    def test_pandas_no_nulls(self) -> None:
        series = pd.Series([1, 2, 3])
        result = series_fill_null(series, 0)
        assert result.tolist() == [1, 2, 3]

    def test_pandas_with_nulls(self) -> None:
        series = pd.Series([1.0, None, 3.0])
        result = series_fill_null(series, 99.0)
        assert result.tolist() == [1.0, 99.0, 3.0]

    def test_polars_no_nulls(self) -> None:
        series = pl.Series([1, 2, 3])
        result = series_fill_null(series, 0)
        assert result.to_list() == [1, 2, 3]

    def test_polars_with_nulls(self) -> None:
        series = pl.Series([1, None, 3])
        result = series_fill_null(series, 99)
        assert result.to_list() == [1, 99, 3]
