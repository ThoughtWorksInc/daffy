"""Tests for DataFrame shape validation (min_rows, max_rows, exact_rows)."""

from typing import Any

import pytest

from daffy import df_in, df_out
from tests.utils import DataFrameFactory


class TestMinRowsDfOut:
    def test_min_rows_rejects_too_few_rows(self, make_df: DataFrameFactory) -> None:
        @df_out(min_rows=3)
        def f() -> Any:
            return make_df({"a": [1, 2]})

        with pytest.raises(AssertionError, match=r"has 2 rows but min_rows=3"):
            f()

    def test_min_rows_passes_exact_count(self, make_df: DataFrameFactory) -> None:
        @df_out(min_rows=3)
        def f() -> Any:
            return make_df({"a": [1, 2, 3]})

        result = f()
        assert len(result) == 3

    def test_min_rows_passes_more_than_minimum(self, make_df: DataFrameFactory) -> None:
        @df_out(min_rows=2)
        def f() -> Any:
            return make_df({"a": [1, 2, 3, 4]})

        result = f()
        assert len(result) == 4

    def test_min_rows_rejects_empty_dataframe(self, make_df: DataFrameFactory) -> None:
        @df_out(min_rows=1)
        def f() -> Any:
            return make_df({"a": []})

        with pytest.raises(AssertionError, match=r"has 0 rows but min_rows=1"):
            f()

    def test_min_rows_zero_allows_empty(self, make_df: DataFrameFactory) -> None:
        @df_out(min_rows=0)
        def f() -> Any:
            return make_df({"a": []})

        result = f()
        assert len(result) == 0

    def test_min_rows_error_includes_function_name(self, make_df: DataFrameFactory) -> None:
        @df_out(min_rows=5)
        def my_transform() -> Any:
            return make_df({"a": [1]})

        with pytest.raises(AssertionError, match=r"function 'my_transform'"):
            my_transform()

    def test_min_rows_error_indicates_return_value(self, make_df: DataFrameFactory) -> None:
        @df_out(min_rows=5)
        def f() -> Any:
            return make_df({"a": [1]})

        with pytest.raises(AssertionError, match=r"return value"):
            f()


class TestMinRowsDfIn:
    def test_min_rows_rejects_too_few_rows(self, make_df: DataFrameFactory) -> None:
        @df_in(min_rows=3)
        def f(df: Any) -> Any:
            return df

        with pytest.raises(AssertionError, match=r"has 2 rows but min_rows=3"):
            f(make_df({"a": [1, 2]}))

    def test_min_rows_passes_exact_count(self, make_df: DataFrameFactory) -> None:
        @df_in(min_rows=3)
        def f(df: Any) -> Any:
            return df

        result = f(make_df({"a": [1, 2, 3]}))
        assert len(result) == 3

    def test_min_rows_passes_more_than_minimum(self, make_df: DataFrameFactory) -> None:
        @df_in(min_rows=2)
        def f(df: Any) -> Any:
            return df

        result = f(make_df({"a": [1, 2, 3, 4]}))
        assert len(result) == 4

    def test_min_rows_rejects_empty_dataframe(self, make_df: DataFrameFactory) -> None:
        @df_in(min_rows=1)
        def f(df: Any) -> Any:
            return df

        with pytest.raises(AssertionError, match=r"has 0 rows but min_rows=1"):
            f(make_df({"a": []}))

    def test_min_rows_error_includes_function_name(self, make_df: DataFrameFactory) -> None:
        @df_in(min_rows=5)
        def process_data(df: Any) -> Any:
            return df

        with pytest.raises(AssertionError, match=r"function 'process_data'"):
            process_data(make_df({"a": [1]}))

    def test_min_rows_error_includes_parameter_name(self, make_df: DataFrameFactory) -> None:
        @df_in(min_rows=5)
        def f(df: Any) -> Any:
            return df

        with pytest.raises(AssertionError, match=r"parameter 'df'"):
            f(make_df({"a": [1]}))


class TestMaxRowsDfOut:
    def test_max_rows_rejects_too_many_rows(self, make_df: DataFrameFactory) -> None:
        @df_out(max_rows=2)
        def f() -> Any:
            return make_df({"a": [1, 2, 3, 4]})

        with pytest.raises(AssertionError, match=r"has 4 rows but max_rows=2"):
            f()

    def test_max_rows_passes_exact_count(self, make_df: DataFrameFactory) -> None:
        @df_out(max_rows=3)
        def f() -> Any:
            return make_df({"a": [1, 2, 3]})

        result = f()
        assert len(result) == 3

    def test_max_rows_passes_fewer_than_maximum(self, make_df: DataFrameFactory) -> None:
        @df_out(max_rows=10)
        def f() -> Any:
            return make_df({"a": [1, 2]})

        result = f()
        assert len(result) == 2

    def test_max_rows_allows_empty_dataframe(self, make_df: DataFrameFactory) -> None:
        @df_out(max_rows=5)
        def f() -> Any:
            return make_df({"a": []})

        result = f()
        assert len(result) == 0


class TestMaxRowsDfIn:
    def test_max_rows_rejects_too_many_rows(self, make_df: DataFrameFactory) -> None:
        @df_in(max_rows=2)
        def f(df: Any) -> Any:
            return df

        with pytest.raises(AssertionError, match=r"has 4 rows but max_rows=2"):
            f(make_df({"a": [1, 2, 3, 4]}))

    def test_max_rows_passes_exact_count(self, make_df: DataFrameFactory) -> None:
        @df_in(max_rows=3)
        def f(df: Any) -> Any:
            return df

        result = f(make_df({"a": [1, 2, 3]}))
        assert len(result) == 3

    def test_max_rows_passes_fewer_than_maximum(self, make_df: DataFrameFactory) -> None:
        @df_in(max_rows=10)
        def f(df: Any) -> Any:
            return df

        result = f(make_df({"a": [1, 2]}))
        assert len(result) == 2


class TestMinMaxRowsCombined:
    def test_min_and_max_rows_passes_within_range(self, make_df: DataFrameFactory) -> None:
        @df_out(min_rows=2, max_rows=5)
        def f() -> Any:
            return make_df({"a": [1, 2, 3]})

        result = f()
        assert len(result) == 3

    def test_min_and_max_rows_rejects_below_min(self, make_df: DataFrameFactory) -> None:
        @df_out(min_rows=3, max_rows=10)
        def f() -> Any:
            return make_df({"a": [1, 2]})

        with pytest.raises(AssertionError, match=r"min_rows=3"):
            f()

    def test_min_and_max_rows_rejects_above_max(self, make_df: DataFrameFactory) -> None:
        @df_out(min_rows=1, max_rows=3)
        def f() -> Any:
            return make_df({"a": [1, 2, 3, 4, 5]})

        with pytest.raises(AssertionError, match=r"max_rows=3"):
            f()


class TestExactRowsDfOut:
    def test_exact_rows_rejects_too_few_rows(self, make_df: DataFrameFactory) -> None:
        @df_out(exact_rows=5)
        def f() -> Any:
            return make_df({"a": [1, 2, 3]})

        with pytest.raises(AssertionError, match=r"has 3 rows but exact_rows=5"):
            f()

    def test_exact_rows_rejects_too_many_rows(self, make_df: DataFrameFactory) -> None:
        @df_out(exact_rows=2)
        def f() -> Any:
            return make_df({"a": [1, 2, 3, 4]})

        with pytest.raises(AssertionError, match=r"has 4 rows but exact_rows=2"):
            f()

    def test_exact_rows_passes_exact_count(self, make_df: DataFrameFactory) -> None:
        @df_out(exact_rows=3)
        def f() -> Any:
            return make_df({"a": [1, 2, 3]})

        result = f()
        assert len(result) == 3

    def test_exact_rows_zero_requires_empty(self, make_df: DataFrameFactory) -> None:
        @df_out(exact_rows=0)
        def f() -> Any:
            return make_df({"a": []})

        result = f()
        assert len(result) == 0


class TestExactRowsDfIn:
    def test_exact_rows_rejects_wrong_count(self, make_df: DataFrameFactory) -> None:
        @df_in(exact_rows=3)
        def f(df: Any) -> Any:
            return df

        with pytest.raises(AssertionError, match=r"has 5 rows but exact_rows=3"):
            f(make_df({"a": [1, 2, 3, 4, 5]}))

    def test_exact_rows_passes_exact_count(self, make_df: DataFrameFactory) -> None:
        @df_in(exact_rows=4)
        def f(df: Any) -> Any:
            return df

        result = f(make_df({"a": [1, 2, 3, 4]}))
        assert len(result) == 4
