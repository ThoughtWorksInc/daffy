"""Integration tests for value checks with df_in/df_out decorators."""

from typing import Any

import pandas as pd
import polars as pl
import pytest

from daffy import df_in, df_out


class TestDfInWithChecks:
    def test_gt_check_passes(self) -> None:
        @df_in(columns={"price": {"checks": {"gt": 0}}})
        def process(df: pd.DataFrame) -> pd.DataFrame:
            return df

        df = pd.DataFrame({"price": [1, 2, 3]})
        result = process(df)
        assert list(result["price"]) == [1, 2, 3]

    def test_gt_check_fails(self) -> None:
        @df_in(columns={"price": {"checks": {"gt": 0}}})
        def process(df: pd.DataFrame) -> pd.DataFrame:
            return df

        df = pd.DataFrame({"price": [0, 1, 2]})
        with pytest.raises(AssertionError, match="gt"):
            process(df)

    def test_multiple_checks_pass(self) -> None:
        @df_in(columns={"score": {"checks": {"gt": 0, "lt": 100}}})
        def process(df: pd.DataFrame) -> pd.DataFrame:
            return df

        df = pd.DataFrame({"score": [50, 60, 70]})
        result = process(df)
        assert list(result["score"]) == [50, 60, 70]

    def test_multiple_checks_fail(self) -> None:
        @df_in(columns={"score": {"checks": {"gt": 0, "lt": 100}}})
        def process(df: pd.DataFrame) -> pd.DataFrame:
            return df

        df = pd.DataFrame({"score": [50, 150, 70]})
        with pytest.raises(AssertionError, match="lt"):
            process(df)

    def test_isin_check_passes(self) -> None:
        @df_in(columns={"status": {"checks": {"isin": ["active", "pending", "closed"]}}})
        def process(df: pd.DataFrame) -> pd.DataFrame:
            return df

        df = pd.DataFrame({"status": ["active", "pending", "closed"]})
        result = process(df)
        assert list(result["status"]) == ["active", "pending", "closed"]

    def test_isin_check_fails(self) -> None:
        @df_in(columns={"status": {"checks": {"isin": ["active", "pending", "closed"]}}})
        def process(df: pd.DataFrame) -> pd.DataFrame:
            return df

        df = pd.DataFrame({"status": ["active", "deleted", "closed"]})
        with pytest.raises(AssertionError, match="isin"):
            process(df)

    def test_between_check(self) -> None:
        @df_in(columns={"age": {"checks": {"between": (0, 120)}}})
        def process(df: pd.DataFrame) -> pd.DataFrame:
            return df

        df = pd.DataFrame({"age": [25, 50, 75]})
        result = process(df)
        assert list(result["age"]) == [25, 50, 75]

    def test_checks_with_dtype(self) -> None:
        @df_in(columns={"price": {"dtype": "float64", "checks": {"gt": 0}}})
        def process(df: pd.DataFrame) -> pd.DataFrame:
            return df

        df = pd.DataFrame({"price": [1.0, 2.0, 3.0]})
        result = process(df)
        assert list(result["price"]) == [1.0, 2.0, 3.0]

    def test_checks_with_nullable(self) -> None:
        @df_in(columns={"price": {"nullable": False, "checks": {"gt": 0}}})
        def process(df: pd.DataFrame) -> pd.DataFrame:
            return df

        df = pd.DataFrame({"price": [1.0, 2.0, 3.0]})
        result = process(df)
        assert list(result["price"]) == [1.0, 2.0, 3.0]


class TestDfOutWithChecks:
    def test_check_passes(self) -> None:
        @df_out(columns={"result": {"checks": {"ge": 0}}})
        def process(df: pd.DataFrame) -> pd.DataFrame:
            return df

        df = pd.DataFrame({"result": [0, 1, 2]})
        result = process(df)
        assert list(result["result"]) == [0, 1, 2]

    def test_check_fails(self) -> None:
        @df_out(columns={"result": {"checks": {"ge": 0}}})
        def process(df: pd.DataFrame) -> pd.DataFrame:
            return df

        df = pd.DataFrame({"result": [-1, 0, 1]})
        with pytest.raises(AssertionError, match="ge"):
            process(df)


@pytest.mark.parametrize("df_lib", [pd, pl], ids=["pandas", "polars"])
class TestChecksWithBothLibraries:
    def test_gt_check_passes(self, df_lib: Any) -> None:
        @df_in(columns={"price": {"checks": {"gt": 0}}})
        def process(df: Any) -> Any:
            return df

        df = df_lib.DataFrame({"price": [1, 2, 3]})
        result = process(df)
        assert list(result["price"]) == [1, 2, 3]

    def test_gt_check_fails(self, df_lib: Any) -> None:
        @df_in(columns={"price": {"checks": {"gt": 0}}})
        def process(df: Any) -> Any:
            return df

        df = df_lib.DataFrame({"price": [0, 1, 2]})
        with pytest.raises(AssertionError, match="gt"):
            process(df)

    def test_between_check(self, df_lib: Any) -> None:
        @df_in(columns={"score": {"checks": {"between": (0, 100)}}})
        def process(df: Any) -> Any:
            return df

        df = df_lib.DataFrame({"score": [0, 50, 100]})
        result = process(df)
        assert list(result["score"]) == [0, 50, 100]

    def test_isin_check(self, df_lib: Any) -> None:
        @df_in(columns={"status": {"checks": {"isin": ["a", "b", "c"]}}})
        def process(df: Any) -> Any:
            return df

        df = df_lib.DataFrame({"status": ["a", "b", "c"]})
        result = process(df)
        assert list(result["status"]) == ["a", "b", "c"]

    def test_notnull_check_passes(self, df_lib: Any) -> None:
        @df_in(columns={"value": {"checks": {"notnull": True}}})
        def process(df: Any) -> Any:
            return df

        df = df_lib.DataFrame({"value": [1, 2, 3]})
        result = process(df)
        assert list(result["value"]) == [1, 2, 3]

    def test_notnull_check_fails(self, df_lib: Any) -> None:
        @df_in(columns={"value": {"checks": {"notnull": True}}})
        def process(df: Any) -> Any:
            return df

        df = df_lib.DataFrame({"value": [1, None, 3]})
        with pytest.raises(AssertionError, match="notnull"):
            process(df)

    def test_eq_check(self, df_lib: Any) -> None:
        @df_in(columns={"flag": {"checks": {"eq": "yes"}}})
        def process(df: Any) -> Any:
            return df

        df = df_lib.DataFrame({"flag": ["yes", "yes", "yes"]})
        result = process(df)
        assert list(result["flag"]) == ["yes", "yes", "yes"]

    def test_ne_check(self, df_lib: Any) -> None:
        @df_in(columns={"flag": {"checks": {"ne": "deleted"}}})
        def process(df: Any) -> Any:
            return df

        df = df_lib.DataFrame({"flag": ["active", "pending", "closed"]})
        result = process(df)
        assert list(result["flag"]) == ["active", "pending", "closed"]

    def test_str_regex_check(self, df_lib: Any) -> None:
        @df_in(columns={"code": {"checks": {"str_regex": r"^[A-Z]{2}\d{3}$"}}})
        def process(df: Any) -> Any:
            return df

        df = df_lib.DataFrame({"code": ["AB123", "CD456", "EF789"]})
        result = process(df)
        assert list(result["code"]) == ["AB123", "CD456", "EF789"]


class TestErrorMessages:
    def test_error_includes_column_name(self) -> None:
        @df_in(columns={"price": {"checks": {"gt": 0}}})
        def process(df: pd.DataFrame) -> pd.DataFrame:
            return df

        df = pd.DataFrame({"price": [0]})
        with pytest.raises(AssertionError, match="price"):
            process(df)

    def test_error_includes_check_name(self) -> None:
        @df_in(columns={"value": {"checks": {"lt": 100}}})
        def process(df: pd.DataFrame) -> pd.DataFrame:
            return df

        df = pd.DataFrame({"value": [150]})
        with pytest.raises(AssertionError, match="lt"):
            process(df)

    def test_error_includes_sample_values(self) -> None:
        @df_in(columns={"x": {"checks": {"gt": 0}}})
        def process(df: pd.DataFrame) -> pd.DataFrame:
            return df

        df = pd.DataFrame({"x": [-5, 1, 2]})
        with pytest.raises(AssertionError, match="-5"):
            process(df)
