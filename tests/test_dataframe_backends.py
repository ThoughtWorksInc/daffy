"""Tests for DataFrame backend compatibility (pandas, polars, modin, pyarrow)."""

from typing import Any, Callable

import pytest

from daffy import df_in, df_out

cars = {
    "Brand": ["Honda Civic", "Toyota Corolla", "Ford Focus", "Audi A4"],
    "Price": [22000, 25000, 27000, 35000],
}


class TestBackendCompatibility:
    """Test core daffy functionality across all supported DataFrame backends."""

    def test_df_out_validates_columns(self, make_df: Callable[[dict[str, Any]], Any]) -> None:
        @df_out(columns=["Brand", "Price"])
        def get_data() -> Any:
            return make_df(cars)

        result = get_data()
        assert result is not None

    def test_df_in_validates_columns(self, make_df: Callable[[dict[str, Any]], Any]) -> None:
        @df_in(columns=["Brand", "Price"])
        def process(df: Any) -> Any:
            return df

        result = process(make_df(cars))
        assert result is not None

    def test_strict_mode_rejects_extra_columns(self, make_df: Callable[[dict[str, Any]], Any]) -> None:
        @df_in(columns=["Brand"], strict=True)
        def process(df: Any) -> Any:
            return df

        with pytest.raises(AssertionError, match="unexpected column"):
            process(make_df(cars))

    def test_missing_column_raises_error(self, make_df: Callable[[dict[str, Any]], Any]) -> None:
        @df_in(columns=["Brand", "Price", "NonExistent"])
        def process(df: Any) -> Any:
            return df

        with pytest.raises(AssertionError, match="NonExistent"):
            process(make_df(cars))

    def test_regex_column_pattern(self, make_df: Callable[[dict[str, Any]], Any]) -> None:
        @df_in(columns=["r/.*/"])
        def process(df: Any) -> Any:
            return df

        result = process(make_df(cars))
        assert result is not None
