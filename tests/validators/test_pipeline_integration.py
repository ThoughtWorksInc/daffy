"""Integration tests for the validation pipeline with decorators."""

from typing import Any

import pandas as pd
import pytest

from daffy import df_in, df_out
from daffy.config import clear_config_cache


@pytest.fixture(autouse=True)
def use_pipeline(monkeypatch: pytest.MonkeyPatch) -> None:
    """Enable pipeline mode for all tests in this file."""
    from daffy import config

    clear_config_cache()
    monkeypatch.setattr(config, "get_use_pipeline", lambda: True)
    yield
    clear_config_cache()


class TestPipelineDecorators:
    def test_df_out_basic_columns(self) -> None:
        @df_out(columns=["a", "b"])
        def f() -> Any:
            return pd.DataFrame({"a": [1], "b": [2]})

        assert len(f()) == 1

    def test_df_out_missing_columns(self) -> None:
        @df_out(columns=["a", "b", "c"])
        def f() -> Any:
            return pd.DataFrame({"a": [1]})

        with pytest.raises(AssertionError, match="Missing columns"):
            f()

    def test_df_out_strict_mode(self) -> None:
        @df_out(columns=["a"], strict=True)
        def f() -> Any:
            return pd.DataFrame({"a": [1], "b": [2]})

        with pytest.raises(AssertionError, match="unexpected"):
            f()

    def test_df_out_min_rows(self) -> None:
        @df_out(min_rows=5)
        def f() -> Any:
            return pd.DataFrame({"a": [1, 2]})

        with pytest.raises(AssertionError, match="min_rows=5"):
            f()

    def test_df_out_nullable(self) -> None:
        @df_out(columns={"a": {"nullable": False}})
        def f() -> Any:
            return pd.DataFrame({"a": [1, None, 3]})

        with pytest.raises(AssertionError, match="null"):
            f()

    def test_df_out_unique(self) -> None:
        @df_out(columns={"a": {"unique": True}})
        def f() -> Any:
            return pd.DataFrame({"a": [1, 1, 2]})

        with pytest.raises(AssertionError, match="duplicate"):
            f()

    def test_df_out_composite_unique(self) -> None:
        @df_out(composite_unique=[["a", "b"]])
        def f() -> Any:
            return pd.DataFrame({"a": [1, 1, 2], "b": ["x", "x", "y"]})

        with pytest.raises(AssertionError, match="duplicate"):
            f()

    def test_df_out_allow_empty_false(self) -> None:
        @df_out(allow_empty=False)
        def f() -> Any:
            return pd.DataFrame({"a": []})

        with pytest.raises(AssertionError, match="allow_empty=False"):
            f()

    def test_df_out_checks(self) -> None:
        @df_out(columns={"price": {"checks": {"gt": 0}}})
        def f() -> Any:
            return pd.DataFrame({"price": [-5, 10, 20]})

        with pytest.raises(AssertionError, match="failed check"):
            f()

    def test_df_in_basic(self) -> None:
        @df_in(columns=["a", "b"])
        def f(df: Any) -> Any:
            return df

        result = f(pd.DataFrame({"a": [1], "b": [2]}))
        assert len(result) == 1

    def test_df_in_missing_columns(self) -> None:
        @df_in(columns=["a", "b", "c"])
        def f(df: Any) -> Any:
            return df

        with pytest.raises(AssertionError, match="Missing columns"):
            f(pd.DataFrame({"a": [1]}))

    def test_lazy_mode_collects_errors(self) -> None:
        @df_out(columns=["a", "b"], min_rows=5, lazy=True)
        def f() -> Any:
            return pd.DataFrame({"a": [1, 2]})

        with pytest.raises(AssertionError) as exc_info:
            f()

        error = str(exc_info.value)
        assert "min_rows=5" in error
        assert "Missing columns" in error


class TestPipelineRowValidation:
    def test_row_validation(self) -> None:
        from pydantic import BaseModel

        class RowModel(BaseModel):
            id: int
            name: str

        @df_out(row_validator=RowModel)
        def f() -> Any:
            return pd.DataFrame({"id": [1, "bad", 3], "name": ["a", "b", "c"]})

        with pytest.raises(AssertionError, match="Row validation failed"):
            f()
