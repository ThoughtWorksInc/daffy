"""Tests for pipeline builder."""

import pandas as pd
import pytest

from daffy.validators.builder import build_validation_pipeline
from daffy.validators.checks import ChecksValidator
from daffy.validators.columns import (
    ColumnsExistValidator,
    DtypeValidator,
    NullableValidator,
    StrictModeValidator,
)
from daffy.validators.context import ValidationContext
from daffy.validators.rows import RowValidator
from daffy.validators.shape import ShapeValidator
from daffy.validators.uniqueness import CompositeUniqueValidator, UniqueValidator


class TestBuildValidationPipeline:
    def test_empty_pipeline_when_no_constraints(self) -> None:
        pipeline = build_validation_pipeline(
            columns=None,
            strict=False,
            lazy=False,
            composite_unique=None,
            row_validator=None,
            min_rows=None,
            max_rows=None,
            exact_rows=None,
            allow_empty=True,
            df_columns=["a", "b"],
        )

        assert len(pipeline) == 0

    def test_adds_shape_validator_for_min_rows(self) -> None:
        pipeline = build_validation_pipeline(
            columns=None,
            strict=False,
            lazy=False,
            composite_unique=None,
            row_validator=None,
            min_rows=5,
            max_rows=None,
            exact_rows=None,
            allow_empty=True,
            df_columns=[],
        )

        assert len(pipeline) == 1
        assert isinstance(pipeline.validators[0], ShapeValidator)

    def test_adds_shape_validator_for_allow_empty_false(self) -> None:
        pipeline = build_validation_pipeline(
            columns=None,
            strict=False,
            lazy=False,
            composite_unique=None,
            row_validator=None,
            min_rows=None,
            max_rows=None,
            exact_rows=None,
            allow_empty=False,
            df_columns=[],
        )

        assert len(pipeline) == 1
        assert isinstance(pipeline.validators[0], ShapeValidator)

    def test_adds_columns_exist_validator(self) -> None:
        pipeline = build_validation_pipeline(
            columns=["a", "b"],
            strict=False,
            lazy=False,
            composite_unique=None,
            row_validator=None,
            min_rows=None,
            max_rows=None,
            exact_rows=None,
            allow_empty=True,
            df_columns=["a", "b"],
        )

        assert any(isinstance(v, ColumnsExistValidator) for v in pipeline.validators)

    def test_adds_dtype_validator(self) -> None:
        pipeline = build_validation_pipeline(
            columns={"a": "int64"},
            strict=False,
            lazy=False,
            composite_unique=None,
            row_validator=None,
            min_rows=None,
            max_rows=None,
            exact_rows=None,
            allow_empty=True,
            df_columns=["a"],
        )

        assert any(isinstance(v, DtypeValidator) for v in pipeline.validators)

    def test_adds_nullable_validator(self) -> None:
        pipeline = build_validation_pipeline(
            columns={"a": {"nullable": False}},
            strict=False,
            lazy=False,
            composite_unique=None,
            row_validator=None,
            min_rows=None,
            max_rows=None,
            exact_rows=None,
            allow_empty=True,
            df_columns=["a"],
        )

        assert any(isinstance(v, NullableValidator) for v in pipeline.validators)

    def test_adds_unique_validator(self) -> None:
        pipeline = build_validation_pipeline(
            columns={"a": {"unique": True}},
            strict=False,
            lazy=False,
            composite_unique=None,
            row_validator=None,
            min_rows=None,
            max_rows=None,
            exact_rows=None,
            allow_empty=True,
            df_columns=["a"],
        )

        assert any(isinstance(v, UniqueValidator) for v in pipeline.validators)

    def test_adds_checks_validator(self) -> None:
        pipeline = build_validation_pipeline(
            columns={"a": {"checks": {"gt": 0}}},
            strict=False,
            lazy=False,
            composite_unique=None,
            row_validator=None,
            min_rows=None,
            max_rows=None,
            exact_rows=None,
            allow_empty=True,
            df_columns=["a"],
        )

        assert any(isinstance(v, ChecksValidator) for v in pipeline.validators)

    def test_adds_strict_mode_validator(self) -> None:
        pipeline = build_validation_pipeline(
            columns=["a"],
            strict=True,
            lazy=False,
            composite_unique=None,
            row_validator=None,
            min_rows=None,
            max_rows=None,
            exact_rows=None,
            allow_empty=True,
            df_columns=["a"],
        )

        assert any(isinstance(v, StrictModeValidator) for v in pipeline.validators)

    def test_adds_composite_unique_validator(self) -> None:
        pipeline = build_validation_pipeline(
            columns=None,
            strict=False,
            lazy=False,
            composite_unique=[["a", "b"]],
            row_validator=None,
            min_rows=None,
            max_rows=None,
            exact_rows=None,
            allow_empty=True,
            df_columns=["a", "b"],
        )

        assert any(isinstance(v, CompositeUniqueValidator) for v in pipeline.validators)

    def test_adds_row_validator(self) -> None:
        from pydantic import BaseModel

        class TestModel(BaseModel):
            a: int

        pipeline = build_validation_pipeline(
            columns=None,
            strict=False,
            lazy=False,
            composite_unique=None,
            row_validator=TestModel,
            min_rows=None,
            max_rows=None,
            exact_rows=None,
            allow_empty=True,
            df_columns=["a"],
        )

        assert any(isinstance(v, RowValidator) for v in pipeline.validators)

    def test_sets_lazy_mode(self) -> None:
        pipeline = build_validation_pipeline(
            columns=None,
            strict=False,
            lazy=True,
            composite_unique=None,
            row_validator=None,
            min_rows=None,
            max_rows=None,
            exact_rows=None,
            allow_empty=True,
            df_columns=[],
        )

        assert pipeline.lazy is True


class TestRegexExpansion:
    def test_expands_regex_columns(self) -> None:
        pipeline = build_validation_pipeline(
            columns={"r/col_\\d+/": {"nullable": False}},
            strict=False,
            lazy=False,
            composite_unique=None,
            row_validator=None,
            min_rows=None,
            max_rows=None,
            exact_rows=None,
            allow_empty=True,
            df_columns=["col_1", "col_2", "col_3"],
        )

        nullable_validator = next(v for v in pipeline.validators if isinstance(v, NullableValidator))
        assert set(nullable_validator.non_nullable_columns) == {"col_1", "col_2", "col_3"}


class TestPipelineIntegration:
    def test_full_pipeline_execution(self) -> None:
        from pydantic import BaseModel

        class RowModel(BaseModel):
            id: int
            name: str

        pipeline = build_validation_pipeline(
            columns={"id": {"dtype": "Int64", "unique": True}, "name": {"nullable": False}},
            strict=True,
            lazy=False,
            composite_unique=None,
            row_validator=RowModel,
            min_rows=1,
            max_rows=100,
            exact_rows=None,
            allow_empty=False,
            df_columns=["id", "name"],
        )

        ctx = ValidationContext(df=pd.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"]}))
        pipeline.run(ctx)

    def test_pipeline_fails_on_invalid_data(self) -> None:
        pipeline = build_validation_pipeline(
            columns=["a", "b"],
            strict=False,
            lazy=False,
            composite_unique=None,
            row_validator=None,
            min_rows=None,
            max_rows=None,
            exact_rows=None,
            allow_empty=True,
            df_columns=["a"],
        )

        ctx = ValidationContext(df=pd.DataFrame({"a": [1]}))

        with pytest.raises(AssertionError, match="Missing columns"):
            pipeline.run(ctx)
