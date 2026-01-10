"""Tests for RowValidator."""

import pandas as pd
from pydantic import BaseModel, Field

from daffy.validators.context import ValidationContext
from daffy.validators.rows import RowValidator


class SimpleModel(BaseModel):
    id: int
    name: str


class StrictModel(BaseModel):
    value: int = Field(ge=0, le=100)


class TestRowValidator:
    def test_passes_when_all_rows_valid(self) -> None:
        ctx = ValidationContext(df=pd.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"]}))
        validator = RowValidator(model=SimpleModel)

        assert validator.validate(ctx) == []

    def test_fails_when_row_invalid(self) -> None:
        ctx = ValidationContext(df=pd.DataFrame({"id": [1, "not_int", 3], "name": ["a", "b", "c"]}))
        validator = RowValidator(model=SimpleModel)

        errors = validator.validate(ctx)
        assert len(errors) == 1
        assert "Row validation failed" in errors[0]

    def test_reports_row_index(self) -> None:
        ctx = ValidationContext(df=pd.DataFrame({"id": [1, "bad", 3], "name": ["a", "b", "c"]}))
        validator = RowValidator(model=SimpleModel)

        errors = validator.validate(ctx)
        assert "Row 1:" in errors[0]

    def test_respects_max_errors(self) -> None:
        ctx = ValidationContext(df=pd.DataFrame({"id": ["a", "b", "c", "d", "e"], "name": ["x"] * 5}))
        validator = RowValidator(model=SimpleModel, max_errors=2)

        errors = validator.validate(ctx)
        assert "Row 0:" in errors[0]
        assert "Row 1:" in errors[0]
        assert "Row 2:" not in errors[0]

    def test_reports_additional_errors(self) -> None:
        ctx = ValidationContext(df=pd.DataFrame({"id": ["a", "b", "c", "d", "e"], "name": ["x"] * 5}))
        validator = RowValidator(model=SimpleModel, max_errors=2, early_termination=False)

        errors = validator.validate(ctx)
        assert "... and 3 more row(s)" in errors[0]

    def test_empty_dataframe_passes(self) -> None:
        ctx = ValidationContext(df=pd.DataFrame({"id": [], "name": []}))
        validator = RowValidator(model=SimpleModel)

        assert validator.validate(ctx) == []

    def test_field_constraints(self) -> None:
        ctx = ValidationContext(df=pd.DataFrame({"value": [50, 200]}))
        validator = RowValidator(model=StrictModel)

        errors = validator.validate(ctx)
        assert len(errors) == 1
        assert "Row 1:" in errors[0]

    def test_includes_error_details(self) -> None:
        ctx = ValidationContext(df=pd.DataFrame({"id": ["bad"], "name": ["x"]}))
        validator = RowValidator(model=SimpleModel)

        errors = validator.validate(ctx)
        assert "id:" in errors[0]

    def test_includes_param_info(self) -> None:
        ctx = ValidationContext(
            df=pd.DataFrame({"id": ["bad"], "name": ["x"]}),
            func_name="my_func",
            is_return_value=True,
        )
        validator = RowValidator(model=SimpleModel)

        errors = validator.validate(ctx)
        assert "my_func" in errors[0]

    def test_early_termination_default(self) -> None:
        ctx = ValidationContext(df=pd.DataFrame({"id": ["a"] * 100, "name": ["x"] * 100}))
        validator = RowValidator(model=SimpleModel, max_errors=2)

        errors = validator.validate(ctx)
        assert "5 out of 100 rows" not in errors[0]

    def test_early_termination_disabled(self) -> None:
        ctx = ValidationContext(df=pd.DataFrame({"id": ["a"] * 10, "name": ["x"] * 10}))
        validator = RowValidator(model=SimpleModel, max_errors=2, early_termination=False)

        errors = validator.validate(ctx)
        assert "10 out of 10 rows" in errors[0]
