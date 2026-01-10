"""Tests for uniqueness validators."""

import pandas as pd

from daffy.validators.context import ValidationContext
from daffy.validators.uniqueness import CompositeUniqueValidator, UniqueValidator


class TestUniqueValidator:
    def test_passes_when_all_unique(self) -> None:
        ctx = ValidationContext(df=pd.DataFrame({"a": [1, 2, 3]}))
        validator = UniqueValidator(["a"])

        assert validator.validate(ctx) == []

    def test_fails_when_duplicates(self) -> None:
        ctx = ValidationContext(df=pd.DataFrame({"a": [1, 1, 2]}))
        validator = UniqueValidator(["a"])

        errors = validator.validate(ctx)
        assert len(errors) == 1
        assert "duplicate" in errors[0].lower()
        assert "unique=True" in errors[0]

    def test_reports_duplicate_count(self) -> None:
        ctx = ValidationContext(df=pd.DataFrame({"a": [1, 1, 1, 2, 2]}))
        validator = UniqueValidator(["a"])

        errors = validator.validate(ctx)
        assert "3 duplicate values" in errors[0]

    def test_skips_missing_columns(self) -> None:
        ctx = ValidationContext(df=pd.DataFrame({"a": [1, 1]}))
        validator = UniqueValidator(["nonexistent"])

        assert validator.validate(ctx) == []

    def test_multiple_columns(self) -> None:
        ctx = ValidationContext(df=pd.DataFrame({"a": [1, 1], "b": [1, 1]}))
        validator = UniqueValidator(["a", "b"])

        errors = validator.validate(ctx)
        assert len(errors) == 2


class TestCompositeUniqueValidator:
    def test_passes_when_combinations_unique(self) -> None:
        ctx = ValidationContext(df=pd.DataFrame({"a": [1, 1, 2], "b": ["x", "y", "x"]}))
        validator = CompositeUniqueValidator([["a", "b"]])

        assert validator.validate(ctx) == []

    def test_fails_when_combinations_duplicate(self) -> None:
        ctx = ValidationContext(df=pd.DataFrame({"a": [1, 1, 2], "b": ["x", "x", "y"]}))
        validator = CompositeUniqueValidator([["a", "b"]])

        errors = validator.validate(ctx)
        assert len(errors) == 1
        assert "duplicate" in errors[0].lower()

    def test_reports_duplicate_count(self) -> None:
        ctx = ValidationContext(df=pd.DataFrame({"a": [1, 1, 1], "b": ["x", "x", "x"]}))
        validator = CompositeUniqueValidator([["a", "b"]])

        errors = validator.validate(ctx)
        assert "2 duplicate" in errors[0]

    def test_fails_when_column_missing(self) -> None:
        ctx = ValidationContext(df=pd.DataFrame({"a": [1, 2]}))
        validator = CompositeUniqueValidator([["a", "nonexistent"]])

        errors = validator.validate(ctx)
        assert len(errors) == 1
        assert "missing columns" in errors[0].lower()

    def test_multiple_combinations(self) -> None:
        ctx = ValidationContext(df=pd.DataFrame({"a": [1, 1], "b": [1, 1], "c": [1, 1]}))
        validator = CompositeUniqueValidator([["a", "b"], ["b", "c"]])

        errors = validator.validate(ctx)
        assert len(errors) == 2
