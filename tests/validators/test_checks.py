"""Tests for ChecksValidator."""

import pandas as pd

from daffy.validators.checks import ChecksValidator
from daffy.validators.context import ValidationContext


class TestChecksValidator:
    def test_passes_when_checks_satisfied(self) -> None:
        ctx = ValidationContext(df=pd.DataFrame({"price": [10, 20, 30]}))
        validator = ChecksValidator({"price": {"gt": 0}})

        assert validator.validate(ctx) == []

    def test_fails_when_check_violated(self) -> None:
        ctx = ValidationContext(df=pd.DataFrame({"price": [-5, 10, 20]}))
        validator = ChecksValidator({"price": {"gt": 0}})

        errors = validator.validate(ctx)
        assert len(errors) == 1
        assert "failed check" in errors[0].lower()

    def test_skips_missing_columns(self) -> None:
        ctx = ValidationContext(df=pd.DataFrame({"a": [1]}))
        validator = ChecksValidator({"nonexistent": {"gt": 0}})

        assert validator.validate(ctx) == []

    def test_multiple_checks_per_column(self) -> None:
        ctx = ValidationContext(df=pd.DataFrame({"price": [5, 10, 15]}))
        validator = ChecksValidator({"price": {"gt": 0, "lt": 100}})

        assert validator.validate(ctx) == []

    def test_multiple_columns(self) -> None:
        ctx = ValidationContext(df=pd.DataFrame({"a": [-1], "b": [-1]}))
        validator = ChecksValidator({"a": {"gt": 0}, "b": {"gt": 0}})

        errors = validator.validate(ctx)
        assert len(errors) == 1
        assert "Check violations" in errors[0]
        assert "a" in errors[0]
        assert "b" in errors[0]

    def test_uses_config_max_samples_when_not_set(self) -> None:
        ctx = ValidationContext(df=pd.DataFrame({"price": list(range(100))}))
        validator = ChecksValidator({"price": {"gt": 50}})

        errors = validator.validate(ctx)
        assert len(errors) == 1
        assert "Examples:" in errors[0]
