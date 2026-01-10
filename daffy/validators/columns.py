"""Column validators - existence, dtype, nullable, strict mode."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from daffy.validators.columns_resolver import ResolvedColumns
    from daffy.validators.context import ValidationContext


@dataclass
class ColumnsExistValidator:
    """Validates that required columns exist."""

    resolved: ResolvedColumns

    def validate(self, ctx: ValidationContext) -> list[str]:
        if self.resolved.missing_specs:
            return [f"Missing columns: {self.resolved.missing_specs}{ctx.param_info}. Got columns: {list(ctx.columns)}"]
        return []


@dataclass
class DtypeValidator:
    """Validates column data types."""

    expected: dict[str, Any]

    def validate(self, ctx: ValidationContext) -> list[str]:
        errors = []

        for col, expected_dtype in self.expected.items():
            if ctx.has_column(col):
                actual = ctx.get_dtype(col)
                if str(actual) != str(expected_dtype):
                    errors.append(
                        f"Column {col}{ctx.param_info} has wrong dtype. Was {actual}, expected {expected_dtype}"
                    )

        return errors


@dataclass
class NullableValidator:
    """Validates columns don't contain null values."""

    non_nullable_columns: list[str]

    def validate(self, ctx: ValidationContext) -> list[str]:
        errors = []

        for col in self.non_nullable_columns:
            if ctx.has_column(col):
                null_count = int(ctx.get_series(col).is_null().sum())
                if null_count > 0:
                    errors.append(
                        f"Column '{col}'{ctx.param_info} contains {null_count} null values but nullable=False"
                    )

        return errors


@dataclass
class StrictModeValidator:
    """Validates no unexpected columns exist."""

    allowed_columns: set[str]

    def validate(self, ctx: ValidationContext) -> list[str]:
        extra = ctx.column_set - self.allowed_columns

        if extra:
            return [f"DataFrame{ctx.param_info} contained unexpected column(s): {', '.join(sorted(extra))}"]

        return []
