"""Checks validator - value constraints."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from daffy.checks import validate_checks as run_checks
from daffy.config import get_checks_max_samples

if TYPE_CHECKING:
    from daffy.validators.context import ValidationContext


@dataclass
class ChecksValidator:
    """Validates column values against check constraints.

    Example:
        ChecksValidator({
            "price": {"gt": 0, "lt": 10000},
            "status": {"isin": ["active", "inactive"]},
        })

    """

    checks_by_column: dict[str, dict[str, Any]]
    max_samples: int | None = None

    def validate(self, ctx: ValidationContext) -> list[str]:
        errors = []
        max_samples = self.max_samples if self.max_samples is not None else get_checks_max_samples()

        for col, checks in self.checks_by_column.items():
            if not ctx.has_column(col):
                continue

            violations = run_checks(ctx.df, col, checks, max_samples)

            for col_name, check_name, fail_count, samples in violations:
                errors.append(
                    f"Column '{col_name}'{ctx.param_info} failed check {check_name}: "
                    f"{fail_count} values failed. Examples: {samples}"
                )

        return errors
