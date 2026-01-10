"""Shape validator - row count constraints."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from daffy.validators.context import ValidationContext


@dataclass
class ShapeValidator:
    min_rows: int | None = None
    max_rows: int | None = None
    exact_rows: int | None = None
    allow_empty: bool = True

    def validate(self, ctx: ValidationContext) -> list[str]:
        errors = []
        n = ctx.row_count

        if not self.allow_empty and n == 0:
            errors.append(f"DataFrame{ctx.param_info} is empty but allow_empty=False")

        if self.exact_rows is not None and n != self.exact_rows:
            errors.append(f"DataFrame{ctx.param_info} has {n} rows but exact_rows={self.exact_rows}")

        if self.min_rows is not None and n < self.min_rows:
            errors.append(f"DataFrame{ctx.param_info} has {n} rows but min_rows={self.min_rows}")

        if self.max_rows is not None and n > self.max_rows:
            errors.append(f"DataFrame{ctx.param_info} has {n} rows but max_rows={self.max_rows}")

        return errors
