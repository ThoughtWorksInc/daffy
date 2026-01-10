"""Row validator - Pydantic model validation."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from daffy.config import get_row_validation_max_errors
from daffy.pydantic_types import require_pydantic

if TYPE_CHECKING:
    from daffy.validators.context import ValidationContext


@dataclass
class RowValidator:
    """Validates each row against a Pydantic model.

    Example:
        class UserRow(BaseModel):
            id: int
            email: str

        RowValidator(UserRow, max_errors=10)

    """

    model: type
    max_errors: int | None = None
    early_termination: bool = True

    def validate(self, ctx: ValidationContext) -> list[str]:
        require_pydantic()
        from pydantic import ValidationError  # noqa: PLC0415

        if ctx.row_count == 0:
            return []

        max_errors = self.max_errors if self.max_errors is not None else get_row_validation_max_errors()
        failed_rows: list[tuple[int, Any]] = []
        total_errors = 0

        for idx, row in enumerate(ctx.nw_df.iter_rows(named=True)):
            try:
                self.model.model_validate(row)  # pyright: ignore[reportAttributeAccessIssue]
            except ValidationError as e:  # noqa: PERF203
                total_errors += 1
                if len(failed_rows) < max_errors:
                    failed_rows.append((idx, e))
                elif self.early_termination:
                    break

        if not failed_rows:
            return []

        return [self._format_errors(failed_rows, total_errors, ctx)]

    def _format_errors(
        self,
        failed_rows: list[tuple[int, Any]],
        total_errors: int,
        ctx: ValidationContext,
    ) -> str:
        lines = [f"Row validation failed for {total_errors} out of {ctx.row_count} rows:", ""]

        for idx, error in failed_rows:
            lines.append(f"  Row {idx}:")
            for err in error.errors():
                field = ".".join(str(loc) for loc in err["loc"])
                lines.append(f"    - {field}: {err['msg']}" if field else f"    - {err['msg']}")
            lines.append("")

        if total_errors > len(failed_rows):
            lines.append(f"  ... and {total_errors - len(failed_rows)} more row(s) with errors")

        return "\n".join(lines) + ctx.param_info
