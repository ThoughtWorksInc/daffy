"""Row-level DataFrame validation using Pydantic models.

This module provides efficient row-by-row validation of DataFrames using Pydantic
models. It supports Pandas, Polars, Modin, and PyArrow DataFrames.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import narwhals as nw

from daffy.narwhals_compat import is_supported_dataframe
from daffy.pydantic_types import HAS_PYDANTIC, require_pydantic

_PYDANTIC_ROOT_FIELD = "__root__"

if TYPE_CHECKING:
    from pydantic import BaseModel, ValidationError  # noqa: F401

if HAS_PYDANTIC:
    from pydantic import ValidationError as PydanticValidationError
else:
    PydanticValidationError = None  # type: ignore[assignment, misc]  # pragma: no cover


def validate_dataframe_rows(
    df: Any,
    row_validator: type[BaseModel],
    max_errors: int = 5,
    early_termination: bool = True,
) -> None:
    """
    Validate DataFrame rows against a Pydantic model.

    Args:
        df: DataFrame to validate (Pandas, Polars, Modin, or PyArrow)
        row_validator: Pydantic BaseModel class for validation
        max_errors: Maximum number of errors to collect before stopping
        early_termination: Stop scanning after max_errors reached (faster for large datasets)

    Raises:
        AssertionError: If any rows fail validation (consistent with Daffy)
        ImportError: If Pydantic is not installed
        TypeError: If df is not a DataFrame
    """
    require_pydantic()

    if not is_supported_dataframe(df):
        raise TypeError(f"Expected DataFrame, got {type(df)}")

    if len(df) == 0:
        return

    _validate_optimized(df, row_validator, max_errors, early_termination)


def _validate_optimized(
    df: Any,
    row_validator: type[BaseModel],
    max_errors: int,
    early_termination: bool,
) -> None:
    """Optimized row-by-row validation using Narwhals for unified iteration."""
    failed_rows: list[tuple[Any, PydanticValidationError]] = []
    total_errors = 0

    # Unified iteration using Narwhals - works for pandas, polars, and future backends
    for idx, row in enumerate(nw.from_native(df, eager_only=True).iter_rows(named=True)):
        try:
            row_validator.model_validate(row)
        except PydanticValidationError as e:  # type: ignore[misc]
            total_errors += 1
            if len(failed_rows) < max_errors:
                failed_rows.append((idx, e))
            elif early_termination:
                break

    if failed_rows:
        _raise_validation_error(df, failed_rows, total_errors, early_termination)


def _raise_validation_error(
    df: Any,
    errors: list[tuple[Any, Any]],
    total_errors: int,
    stopped_early: bool,
) -> None:
    """Format and raise AssertionError with row validation details."""
    error_lines = [
        f"Row validation failed for {total_errors} out of {len(df)} rows:",
        "",
    ]

    for idx_label, error in errors:
        error_lines.append(f"  Row {idx_label}:")

        # error is always a Pydantic ValidationError with .errors() method
        for err_dict in error.errors():
            field = ".".join(str(loc) for loc in err_dict["loc"] if loc != _PYDANTIC_ROOT_FIELD)
            error_lines.append(f"    - {field}: {err_dict['msg']}" if field else f"    - {err_dict['msg']}")

        error_lines.append("")

    if stopped_early and total_errors > len(errors):
        error_lines.append(
            f"  ... stopped scanning early (at least {total_errors - len(errors)} more row(s) with errors)"
        )
    elif total_errors > len(errors):
        error_lines.append(f"  ... and {total_errors - len(errors)} more row(s) with errors")

    raise AssertionError("\n".join(error_lines))
