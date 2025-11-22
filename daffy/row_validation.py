"""Row-level DataFrame validation using Pydantic models.

This module provides efficient row-by-row validation of DataFrames using Pydantic
models. It supports both pandas and polars DataFrames and uses batch validation
with TypeAdapter for optimal performance.
"""

from __future__ import annotations

import math
from typing import TYPE_CHECKING, Any, Iterator

from daffy.dataframe_types import get_dataframe_types, is_pandas_dataframe, is_polars_dataframe
from daffy.pydantic_types import HAS_PYDANTIC, require_pydantic

if TYPE_CHECKING:
    from pydantic import BaseModel, ValidationError  # noqa: F401

    from daffy.dataframe_types import DataFrameType

if HAS_PYDANTIC:
    from pydantic import TypeAdapter
    from pydantic import ValidationError as PydanticValidationError
else:
    PydanticValidationError = None  # type: ignore[assignment, misc]
    TypeAdapter = None  # type: ignore[assignment, misc]


def _iterate_dataframe_with_index(df: DataFrameType) -> Iterator[tuple[Any, dict[str, Any]]]:
    """Iterate over DataFrame rows, yielding (index_label, row_dict) tuples."""
    if is_polars_dataframe(df):
        for idx, row in enumerate(df.iter_rows(named=True)):  # type: ignore[attr-defined]
            yield idx, row
    elif is_pandas_dataframe(df):
        for idx_label, row in df.iterrows():  # type: ignore[attr-defined]
            yield idx_label, row.to_dict()
    else:
        raise TypeError(f"Unknown DataFrame type: {type(df)}")


def _convert_nan_to_none(row_dict: dict[str, Any]) -> dict[str, Any]:
    """Convert NaN values to None for Pydantic compatibility."""
    return {k: (None if isinstance(v, float) and math.isnan(v) else v) for k, v in row_dict.items()}


def validate_dataframe_rows(
    df: DataFrameType,
    row_validator: type[BaseModel],
    max_errors: int = 5,
    convert_nans: bool = True,
) -> None:
    """
    Validate DataFrame rows against a Pydantic model using batch validation.

    This function validates all rows in a DataFrame against a Pydantic model.
    It uses batch validation with TypeAdapter for optimal performance, falling
    back to iterative validation if needed.

    Args:
        df: DataFrame to validate (pandas or polars)
        row_validator: Pydantic BaseModel class for validation
        max_errors: Maximum number of errors to collect before stopping
        convert_nans: Whether to convert NaN to None for Pydantic

    Raises:
        AssertionError: If any rows fail validation (consistent with Daffy)
        ImportError: If Pydantic is not installed
        TypeError: If df is not a DataFrame
    """
    require_pydantic()

    if not isinstance(df, get_dataframe_types()):
        raise TypeError(f"Expected DataFrame, got {type(df)}")

    if len(df) == 0:
        return

    try:
        _validate_batch(df, row_validator, max_errors, convert_nans)
    except (TypeError, AttributeError, KeyError):
        _validate_iterative(df, row_validator, max_errors, convert_nans)


def _validate_batch(
    df: DataFrameType,
    row_validator: type[BaseModel],
    max_errors: int,
    convert_nans: bool,
) -> None:
    """Batch validation using TypeAdapter."""
    if is_polars_dataframe(df):
        records = list(df.iter_rows(named=True))  # type: ignore[attr-defined]
    elif is_pandas_dataframe(df):
        records = df.to_dict("records")  # type: ignore[attr-defined]
    else:
        raise TypeError(f"Cannot convert {type(df)} to records")

    if convert_nans:
        records = [_convert_nan_to_none(r) for r in records]

    adapter = TypeAdapter(list[row_validator])  # type: ignore[misc]

    try:
        adapter.validate_python(records)
    except PydanticValidationError as e:  # type: ignore[misc]
        errors_by_row: list[tuple[Any, Any]] = []
        unique_row_indices: set[int] = set()
        all_errors = list(e.errors())  # type: ignore[misc]

        for error in all_errors:
            if error["loc"] and isinstance(error["loc"][0], int):
                unique_row_indices.add(error["loc"][0])

        for error in all_errors:
            if len(errors_by_row) >= max_errors:
                break

            if error["loc"] and isinstance(error["loc"][0], int):
                row_idx = error["loc"][0]
                idx_label = df.index[row_idx] if is_pandas_dataframe(df) else row_idx  # type: ignore[attr-defined]
                errors_by_row.append((idx_label, error))

        if errors_by_row:
            _raise_validation_error(df, errors_by_row, len(unique_row_indices))


def _validate_iterative(
    df: DataFrameType,
    row_validator: type[BaseModel],
    max_errors: int,
    convert_nans: bool,
) -> None:
    """Fallback iterative validation."""
    failed_rows: list[tuple[Any, PydanticValidationError]] = []

    for idx_label, row_dict in _iterate_dataframe_with_index(df):
        if convert_nans:
            row_dict = _convert_nan_to_none(row_dict)

        try:
            row_validator.model_validate(row_dict)
        except PydanticValidationError as e:  # type: ignore[misc]
            failed_rows.append((idx_label, e))
            if len(failed_rows) >= max_errors:
                break

    if failed_rows:
        _raise_validation_error(df, failed_rows, len(failed_rows))


def _raise_validation_error(
    df: DataFrameType,
    errors: list[tuple[Any, Any]],
    total_errors: int,
) -> None:
    """Format and raise AssertionError with row validation details."""
    error_lines = [
        f"Row validation failed for {total_errors} out of {len(df)} rows:",
        "",
    ]

    for idx_label, error in errors:
        error_lines.append(f"  Row {idx_label}:")

        if isinstance(error, dict):
            field_path = ".".join(str(x) for x in error["loc"][1:] if x != "__root__")
            error_lines.append(f"    - {field_path}: {error['msg']}" if field_path else f"    - {error['msg']}")
        elif hasattr(error, "errors"):
            for err_dict in error.errors():
                field = ".".join(str(loc) for loc in err_dict["loc"] if loc != "__root__")
                error_lines.append(f"    - {field}: {err_dict['msg']}" if field else f"    - {err_dict['msg']}")
        else:
            error_lines.append(f"    - {str(error)}")

        error_lines.append("")

    if total_errors > len(errors):
        error_lines.append(f"  ... and {total_errors - len(errors)} more row(s) with errors")

    raise AssertionError("\n".join(error_lines))
