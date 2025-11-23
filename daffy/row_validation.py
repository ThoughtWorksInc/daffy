"""Row-level DataFrame validation using Pydantic models.

This module provides efficient row-by-row validation of DataFrames using Pydantic
models. It supports both pandas and polars DataFrames with optimized conversion
and validation strategies.
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
    from pydantic import ValidationError as PydanticValidationError
else:
    PydanticValidationError = None  # type: ignore[assignment, misc]


def _pandas_to_records_fast(df: Any) -> list[dict[str, Any]]:
    """Fast conversion of pandas DataFrame to list of dicts using NumPy.

    This is ~2x faster than df.to_dict("records").
    """
    columns = df.columns.tolist()
    values = df.to_numpy()
    return [dict(zip(columns, row)) for row in values]


def _prepare_dataframe_for_validation(df: Any, convert_nans: bool) -> Any:
    """Prepare DataFrame for validation by handling NaN values.

    For pandas DataFrames with NaN conversion enabled, uses vectorized
    operations which are ~6x faster than row-by-row conversion.
    """
    if convert_nans and is_pandas_dataframe(df):
        # Use vectorized where() to replace NaN with None
        # This is much faster than converting after dict creation
        import pandas as pd

        return df.where(pd.notna(df), None)
    return df


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
    Validate DataFrame rows against a Pydantic model.

    This function validates all rows in a DataFrame against a Pydantic model
    using optimized row-by-row validation with fast DataFrame conversion.

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

    # Prepare DataFrame with vectorized NaN conversion if needed
    df_prepared = _prepare_dataframe_for_validation(df, convert_nans)

    # Use optimized row-by-row validation
    _validate_optimized(df_prepared, row_validator, max_errors, convert_nans)


def _validate_optimized(
    df: DataFrameType,
    row_validator: type[BaseModel],
    max_errors: int,
    convert_nans: bool,
) -> None:
    """Optimized row-by-row validation with fast DataFrame conversion."""
    failed_rows: list[tuple[Any, PydanticValidationError]] = []
    total_errors = 0

    # Use fast conversion for pandas DataFrames
    if is_pandas_dataframe(df):
        # Fast NumPy-based conversion (~2x faster than to_dict("records"))
        columns = df.columns.tolist()  # type: ignore[attr-defined]
        values = df.to_numpy()  # type: ignore[attr-defined]

        # Since NaN conversion was already done vectorized in df_prepared,
        # we don't need row-by-row conversion anymore
        for row_idx, row_values in enumerate(values):
            row_dict = dict(zip(columns, row_values))
            idx_label = df.index[row_idx]  # type: ignore[attr-defined]

            try:
                row_validator.model_validate(row_dict)
            except PydanticValidationError as e:  # type: ignore[misc]
                total_errors += 1
                if len(failed_rows) < max_errors:
                    failed_rows.append((idx_label, e))

    elif is_polars_dataframe(df):
        # Polars: use iter_rows which is already optimized
        for idx, row in enumerate(df.iter_rows(named=True)):  # type: ignore[attr-defined]
            # For polars, still need to handle NaN conversion if not done
            if convert_nans:
                row = _convert_nan_to_none(row)

            try:
                row_validator.model_validate(row)
            except PydanticValidationError as e:  # type: ignore[misc]
                total_errors += 1
                if len(failed_rows) < max_errors:
                    failed_rows.append((idx, e))
    else:
        raise TypeError(f"Unknown DataFrame type: {type(df)}")

    if failed_rows:
        _raise_validation_error(df, failed_rows, total_errors)


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
