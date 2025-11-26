"""Row-level DataFrame validation using Pydantic models.

This module provides efficient row-by-row validation of DataFrames using Pydantic
models. It supports both pandas and polars DataFrames with optimized conversion
and validation strategies.
"""

from __future__ import annotations

import math
from typing import TYPE_CHECKING, Any

from daffy.dataframe_types import get_dataframe_types, is_pandas_dataframe, is_polars_dataframe, pd
from daffy.pydantic_types import HAS_PYDANTIC, require_pydantic

_PYDANTIC_ROOT_FIELD = "__root__"

if TYPE_CHECKING:
    from pydantic import BaseModel, ValidationError  # noqa: F401

    from daffy.dataframe_types import DataFrameType

if HAS_PYDANTIC:
    from pydantic import ValidationError as PydanticValidationError
else:
    PydanticValidationError = None  # type: ignore[assignment, misc]


def _prepare_dataframe_for_validation(df: Any, convert_nans: bool) -> Any:
    """Prepare DataFrame for validation by handling NaN values.

    For pandas DataFrames with NaN conversion enabled, converts NaN to None.
    This requires converting numeric columns to object dtype to preserve None values.
    """
    if convert_nans and is_pandas_dataframe(df) and pd is not None:
        # Copy DataFrame to avoid modifying original
        df = df.copy()

        # Convert float/numeric columns with NaN to object dtype and replace NaN with None
        # This is necessary because pandas float64 columns convert None back to NaN
        for col in df.columns:
            if pd.api.types.is_float_dtype(df[col]) or pd.api.types.is_integer_dtype(df[col]):
                if df[col].isna().any():
                    mask = pd.isna(df[col])
                    df[col] = df[col].astype("object")
                    df.loc[mask, col] = None

        return df
    return df


def _convert_nan_to_none(row_dict: dict[str, Any]) -> dict[str, Any]:
    """Convert NaN values to None for Pydantic compatibility."""
    return {k: (None if isinstance(v, float) and math.isnan(v) else v) for k, v in row_dict.items()}


def validate_dataframe_rows(
    df: DataFrameType,
    row_validator: type[BaseModel],
    max_errors: int = 5,
    convert_nans: bool = True,
    early_termination: bool = True,
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
        early_termination: Stop scanning after max_errors reached (faster for large datasets)

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
    _validate_optimized(df_prepared, row_validator, max_errors, convert_nans, early_termination)


def _validate_optimized(
    df: DataFrameType,
    row_validator: type[BaseModel],
    max_errors: int,
    convert_nans: bool,
    early_termination: bool,
) -> None:
    """Optimized row-by-row validation with fast DataFrame conversion."""
    failed_rows: list[tuple[Any, PydanticValidationError]] = []
    total_errors = 0

    # Use fast conversion for pandas DataFrames
    if is_pandas_dataframe(df):
        # Fast NumPy-based conversion (~2x faster than to_dict("records"))
        # Note: When we have None values (from NaN conversion), to_numpy() converts them back to NaN
        # because NumPy doesn't support None in numeric arrays. So we use itertuples instead
        # which preserves None values.
        columns = df.columns.tolist()  # type: ignore[attr-defined]

        for row_idx, row_values in enumerate(df.itertuples(index=False, name=None)):  # type: ignore[attr-defined]
            row_dict = dict(zip(columns, row_values))
            idx_label = df.index[row_idx]  # type: ignore[attr-defined]

            try:
                row_validator.model_validate(row_dict)
            except PydanticValidationError as e:  # type: ignore[misc]
                total_errors += 1
                if len(failed_rows) < max_errors:
                    failed_rows.append((idx_label, e))
                elif early_termination:
                    # Stop scanning after collecting max_errors
                    break

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
                elif early_termination:
                    # Stop scanning after collecting max_errors
                    break
    else:
        raise TypeError(f"Unknown DataFrame type: {type(df)}")

    if failed_rows:
        _raise_validation_error(df, failed_rows, total_errors, early_termination)


def _raise_validation_error(
    df: DataFrameType,
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
