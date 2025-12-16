"""Validation logic for DAFFY DataFrame Column Validator."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any, Union

from daffy.dataframe_types import DataFrameType, count_null_values
from daffy.patterns import (
    RegexColumnDef,
    compile_regex_pattern,
    compile_regex_patterns,
    find_regex_matches,
    is_regex_pattern,
    is_regex_string,
    match_column_with_regex,
)
from daffy.utils import describe_dataframe, format_param_context

ColumnsList = Sequence[Union[str, RegexColumnDef]]
ColumnsDict = dict[Union[str, RegexColumnDef], Any]
ColumnsDef = Union[ColumnsList, ColumnsDict, None]


def _find_missing_columns(column_spec: str | RegexColumnDef, df_columns: list[str]) -> list[str]:
    """Find missing columns for a single column specification."""
    if isinstance(column_spec, str):
        return [] if column_spec in df_columns else [column_spec]
    elif is_regex_pattern(column_spec):
        pattern_str, _ = column_spec
        matches = match_column_with_regex(column_spec, df_columns)
        return [] if matches else [pattern_str]
    return []


def _find_dtype_mismatches(
    column_spec: str | RegexColumnDef, df: DataFrameType, expected_dtype: Any, df_columns: list[str]
) -> list[tuple[str, Any, Any]]:
    """Find dtype mismatches for a single column specification."""
    mismatches: list[tuple[str, Any, Any]] = []
    if isinstance(column_spec, str):
        if column_spec in df_columns and df[column_spec].dtype != expected_dtype:
            mismatches.append((column_spec, df[column_spec].dtype, expected_dtype))
    elif is_regex_pattern(column_spec):
        matches = match_column_with_regex(column_spec, df_columns)
        for matched_col in matches:
            if df[matched_col].dtype != expected_dtype:
                mismatches.append((matched_col, df[matched_col].dtype, expected_dtype))
    return mismatches


def _find_nullable_violations(
    column_spec: str | RegexColumnDef, df: DataFrameType, df_columns: list[str]
) -> list[tuple[str, int]]:
    """Find nullable violations for a column spec.

    Returns:
        List of (column_name, null_count) tuples for columns with null values.
    """
    violations: list[tuple[str, int]] = []
    if isinstance(column_spec, str):
        if column_spec in df_columns:
            null_count = count_null_values(df, column_spec)
            if null_count > 0:
                violations.append((column_spec, null_count))
    elif is_regex_pattern(column_spec):
        matches = match_column_with_regex(column_spec, df_columns)
        for matched_col in matches:
            null_count = count_null_values(df, matched_col)
            if null_count > 0:
                violations.append((matched_col, null_count))
    return violations


def validate_dataframe(
    df: DataFrameType,
    columns: ColumnsList | ColumnsDict,
    strict: bool,
    param_name: str | None = None,
    func_name: str | None = None,
    is_return_value: bool = False,
) -> None:
    """Validate DataFrame columns and optionally data types.

    Args:
        df: DataFrame to validate (pandas or polars)
        columns: Column specification - list of names/patterns or dict mapping names to dtypes
        strict: If True, disallow extra columns not in the specification
        param_name: Parameter name for error context
        func_name: Function name for error context
        is_return_value: True if validating a return value

    Raises:
        AssertionError: If validation fails (missing columns, dtype mismatch, or extra columns in strict mode)
    """
    df_columns = list(df.columns)  # Cache the column list conversion
    all_missing_columns: list[str] = []
    all_dtype_mismatches: list[tuple[str, Any, Any]] = []
    all_nullable_violations: list[tuple[str, int]] = []
    all_matched_by_regex: set[str] = set()

    if isinstance(columns, dict):
        for column, spec_value in columns.items():
            column_spec = (
                compile_regex_pattern(column) if isinstance(column, str) and is_regex_string(column) else column
            )
            all_missing_columns.extend(_find_missing_columns(column_spec, df_columns))
            all_matched_by_regex.update(find_regex_matches(column_spec, df_columns))

            # Handle both simple dtype specs ("float64") and rich specs ({"dtype": ..., "nullable": ...})
            if isinstance(spec_value, dict):
                # Rich column spec: {"dtype": ..., "nullable": ..., etc.}
                expected_dtype = spec_value.get("dtype")
                nullable = spec_value.get("nullable", True)  # Default to True (allow nulls)

                if expected_dtype is not None:
                    all_dtype_mismatches.extend(_find_dtype_mismatches(column_spec, df, expected_dtype, df_columns))
                if not nullable:
                    all_nullable_violations.extend(_find_nullable_violations(column_spec, df, df_columns))
            else:
                # Simple dtype spec: just the dtype string
                all_dtype_mismatches.extend(_find_dtype_mismatches(column_spec, df, spec_value, df_columns))
    else:
        processed_columns = compile_regex_patterns(columns)
        for column_spec in processed_columns:
            all_missing_columns.extend(_find_missing_columns(column_spec, df_columns))
            all_matched_by_regex.update(find_regex_matches(column_spec, df_columns))

    param_info = format_param_context(param_name, func_name, is_return_value)

    if all_missing_columns:
        raise AssertionError(f"Missing columns: {all_missing_columns}{param_info}. Got {describe_dataframe(df)}")

    if all_dtype_mismatches:
        mismatch_descriptions = ", ".join(
            f"Column {col}{param_info} has wrong dtype. Was {was}, expected {expected}"
            for col, was, expected in all_dtype_mismatches
        )
        raise AssertionError(mismatch_descriptions)

    if all_nullable_violations:
        if len(all_nullable_violations) == 1:
            col, count = all_nullable_violations[0]
            raise AssertionError(f"Column '{col}'{param_info} contains {count} null values but nullable=False")
        else:
            violation_desc = ", ".join(
                f"Column '{col}' contains {count} null values" for col, count in all_nullable_violations
            )
            raise AssertionError(f"Nullable violations: {violation_desc}{param_info}")

    if strict:
        explicit_columns = {col for col in columns if isinstance(col, str)}
        allowed_columns = explicit_columns.union(all_matched_by_regex)
        extra_columns = set(df_columns) - allowed_columns
        if extra_columns:
            raise AssertionError(f"DataFrame{param_info} contained unexpected column(s): {', '.join(extra_columns)}")
