"""Validation logic for DAFFY DataFrame Column Validator."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import Any, TypeAlias, TypedDict

import narwhals as nw

from daffy.checks import CheckViolation, validate_checks
from daffy.config import get_checks_max_samples, get_lazy
from daffy.dataframe_types import count_duplicate_rows, count_duplicate_values, count_null_values
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


class ColumnConstraints(TypedDict, total=False):
    """Type-safe specification for column constraints.

    All fields are optional. Use this instead of untyped dicts to catch
    typos like {"nulllable": False} at type-check time.
    """

    dtype: Any
    nullable: bool
    unique: bool
    required: bool
    checks: dict[str, Any]


ColumnsList: TypeAlias = Sequence[str | RegexColumnDef]
ColumnsDict: TypeAlias = dict[str | RegexColumnDef, Any]
ColumnsDef: TypeAlias = ColumnsList | ColumnsDict | None


def _get_columns_to_check(column_spec: str | RegexColumnDef, df_columns: list[str]) -> list[str]:
    """Get list of existing column names to check for a given spec."""
    if isinstance(column_spec, str):
        return [column_spec] if column_spec in df_columns else []
    if is_regex_pattern(column_spec):
        return match_column_with_regex(column_spec, df_columns)
    return []  # Invalid type - skip silently for backwards compatibility


def _find_missing_columns(column_spec: str | RegexColumnDef, df_columns: list[str]) -> list[str]:
    """Find missing columns for a single column specification."""
    if isinstance(column_spec, str):
        return [] if column_spec in df_columns else [column_spec]
    if is_regex_pattern(column_spec):
        pattern_str, _ = column_spec
        matches = match_column_with_regex(column_spec, df_columns)
        return [] if matches else [pattern_str]
    return []  # Invalid type - skip silently for backwards compatibility


def _find_dtype_mismatches(
    column_spec: str | RegexColumnDef, df: Any, expected_dtype: Any, df_columns: list[str]
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


def _format_violation_error(
    violations: list[tuple[str, int]],
    param_info: str,
    violation_type: str,
    constraint: str,
) -> str:
    """Format an error message for column violations.

    Args:
        violations: List of (column_name, count) tuples
        param_info: Parameter context string (e.g., " in function 'f' parameter 'df'")
        violation_type: Type of violation (e.g., "null", "duplicate")
        constraint: The constraint that was violated (e.g., "nullable=False", "unique=True")
    """
    if len(violations) == 1:
        col, count = violations[0]
        return f"Column '{col}'{param_info} contains {count} {violation_type} values but {constraint}"
    violation_desc = ", ".join(f"Column '{col}' contains {count} {violation_type} values" for col, count in violations)
    return f"{violation_type.capitalize()} violations: {violation_desc}{param_info}"


def _find_column_violations(
    column_spec: str | RegexColumnDef,
    df: Any,
    df_columns: list[str],
    count_fn: Callable[[Any, str], int],
) -> list[tuple[str, int]]:
    """Find violations for a column spec using the given count function.

    Args:
        column_spec: Column name or regex pattern
        df: DataFrame to check
        df_columns: List of column names in the DataFrame
        count_fn: Function that counts violations (e.g., count_null_values, count_duplicate_values)

    Returns:
        List of (column_name, violation_count) tuples for columns with violations.
    """
    violations: list[tuple[str, int]] = []
    for col in _get_columns_to_check(column_spec, df_columns):
        count = count_fn(df, col)
        if count > 0:
            violations.append((col, count))
    return violations


def _raise_or_collect(msg: str, lazy: bool, errors: list[str]) -> None:
    """Raise immediately or collect error for lazy mode."""
    if not lazy:
        raise AssertionError(msg)
    errors.append(msg)


def validate_dataframe(
    df: Any,
    columns: ColumnsList | ColumnsDict,
    strict: bool,
    param_name: str | None = None,
    func_name: str | None = None,
    is_return_value: bool = False,
    lazy: bool | None = None,
    composite_unique: list[list[str]] | None = None,
) -> None:
    """Validate DataFrame columns and optionally data types.

    Args:
        df: DataFrame to validate (Pandas, Polars, Modin, or PyArrow)
        columns: Column specification - list of names/patterns or dict mapping names to dtypes
        strict: If True, disallow extra columns not in the specification
        param_name: Parameter name for error context
        func_name: Function name for error context
        is_return_value: True if validating a return value
        lazy: If True, collect all errors before raising. If None, use config value.
        composite_unique: List of column name lists that must be unique together

    Raises:
        AssertionError: If validation fails (missing columns, dtype mismatch, or extra columns in strict mode)
    """
    lazy = get_lazy(lazy)
    df_columns = nw.from_native(df, eager_only=True).columns
    all_missing_columns: list[str] = []
    all_dtype_mismatches: list[tuple[str, Any, Any]] = []
    all_nullable_violations: list[tuple[str, int]] = []
    all_uniqueness_violations: list[tuple[str, int]] = []
    all_check_violations: list[CheckViolation] = []
    all_matched_by_regex: set[str] = set()

    if isinstance(columns, dict):
        for column, spec_value in columns.items():
            column_spec = (
                compile_regex_pattern(column) if isinstance(column, str) and is_regex_string(column) else column
            )
            all_matched_by_regex.update(find_regex_matches(column_spec, df_columns))

            if isinstance(spec_value, dict):
                required = spec_value.get("required", True)
                expected_dtype = spec_value.get("dtype")
                nullable = spec_value.get("nullable", True)
                unique = spec_value.get("unique", False)
                checks = spec_value.get("checks")

                if required:
                    all_missing_columns.extend(_find_missing_columns(column_spec, df_columns))
                if expected_dtype is not None:
                    all_dtype_mismatches.extend(_find_dtype_mismatches(column_spec, df, expected_dtype, df_columns))
                if not nullable:
                    all_nullable_violations.extend(
                        _find_column_violations(column_spec, df, df_columns, count_null_values)
                    )
                if unique:
                    all_uniqueness_violations.extend(
                        _find_column_violations(column_spec, df, df_columns, count_duplicate_values)
                    )
                if checks:
                    max_samples = get_checks_max_samples()
                    for col in _get_columns_to_check(column_spec, df_columns):
                        all_check_violations.extend(validate_checks(df, col, checks, max_samples))
            else:
                all_missing_columns.extend(_find_missing_columns(column_spec, df_columns))
                all_dtype_mismatches.extend(_find_dtype_mismatches(column_spec, df, spec_value, df_columns))
    else:
        processed_columns = compile_regex_patterns(columns)
        for column_spec in processed_columns:
            all_missing_columns.extend(_find_missing_columns(column_spec, df_columns))
            all_matched_by_regex.update(find_regex_matches(column_spec, df_columns))

    param_info = format_param_context(param_name, func_name, is_return_value)

    # Collect error messages
    errors: list[str] = []

    if all_missing_columns:
        msg = f"Missing columns: {all_missing_columns}{param_info}. Got {describe_dataframe(df)}"
        _raise_or_collect(msg, lazy, errors)

    if all_dtype_mismatches:
        msg = ", ".join(
            f"Column {col}{param_info} has wrong dtype. Was {was}, expected {expected}"
            for col, was, expected in all_dtype_mismatches
        )
        _raise_or_collect(msg, lazy, errors)

    if all_nullable_violations:
        msg = _format_violation_error(all_nullable_violations, param_info, "null", "nullable=False")
        _raise_or_collect(msg, lazy, errors)

    if all_uniqueness_violations:
        msg = _format_violation_error(all_uniqueness_violations, param_info, "duplicate", "unique=True")
        _raise_or_collect(msg, lazy, errors)

    if composite_unique:
        # Validate that all referenced columns exist
        for col_combo in composite_unique:
            missing_cols = [c for c in col_combo if c not in df_columns]
            if missing_cols:
                col_desc = " + ".join(f"'{c}'" for c in col_combo)
                msg = (
                    f"composite_unique references missing columns {missing_cols} "
                    f"in combination [{col_desc}]{param_info}"
                )
                if not lazy:
                    raise AssertionError(msg)
                errors.append(msg)
                continue  # Skip duplicate check for this combo since columns don't exist

            dup_count = count_duplicate_rows(df, col_combo)
            if dup_count > 0:
                col_desc = " + ".join(f"'{c}'" for c in col_combo)
                msg = (
                    f"Columns {col_desc}{param_info} contain {dup_count} duplicate combinations "
                    "but composite_unique is set"
                )
                if not lazy:
                    raise AssertionError(msg)
                errors.append(msg)

    if all_check_violations:
        if len(all_check_violations) == 1:
            col, check, count, samples = all_check_violations[0]
            msg = f"Column '{col}'{param_info} failed check {check}: {count} values failed. Examples: {samples}"
        else:
            violation_lines: list[str] = []
            for col, check, count, samples in all_check_violations:
                violation_lines.append(f"Column '{col}' failed {check}: {count} values. Examples: {samples}")
            msg = f"Check violations{param_info}:\n  " + "\n  ".join(violation_lines)
        _raise_or_collect(msg, lazy, errors)

    if strict:
        explicit_columns = {col for col in columns if isinstance(col, str)}
        allowed_columns = explicit_columns.union(all_matched_by_regex)
        extra_columns = set(df_columns) - allowed_columns
        if extra_columns:
            msg = f"DataFrame{param_info} contained unexpected column(s): {', '.join(extra_columns)}"
            _raise_or_collect(msg, lazy, errors)

    # In lazy mode, raise all collected errors at once
    if lazy and errors:
        raise AssertionError("\n\n".join(errors))
