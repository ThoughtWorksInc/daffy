"""Validation logic for DAFFY DataFrame Column Validator."""

from typing import Any, Dict, List, Optional, Tuple, Union
from typing import Sequence as Seq

from daffy.patterns import (
    RegexColumnDef,
    compile_regex_pattern,
    compile_regex_patterns,
    find_regex_matches,
    is_regex_pattern,
    is_regex_string,
    match_column_with_regex,
)
from daffy.utils import DataFrameType, describe_dataframe, format_param_context

ColumnsList = Seq[Union[str, RegexColumnDef]]
ColumnsDict = Dict[Union[str, RegexColumnDef], Any]
ColumnsDef = Union[ColumnsList, ColumnsDict, None]


def _find_missing_columns(column_spec: Union[str, RegexColumnDef], df_columns: List[str]) -> List[str]:
    """Find missing columns for a single column specification."""
    if isinstance(column_spec, str):
        return [column_spec] if column_spec not in df_columns else []
    elif is_regex_pattern(column_spec):
        pattern_str, _ = column_spec
        matches = match_column_with_regex(column_spec, df_columns)
        return [pattern_str] if not matches else []
    return []


def _find_dtype_mismatches(
    column_spec: Union[str, RegexColumnDef], df: DataFrameType, expected_dtype: Any, df_columns: List[str]
) -> List[Tuple[str, Any, Any]]:
    """Find dtype mismatches for a single column specification."""
    mismatches = []
    if isinstance(column_spec, str):
        if column_spec in df_columns and df[column_spec].dtype != expected_dtype:
            mismatches.append((column_spec, df[column_spec].dtype, expected_dtype))
    elif is_regex_pattern(column_spec):
        matches = match_column_with_regex(column_spec, df_columns)
        for matched_col in matches:
            if df[matched_col].dtype != expected_dtype:
                mismatches.append((matched_col, df[matched_col].dtype, expected_dtype))
    return mismatches


def validate_dataframe(
    df: DataFrameType, columns: Union[ColumnsList, ColumnsDict], strict: bool, param_name: Optional[str] = None
) -> None:
    df_columns = list(df.columns)  # Cache the column list conversion
    all_missing_columns = []
    all_dtype_mismatches = []
    all_matched_by_regex = set()

    if isinstance(columns, list):
        processed_columns = compile_regex_patterns(columns)
        for column_spec in processed_columns:
            all_missing_columns.extend(_find_missing_columns(column_spec, df_columns))
            all_matched_by_regex.update(find_regex_matches(column_spec, df_columns))
    else:  # isinstance(columns, dict)
        assert isinstance(columns, dict)
        for column, expected_dtype in columns.items():
            column_spec = (
                compile_regex_pattern(column) if isinstance(column, str) and is_regex_string(column) else column
            )
            all_missing_columns.extend(_find_missing_columns(column_spec, df_columns))
            all_dtype_mismatches.extend(_find_dtype_mismatches(column_spec, df, expected_dtype, df_columns))
            all_matched_by_regex.update(find_regex_matches(column_spec, df_columns))

    param_info = format_param_context(param_name)

    if all_missing_columns:
        raise AssertionError(f"Missing columns: {all_missing_columns}{param_info}. Got {describe_dataframe(df)}")

    if all_dtype_mismatches:
        mismatch_descriptions = ", ".join(
            f"Column {col}{param_info} has wrong dtype. Was {was}, expected {expected}"
            for col, was, expected in all_dtype_mismatches
        )
        raise AssertionError(mismatch_descriptions)

    if strict:
        explicit_columns = {col for col in columns if isinstance(col, str)}
        allowed_columns = explicit_columns.union(all_matched_by_regex)
        extra_columns = set(df_columns) - allowed_columns
        if extra_columns:
            raise AssertionError(f"DataFrame{param_info} contained unexpected column(s): {', '.join(extra_columns)}")
