"""Pattern matching utilities for DAFFY DataFrame Column Validator."""

from __future__ import annotations

import re
from collections.abc import Sequence
from typing import Any

# Regex column pattern format delimiters
_REGEX_PREFIX = "r/"
_REGEX_SUFFIX = "/"

RegexColumnDef = tuple[str, re.Pattern[str]]


def is_regex_pattern(column: Any) -> bool:
    return (
        isinstance(column, tuple)
        and len(column) == 2
        and isinstance(column[0], str)
        and isinstance(column[1], re.Pattern)
    )


def as_regex_pattern(column: str | RegexColumnDef) -> RegexColumnDef | None:
    """Convert column to RegexColumnDef if it is a regex pattern, otherwise return None."""
    if is_regex_pattern(column):
        return column  # type: ignore[return-value]  # We know it's the right type after the check
    return None


def is_regex_string(column: str) -> bool:
    return column.startswith(_REGEX_PREFIX) and column.endswith(_REGEX_SUFFIX)


def compile_regex_pattern(pattern_string: str) -> RegexColumnDef:
    """Compile a regex pattern from r/pattern/ format.

    Args:
        pattern_string: Pattern in format "r/pattern/" (e.g., "r/col_\\d+/")

    Returns:
        Tuple of (original pattern string, compiled regex pattern)

    Raises:
        ValueError: If the regex pattern is invalid
    """
    pattern_str = pattern_string[len(_REGEX_PREFIX) : -len(_REGEX_SUFFIX)]
    try:
        compiled_pattern = re.compile(pattern_str)
    except re.error as e:
        raise ValueError(f"Invalid regex pattern '{pattern_str}': {e}") from e
    return (pattern_string, compiled_pattern)


def compile_regex_patterns(columns: Sequence[Any]) -> list[str | RegexColumnDef]:
    """Compile all regex patterns in a column specification.

    Converts column strings in r/pattern/ format to compiled regex tuples.
    Non-regex strings are passed through unchanged.

    Args:
        columns: Sequence of column names, possibly containing regex patterns

    Returns:
        List with regex patterns compiled into RegexColumnDef tuples
    """
    return [compile_regex_pattern(col) if isinstance(col, str) and is_regex_string(col) else col for col in columns]


def match_column_with_regex(column_pattern: RegexColumnDef, df_columns: list[str]) -> list[str]:
    """Find DataFrame columns matching a regex pattern.

    Args:
        column_pattern: Tuple of (pattern string, compiled regex)
        df_columns: List of column names from the DataFrame

    Returns:
        List of column names that match the regex pattern
    """
    _, pattern = column_pattern
    return [col for col in df_columns if pattern.match(col)]


def find_regex_matches(column_spec: str | RegexColumnDef, df_columns: list[str]) -> set[str]:
    """Find regex matches for a single column specification."""
    regex_pattern = as_regex_pattern(column_spec)
    if regex_pattern:
        return set(match_column_with_regex(regex_pattern, df_columns))
    return set()
