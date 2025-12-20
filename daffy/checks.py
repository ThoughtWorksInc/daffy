"""Value check implementations for column validation."""

from __future__ import annotations

from typing import Any

from daffy.narwhals_compat import (
    series_fill_null,
    series_filter_to_list,
    series_is_in,
    series_is_null,
    series_str_match,
)

CheckViolation = tuple[str, str, int, list[Any]]


def apply_check(series: Any, check_name: str, check_value: Any, max_samples: int = 5) -> tuple[int, list[Any]]:
    """Apply a single check to a series.

    Returns:
        Tuple of (fail_count, sample_failing_values)
    """
    check_masks = {
        "gt": lambda: ~(series > check_value),
        "ge": lambda: ~(series >= check_value),
        "lt": lambda: ~(series < check_value),
        "le": lambda: ~(series <= check_value),
        "between": lambda: ~((series >= check_value[0]) & (series <= check_value[1])),
        "eq": lambda: series != check_value,
        "ne": lambda: series == check_value,
        "isin": lambda: ~series_is_in(series, check_value),
        "notnull": lambda: series_is_null(series),
        "str_regex": lambda: ~series_str_match(series, check_value),
    }

    if check_name not in check_masks:
        raise ValueError(f"Unknown check: {check_name}")

    mask = series_fill_null(check_masks[check_name](), True)

    fail_count = int(mask.sum())
    if fail_count == 0:
        return 0, []

    return fail_count, series_filter_to_list(series, mask, max_samples)


def validate_checks(df: Any, column: str, checks: dict[str, Any], max_samples: int = 5) -> list[CheckViolation]:
    """Run all checks on a column.

    Returns:
        List of (column, check_name, fail_count, sample_values) tuples for failures.
    """
    violations: list[CheckViolation] = []
    series = df[column]

    for check_name, check_value in checks.items():
        fail_count, samples = apply_check(series, check_name, check_value, max_samples)
        if fail_count > 0:
            violations.append((column, check_name, fail_count, samples))

    return violations
