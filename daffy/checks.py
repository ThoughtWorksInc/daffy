"""Value check implementations for column validation."""

from __future__ import annotations

from typing import Any

CheckViolation = tuple[str, str, int, list[Any]]


def _get_failing_values(series: Any, mask: Any, max_samples: int) -> list[Any]:
    """Extract sample failing values from a series based on a boolean mask."""
    try:
        failing = series[mask]
        if hasattr(failing, "to_list"):
            return failing.head(max_samples).to_list()
        return list(failing.head(max_samples))
    except (AttributeError, IndexError, KeyError, TypeError):
        return []


def _series_isin(series: Any, values: Any) -> Any:
    """Check if series values are in the given set (pandas/polars compatible)."""
    if hasattr(series, "is_in"):
        return series.is_in(values)
    return series.isin(values)


def _series_is_null(series: Any) -> Any:
    """Get null mask for a series (pandas/polars compatible)."""
    if hasattr(series, "is_null"):
        return series.is_null()
    return series.isna()


def _series_str_match(series: Any, pattern: str) -> Any:
    """Check if string series matches regex pattern (pandas/polars compatible)."""
    if hasattr(series, "str") and hasattr(series.str, "match"):
        return series.str.match(pattern, na=False)
    return series.str.contains(f"^(?:{pattern})$")


def _fill_null_mask(mask: Any) -> Any:
    """Fill null values in a mask with True (pandas/polars compatible)."""
    if hasattr(mask, "fill_null"):
        return mask.fill_null(True)
    return mask.fillna(True)


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
        "isin": lambda: ~_series_isin(series, check_value),
        "notnull": lambda: _series_is_null(series),
        "str_regex": lambda: ~_series_str_match(series, check_value),
    }

    if check_name not in check_masks:
        raise ValueError(f"Unknown check: {check_name}")

    mask = _fill_null_mask(check_masks[check_name]())

    fail_count = int(mask.sum())
    if fail_count == 0:
        return 0, []

    return fail_count, _get_failing_values(series, mask, max_samples)


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
