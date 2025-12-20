"""Value check implementations for column validation."""

from __future__ import annotations

from typing import Any

import narwhals as nw

CheckViolation = tuple[str, str, int, list[Any]]


def _nw_series(series: Any) -> nw.Series[Any]:
    """Wrap native series in Narwhals."""
    return nw.from_native(series, series_only=True)


def apply_check(series: Any, check_name: str, check_value: Any, max_samples: int = 5) -> tuple[int, list[Any]]:
    """Apply a single check to a series.

    Returns:
        Tuple of (fail_count, sample_failing_values)
    """
    nws = _nw_series(series)

    check_masks = {
        "gt": lambda: ~(series > check_value),
        "ge": lambda: ~(series >= check_value),
        "lt": lambda: ~(series < check_value),
        "le": lambda: ~(series <= check_value),
        "between": lambda: ~((series >= check_value[0]) & (series <= check_value[1])),
        "eq": lambda: series != check_value,
        "ne": lambda: series == check_value,
        "isin": lambda: nw.to_native(~nws.is_in(check_value)),
        "notnull": lambda: nw.to_native(nws.is_null()),
        "str_regex": lambda: nw.to_native(~nws.str.contains(f"^(?:{check_value})")),
    }

    if check_name not in check_masks:
        raise ValueError(f"Unknown check: {check_name}")

    mask = nw.to_native(_nw_series(check_masks[check_name]()).fill_null(True))

    fail_count = int(mask.sum())
    if fail_count == 0:
        return 0, []

    # Get sample failing values
    nw_mask = _nw_series(mask)
    samples = nws.filter(nw_mask).head(max_samples).to_list()
    return fail_count, samples


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
