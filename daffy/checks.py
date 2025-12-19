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
    except Exception:
        return []


def apply_check(series: Any, check_name: str, check_value: Any, max_samples: int = 5) -> tuple[int, list[Any]]:
    """Apply a single check to a series.

    Returns:
        Tuple of (fail_count, sample_failing_values)
    """
    if check_name == "gt":
        mask = ~(series > check_value)
    elif check_name == "ge":
        mask = ~(series >= check_value)
    elif check_name == "lt":
        mask = ~(series < check_value)
    elif check_name == "le":
        mask = ~(series <= check_value)
    elif check_name == "between":
        lo, hi = check_value
        mask = ~((series >= lo) & (series <= hi))
    elif check_name == "eq":
        mask = series != check_value
    elif check_name == "ne":
        mask = series == check_value
    elif check_name == "isin":
        mask = ~series.isin(check_value)
    elif check_name == "notnull":
        if hasattr(series, "is_null"):
            mask = series.is_null()
        else:
            mask = series.isna()
    elif check_name == "str_regex":
        if hasattr(series, "str") and hasattr(series.str, "match"):
            mask = ~series.str.match(check_value, na=False)
        else:
            mask = ~series.str.contains(f"^(?:{check_value})$")
    else:
        raise ValueError(f"Unknown check: {check_name}")

    if hasattr(mask, "fill_null"):
        mask = mask.fill_null(True)
    else:
        mask = mask.fillna(True)

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


def format_check_error(check_name: str, check_value: Any) -> str:
    """Format a check name and value for error messages."""
    return f"{check_name}({check_value!r})"
