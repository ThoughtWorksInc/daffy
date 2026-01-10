"""Column specification parsing."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Sequence


@dataclass
class ParsedColumnSpec:
    """Parsed column specification ready for validators."""

    required_columns: list[str] = field(default_factory=list)
    optional_columns: list[str] = field(default_factory=list)
    dtype_constraints: dict[str, Any] = field(default_factory=dict)
    non_nullable_columns: list[str] = field(default_factory=list)
    unique_columns: list[str] = field(default_factory=list)
    checks_by_column: dict[str, dict[str, Any]] = field(default_factory=dict)

    @property
    def all_columns(self) -> list[str]:
        """All columns (required + optional) for strict mode."""
        return self.required_columns + self.optional_columns


def _col_to_str(col_spec: Any) -> str:
    """Convert column specification to string.

    Handles:
    - Plain strings: "column_name"
    - Regex tuples: ("column_name", compiled_pattern)
    """
    if isinstance(col_spec, tuple):
        return str(col_spec[0])
    return str(col_spec)


def parse_column_spec(columns: Sequence[Any] | dict[Any, Any] | None) -> ParsedColumnSpec:
    """Parse user-provided column specification into validator-ready format.

    Handles:
    - List: ["col1", "col2"] → required columns
    - Dict with dtype: {"col1": "int64"} → required + dtype check
    - Dict with constraints: {"col1": {"dtype": ..., "nullable": False, "required": False, ...}}
    """
    result = ParsedColumnSpec()

    if columns is None:
        return result

    if not isinstance(columns, dict):
        result.required_columns = [_col_to_str(c) for c in columns]
        return result

    for col_spec, value in columns.items():
        col_name = _col_to_str(col_spec)

        if isinstance(value, dict):
            is_required = value.get("required", True)
            if is_required:
                result.required_columns.append(col_name)
            else:
                result.optional_columns.append(col_name)

            if "dtype" in value:
                result.dtype_constraints[col_name] = value["dtype"]
            if value.get("nullable") is False:
                result.non_nullable_columns.append(col_name)
            if value.get("unique") is True:
                result.unique_columns.append(col_name)
            if "checks" in value:
                result.checks_by_column[col_name] = value["checks"]
        else:
            result.required_columns.append(col_name)
            result.dtype_constraints[col_name] = value

    return result
