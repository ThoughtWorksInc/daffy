"""Validation context - shared state for all validators."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import narwhals as nw


@dataclass
class ValidationContext:
    """Immutable context created once per validation, shared by all validators.

    This is the KEY performance optimization - we convert to narwhals ONCE
    and cache all commonly needed DataFrame metadata.
    """

    df: Any
    func_name: str = ""
    param_name: str | None = None
    is_return_value: bool = False

    nw_df: Any = field(init=False, repr=False)
    columns: tuple[str, ...] = field(init=False)
    column_set: frozenset[str] = field(init=False)
    row_count: int = field(init=False)
    schema: dict[str, Any] = field(init=False)

    def __post_init__(self) -> None:
        self.nw_df = nw.from_native(self.df, eager_only=True)
        self.columns = tuple(self.nw_df.columns)
        self.column_set = frozenset(self.columns)
        self.row_count = self.nw_df.shape[0]
        self.schema = {col: self.nw_df[col].dtype for col in self.columns}

    @property
    def param_info(self) -> str:
        """Format context for error messages."""
        if self.func_name:
            if self.is_return_value:
                return f" in function '{self.func_name}' return value"
            if self.param_name:
                return f" in function '{self.func_name}' parameter '{self.param_name}'"
        return ""

    def has_column(self, col: str) -> bool:
        """O(1) column existence check."""
        return col in self.column_set

    def get_series(self, col: str) -> Any:
        """Get a column as narwhals Series."""
        return self.nw_df[col]

    def get_dtype(self, col: str) -> Any:
        """Get column dtype from cached schema."""
        return self.schema.get(col)
