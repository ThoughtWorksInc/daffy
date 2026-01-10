"""Type definitions for DAFFY DataFrame validation."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any, TypeAlias, TypedDict

from daffy.patterns import RegexColumnDef


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
