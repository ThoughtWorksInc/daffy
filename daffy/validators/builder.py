"""Pipeline builder - assembles validators from decorator parameters."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Sequence

from daffy.validators.checks import ChecksValidator
from daffy.validators.columns import ColumnsExistValidator, DtypeValidator, NullableValidator, StrictModeValidator
from daffy.validators.columns_resolver import ResolvedColumns
from daffy.validators.pipeline import ValidationPipeline
from daffy.validators.rows import RowValidator
from daffy.validators.shape import ShapeValidator
from daffy.validators.spec_parser import parse_column_spec
from daffy.validators.uniqueness import CompositeUniqueValidator, UniqueValidator


def build_validation_pipeline(
    columns: Sequence[Any] | dict[Any, Any] | None,
    strict: bool,
    lazy: bool,
    composite_unique: list[list[str]] | None,
    row_validator: type | None,
    min_rows: int | None,
    max_rows: int | None,
    exact_rows: int | None,
    allow_empty: bool,
    df_columns: list[str],
) -> ValidationPipeline:
    """Build a validation pipeline from decorator parameters.

    This is the ONLY place where we decide which validators to include.
    """
    pipeline = ValidationPipeline(lazy=lazy)

    # 1. Shape validation (fast, fail early on empty)
    has_shape_constraints = any(
        [
            min_rows is not None,
            max_rows is not None,
            exact_rows is not None,
            not allow_empty,
        ]
    )
    if has_shape_constraints:
        pipeline.add(
            ShapeValidator(
                min_rows=min_rows,
                max_rows=max_rows,
                exact_rows=exact_rows,
                allow_empty=allow_empty,
            )
        )

    # 2. Column-based validation
    if columns:
        spec = parse_column_spec(columns)

        # Resolve required columns (must exist)
        resolved_required = ResolvedColumns.from_specs(spec.required_columns, df_columns)
        pipeline.add(ColumnsExistValidator(resolved_required))

        # Resolve optional columns (may exist) for constraints
        resolved_optional = ResolvedColumns.from_specs(spec.optional_columns, df_columns)

        # Combine both for expanding constraints
        all_resolved = ResolvedColumns.from_specs(spec.all_columns, df_columns)

        expanded_dtypes = _expand_regex_specs(spec.dtype_constraints, all_resolved)
        expanded_non_nullable = _expand_regex_list(spec.non_nullable_columns, all_resolved)
        expanded_unique = _expand_regex_list(spec.unique_columns, all_resolved)
        expanded_checks = _expand_regex_specs(spec.checks_by_column, all_resolved)

        if expanded_dtypes:
            pipeline.add(DtypeValidator(expanded_dtypes))

        if expanded_non_nullable:
            pipeline.add(NullableValidator(expanded_non_nullable))

        if expanded_unique:
            pipeline.add(UniqueValidator(expanded_unique))

        if expanded_checks:
            pipeline.add(ChecksValidator(expanded_checks))

        if strict:
            allowed = set(spec.all_columns) | resolved_required.all_matched | resolved_optional.all_matched
            pipeline.add(StrictModeValidator(allowed))

    # 3. Composite unique
    if composite_unique:
        pipeline.add(CompositeUniqueValidator(composite_unique))

    # 4. Row validation (expensive, always last)
    if row_validator:
        pipeline.add(RowValidator(row_validator))

    return pipeline


def _expand_regex_specs(specs: dict[str, Any], resolved: ResolvedColumns) -> dict[str, Any]:
    """Expand regex column specs to actual column names.

    Specs that don't match any columns are skipped (not added as literals).
    """
    result: dict[str, Any] = {}
    for spec, value in specs.items():
        columns = resolved.get_columns_for_spec(spec)
        if columns:
            for col in columns:
                result[col] = value
    return result


def _expand_regex_list(specs: list[str], resolved: ResolvedColumns) -> list[str]:
    """Expand regex column specs to actual column names.

    Specs that don't match any columns are skipped (not added as literals).
    """
    result: list[str] = []
    for spec in specs:
        cols = resolved.get_columns_for_spec(spec)
        if cols:
            result.extend(cols)
    return result
