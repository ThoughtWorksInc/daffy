"""Validation pipeline - orchestrates validators."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from daffy.validators.base import SkippableValidator, Validator

if TYPE_CHECKING:
    from daffy.validators.context import ValidationContext


@dataclass
class ValidationPipeline:
    """Orchestrates validation execution with lazy/eager error handling.

    Example:
        pipeline = ValidationPipeline(lazy=True)
        pipeline.add(ShapeValidator(min_rows=1))
        pipeline.add(ColumnsExistValidator(resolved))
        pipeline.run(ctx)  # Raises AssertionError if any fail

    """

    lazy: bool = False
    validators: list[Validator] = field(default_factory=list)

    def add(self, validator: Validator) -> ValidationPipeline:
        """Add validator to pipeline. Returns self for chaining."""
        self.validators.append(validator)
        return self

    def add_if(self, condition: bool, validator: Validator) -> ValidationPipeline:
        """Conditionally add validator."""
        if condition:
            self.validators.append(validator)
        return self

    def run(self, ctx: ValidationContext) -> None:
        """Execute all validators.

        In lazy mode: collect all errors, raise once at end.
        In eager mode: raise on first error.
        """
        all_errors: list[str] = []

        for validator in self.validators:
            if isinstance(validator, SkippableValidator) and validator.should_skip(ctx):
                continue

            errors = validator.validate(ctx)

            if errors:
                if not self.lazy:
                    raise AssertionError(errors[0])
                all_errors.extend(errors)

        if all_errors:
            raise AssertionError("\n\n".join(all_errors))

    def __len__(self) -> int:
        return len(self.validators)
