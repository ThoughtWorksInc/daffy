"""Column resolution - handles regex patterns."""

from __future__ import annotations

from dataclasses import dataclass, field

from daffy.patterns import compile_regex_pattern, is_regex_string, match_column_with_regex


@dataclass
class ResolvedColumn:
    """A single column specification resolved to actual column names."""

    spec: str
    is_regex: bool
    matched_columns: list[str]

    @property
    def exists(self) -> bool:
        return len(self.matched_columns) > 0


@dataclass
class ResolvedColumns:
    """Pre-resolved column specifications.

    Created once during pipeline building, reused by all validators.
    """

    resolved: list[ResolvedColumn] = field(default_factory=list)
    _by_spec: dict[str, list[str]] = field(default_factory=dict, repr=False)
    all_matched: set[str] = field(default_factory=set)
    missing_specs: list[str] = field(default_factory=list)

    @classmethod
    def from_specs(cls, specs: list[str], df_columns: list[str]) -> ResolvedColumns:
        """Resolve column specifications against actual DataFrame columns."""
        result = cls()

        for spec in specs:
            if is_regex_string(spec):
                pattern = compile_regex_pattern(spec)
                matched = match_column_with_regex(pattern, df_columns)
                resolved = ResolvedColumn(spec, is_regex=True, matched_columns=matched)
            else:
                matched = [spec] if spec in df_columns else []
                resolved = ResolvedColumn(spec, is_regex=False, matched_columns=matched)

            result.resolved.append(resolved)
            result._by_spec[spec] = matched
            result.all_matched.update(matched)

            if not resolved.exists:
                result.missing_specs.append(spec)

        return result

    def get_columns_for_spec(self, spec: str) -> list[str]:
        """Get matched columns for a specific spec (O(1) lookup)."""
        return self._by_spec.get(spec, [])
