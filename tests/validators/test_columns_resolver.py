"""Tests for column resolution."""

from daffy.validators.columns_resolver import ResolvedColumn, ResolvedColumns


class TestResolvedColumn:
    def test_exists_when_matched(self) -> None:
        col = ResolvedColumn(spec="a", is_regex=False, matched_columns=["a"])
        assert col.exists

    def test_not_exists_when_no_match(self) -> None:
        col = ResolvedColumn(spec="a", is_regex=False, matched_columns=[])
        assert not col.exists


class TestResolvedColumns:
    def test_resolves_literal_columns(self) -> None:
        resolved = ResolvedColumns.from_specs(["a", "b"], ["a", "b", "c"])

        assert resolved.all_matched == {"a", "b"}
        assert resolved.missing_specs == []

    def test_tracks_missing_columns(self) -> None:
        resolved = ResolvedColumns.from_specs(["a", "missing"], ["a", "b"])

        assert "missing" in resolved.missing_specs
        assert "a" in resolved.all_matched

    def test_resolves_regex_patterns(self) -> None:
        resolved = ResolvedColumns.from_specs(
            ["r/col_\\d+/"],
            ["col_1", "col_2", "col_3", "other"],
        )

        assert resolved.all_matched == {"col_1", "col_2", "col_3"}
        assert resolved.missing_specs == []

    def test_regex_with_no_matches(self) -> None:
        resolved = ResolvedColumns.from_specs(
            ["r/xyz_\\d+/"],
            ["a", "b", "c"],
        )

        assert "r/xyz_\\d+/" in resolved.missing_specs
        assert resolved.all_matched == set()

    def test_get_columns_for_spec(self) -> None:
        resolved = ResolvedColumns.from_specs(
            ["a", "r/col_\\d+/"],
            ["a", "col_1", "col_2"],
        )

        assert resolved.get_columns_for_spec("a") == ["a"]
        assert set(resolved.get_columns_for_spec("r/col_\\d+/")) == {"col_1", "col_2"}
        assert resolved.get_columns_for_spec("nonexistent") == []

    def test_mixed_specs(self) -> None:
        resolved = ResolvedColumns.from_specs(
            ["id", "r/feature_\\d+/", "missing"],
            ["id", "name", "feature_1", "feature_2"],
        )

        assert resolved.all_matched == {"id", "feature_1", "feature_2"}
        assert resolved.missing_specs == ["missing"]
