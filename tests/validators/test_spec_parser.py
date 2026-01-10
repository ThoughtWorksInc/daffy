"""Tests for column spec parsing."""

from daffy.validators.spec_parser import parse_column_spec


class TestParseColumnSpec:
    def test_none_returns_empty_spec(self) -> None:
        result = parse_column_spec(None)

        assert result.required_columns == []
        assert result.dtype_constraints == {}
        assert result.non_nullable_columns == []
        assert result.unique_columns == []
        assert result.checks_by_column == {}

    def test_list_creates_required_columns(self) -> None:
        result = parse_column_spec(["a", "b", "c"])

        assert result.required_columns == ["a", "b", "c"]
        assert result.dtype_constraints == {}

    def test_dict_with_dtype_shorthand(self) -> None:
        result = parse_column_spec({"a": "int64", "b": "float64"})

        assert result.required_columns == ["a", "b"]
        assert result.dtype_constraints == {"a": "int64", "b": "float64"}

    def test_dict_with_dtype_in_dict(self) -> None:
        result = parse_column_spec({"a": {"dtype": "int64"}})

        assert result.required_columns == ["a"]
        assert result.dtype_constraints == {"a": "int64"}

    def test_dict_with_nullable_false(self) -> None:
        result = parse_column_spec({"a": {"nullable": False}})

        assert result.required_columns == ["a"]
        assert result.non_nullable_columns == ["a"]

    def test_dict_with_nullable_true_not_added(self) -> None:
        result = parse_column_spec({"a": {"nullable": True}})

        assert result.non_nullable_columns == []

    def test_dict_with_unique_true(self) -> None:
        result = parse_column_spec({"a": {"unique": True}})

        assert result.required_columns == ["a"]
        assert result.unique_columns == ["a"]

    def test_dict_with_unique_false_not_added(self) -> None:
        result = parse_column_spec({"a": {"unique": False}})

        assert result.unique_columns == []

    def test_dict_with_checks(self) -> None:
        result = parse_column_spec({"a": {"checks": {"gt": 0, "lt": 100}}})

        assert result.checks_by_column == {"a": {"gt": 0, "lt": 100}}

    def test_full_constraint_dict(self) -> None:
        result = parse_column_spec(
            {
                "id": {"dtype": "int64", "nullable": False, "unique": True},
                "price": {"dtype": "float64", "checks": {"gt": 0}},
            }
        )

        assert result.required_columns == ["id", "price"]
        assert result.dtype_constraints == {"id": "int64", "price": "float64"}
        assert result.non_nullable_columns == ["id"]
        assert result.unique_columns == ["id"]
        assert result.checks_by_column == {"price": {"gt": 0}}
