"""Tests for optional columns (required=False) feature."""

import pandas as pd

from daffy import df_in


def test_optional_column_missing_ok() -> None:
    """Optional column that is missing should not raise an error."""

    @df_in(columns={"A": "int64", "B": {"dtype": "float64", "required": False}})
    def process(df: pd.DataFrame) -> pd.DataFrame:
        return df

    # DataFrame only has column A, missing optional column B
    df = pd.DataFrame({"A": [1, 2, 3]})
    result = process(df)

    assert list(result.columns) == ["A"]


def test_optional_column_present_validated() -> None:
    """Optional column that is present should be validated normally."""

    @df_in(columns={"A": "int64", "B": {"dtype": "float64", "required": False}})
    def process(df: pd.DataFrame) -> pd.DataFrame:
        return df

    # DataFrame has both columns, dtype is correct
    df = pd.DataFrame({"A": [1, 2, 3], "B": [1.0, 2.0, 3.0]})
    result = process(df)

    assert list(result.columns) == ["A", "B"]


def test_optional_column_dtype_mismatch() -> None:
    """Optional column with wrong dtype should raise an error."""
    import pytest

    @df_in(columns={"A": "int64", "B": {"dtype": "float64", "required": False}})
    def process(df: pd.DataFrame) -> pd.DataFrame:
        return df

    # Column B is present but has wrong dtype (int instead of float)
    df = pd.DataFrame({"A": [1, 2, 3], "B": [1, 2, 3]})

    with pytest.raises(AssertionError, match="wrong dtype"):
        process(df)


def test_optional_column_nullable_violation() -> None:
    """Optional column with null values when nullable=False should raise an error."""
    import pytest

    @df_in(columns={"A": "int64", "B": {"dtype": "float64", "nullable": False, "required": False}})
    def process(df: pd.DataFrame) -> pd.DataFrame:
        return df

    # Column B is present but contains null values
    df = pd.DataFrame({"A": [1, 2, 3], "B": [1.0, None, 3.0]})

    with pytest.raises(AssertionError, match="null values"):
        process(df)


def test_optional_column_unique_violation() -> None:
    """Optional column with duplicate values when unique=True should raise an error."""
    import pytest

    @df_in(columns={"A": "int64", "B": {"dtype": "float64", "unique": True, "required": False}})
    def process(df: pd.DataFrame) -> pd.DataFrame:
        return df

    # Column B is present but contains duplicate values
    df = pd.DataFrame({"A": [1, 2, 3], "B": [1.0, 1.0, 3.0]})

    with pytest.raises(AssertionError, match="duplicate values"):
        process(df)


def test_required_default_true() -> None:
    """Column without required key should be required by default."""
    import pytest

    @df_in(columns={"A": "int64", "B": {"dtype": "float64"}})
    def process(df: pd.DataFrame) -> pd.DataFrame:
        return df

    # DataFrame missing column B (which is required by default)
    df = pd.DataFrame({"A": [1, 2, 3]})

    with pytest.raises(AssertionError, match="Missing columns"):
        process(df)
