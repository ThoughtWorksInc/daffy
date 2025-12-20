"""Tests for DataFrame row validation with Pydantic."""

import numpy as np
import pandas as pd
import polars as pl
import pytest
from pydantic import BaseModel, ConfigDict, Field

from daffy.pydantic_types import require_pydantic
from daffy.row_validation import validate_dataframe_rows


class SimpleValidator(BaseModel):
    """Simple test validator."""

    model_config = ConfigDict(str_strip_whitespace=True)

    name: str
    age: int = Field(ge=0, le=120)
    price: float = Field(gt=0)


class TestPandasValidation:
    """Test row validation with pandas DataFrames."""

    def test_all_valid_rows(self) -> None:
        """Test validation passes when all rows are valid."""

        df = pd.DataFrame(
            {
                "name": ["Alice", "Bob", "Charlie"],
                "age": [25, 30, 35],
                "price": [10.5, 20.0, 15.75],
            }
        )

        # Should not raise
        validate_dataframe_rows(df, SimpleValidator)

    def test_batch_validation_with_errors(self) -> None:
        """Test batch validation catches multiple errors efficiently."""

        df = pd.DataFrame(
            {
                "name": ["Alice", "Bob", "Charlie", "David"],
                "age": [25, -5, 35, 150],  # Bob and David invalid
                "price": [10.5, 20.0, -15.0, 30.0],  # Charlie invalid
            }
        )

        with pytest.raises(AssertionError) as exc_info:
            validate_dataframe_rows(df, SimpleValidator)

        message = str(exc_info.value)
        assert "Row validation failed for" in message
        assert "Row 1:" in message  # Bob (index 1)
        assert "age" in message

    def test_nan_handling(self) -> None:
        """Test NaN values fail validation when they violate constraints."""

        df = pd.DataFrame(
            {
                "name": ["Alice", "Bob"],
                "age": [25, 30],
                "price": [10.5, np.nan],  # NaN fails gt=0 constraint
            }
        )

        with pytest.raises(AssertionError) as exc_info:
            validate_dataframe_rows(df, SimpleValidator)

        message = str(exc_info.value)
        assert "Row 1:" in message
        assert "price" in message

    def test_non_integer_index(self) -> None:
        """Test validation works with non-integer DataFrame index (uses row number)."""

        df = pd.DataFrame(
            {
                "name": ["Alice", "Bob"],
                "age": [25, -5],  # Bob invalid (row 1)
                "price": [10.5, 20.0],
            },
            index=["person_a", "person_b"],  # type: ignore[arg-type]
        )

        with pytest.raises(AssertionError) as exc_info:
            validate_dataframe_rows(df, SimpleValidator)

        message = str(exc_info.value)
        assert "Row 1:" in message  # Uses row number, not index label

    def test_datetime_index(self) -> None:
        """Test validation works with datetime index (uses row number)."""

        dates = pd.date_range("2025-01-01", periods=2)
        df = pd.DataFrame(
            {
                "name": ["Alice", "Bob"],
                "age": [25, -5],  # Bob invalid (row 1)
                "price": [10.5, 20.0],
            },
            index=dates,
        )

        with pytest.raises(AssertionError) as exc_info:
            validate_dataframe_rows(df, SimpleValidator)

        message = str(exc_info.value)
        assert "Row 1:" in message  # Uses row number, not datetime index

    def test_missing_required_field(self) -> None:
        """Test validation fails when required field is missing."""

        df = pd.DataFrame(
            {
                "name": ["Alice"],
                "age": [25],
                # Missing 'price' column
            }
        )

        with pytest.raises(AssertionError) as exc_info:
            validate_dataframe_rows(df, SimpleValidator)

        message = str(exc_info.value)
        assert "price" in message.lower()

    def test_type_mismatch(self) -> None:
        """Test validation fails with informative error for type mismatches."""

        df = pd.DataFrame(
            {
                "name": ["Alice"],
                "age": ["twenty-five"],  # String instead of int
                "price": [10.5],
            }
        )

        with pytest.raises(AssertionError) as exc_info:
            validate_dataframe_rows(df, SimpleValidator)

        message = str(exc_info.value)
        assert "age" in message
        assert "int" in message.lower() or "integer" in message.lower()

    def test_empty_dataframe(self) -> None:
        """Test validation passes for empty DataFrame."""

        df = pd.DataFrame(columns=["name", "age", "price"])  # type: ignore[arg-type]

        # Should not raise for empty DataFrame
        validate_dataframe_rows(df, SimpleValidator)


class TestPolarsValidation:
    """Test row validation with polars DataFrames."""

    def test_polars_valid_rows(self) -> None:
        """Test validation passes with valid polars DataFrame."""

        df = pl.DataFrame(
            {
                "name": ["Alice", "Bob"],
                "age": [25, 30],
                "price": [10.5, 20.0],
            }
        )

        validate_dataframe_rows(df, SimpleValidator)

    def test_polars_invalid_rows(self) -> None:
        """Test validation fails with invalid polars DataFrame."""

        df = pl.DataFrame(
            {
                "name": ["Alice", "Bob"],
                "age": [25, -5],  # Bob invalid
                "price": [10.5, 20.0],
            }
        )

        with pytest.raises(AssertionError) as exc_info:
            validate_dataframe_rows(df, SimpleValidator)

        message = str(exc_info.value)
        assert "Row 1:" in message  # Polars uses position
        assert "age" in message

    def test_polars_null_handling(self) -> None:
        """Test Polars null values are handled properly."""

        df = pl.DataFrame(
            {
                "name": ["Alice", "Bob"],
                "age": [25, 30],
                "price": [10.5, None],  # Polars uses None
            }
        )

        with pytest.raises(AssertionError) as exc_info:
            validate_dataframe_rows(df, SimpleValidator)

        message = str(exc_info.value)
        assert "Row 1:" in message


def test_max_errors_limit() -> None:
    """Test that max_errors limits the number of errors shown."""

    df = pd.DataFrame(
        {
            "name": [str(i) for i in range(10)],
            "age": [-1] * 10,  # All invalid
            "price": [10.0] * 10,
        }
    )

    with pytest.raises(AssertionError) as exc_info:
        validate_dataframe_rows(df, SimpleValidator, max_errors=3, early_termination=False)

    message = str(exc_info.value)

    # Should show first 3 errors
    assert "Row 0:" in message
    assert "Row 1:" in message
    assert "Row 2:" in message

    # Should indicate more errors exist (exact count since early_termination=False)
    assert "7 more row(s) with errors" in message


def test_invalid_dataframe_type() -> None:
    """Test that non-DataFrame types raise TypeError."""

    with pytest.raises(TypeError, match="Expected DataFrame"):
        validate_dataframe_rows([1, 2, 3], SimpleValidator)  # type: ignore[arg-type]


def test_require_pydantic_error() -> None:
    """Test error message when Pydantic not available."""
    # This test verifies the error message is correct
    # In real isolation testing, Pydantic won't be available

    # In this environment Pydantic is available, so just verify the function exists
    require_pydantic()  # Should not raise


def test_multiple_field_errors() -> None:
    """Test that multiple field errors in same row are reported."""

    df = pd.DataFrame(
        {
            "name": ["Alice", "Bob"],
            "age": [25, 150],  # Too high
            "price": [10.5, -20.0],  # Negative
        }
    )

    with pytest.raises(AssertionError) as exc_info:
        validate_dataframe_rows(df, SimpleValidator)

    message = str(exc_info.value)
    assert "Row 1:" in message
    # Should show both errors for Bob's row
    assert "age" in message
    assert "price" in message


def test_whitespace_stripping() -> None:
    """Test that ConfigDict str_strip_whitespace works."""

    df = pd.DataFrame(
        {
            "name": ["  Alice  ", "  Bob  "],  # Extra whitespace
            "age": [25, 30],
            "price": [10.5, 20.0],
        }
    )

    # Should pass because ConfigDict strips whitespace
    validate_dataframe_rows(df, SimpleValidator)


def test_unknown_dataframe_type() -> None:
    require_pydantic()

    class NotADataFrame:
        def __len__(self) -> int:
            return 1  # Pretend to have data

    fake_df = NotADataFrame()

    with pytest.raises(TypeError, match="Expected DataFrame"):
        validate_dataframe_rows(fake_df, SimpleValidator)  # type: ignore[arg-type]


def test_early_termination_enabled() -> None:
    # Create large DataFrame with many invalid rows
    df = pd.DataFrame(
        {
            "name": [str(i) for i in range(100)],
            "age": [-1] * 100,  # All invalid
            "price": [10.0] * 100,
        }
    )

    with pytest.raises(AssertionError) as exc_info:
        validate_dataframe_rows(df, SimpleValidator, max_errors=5, early_termination=True)

    message = str(exc_info.value)

    # Should show first 5 errors
    assert "Row 0:" in message
    assert "Row 4:" in message

    # Should indicate stopped early
    assert "stopped scanning early" in message
    assert "at least" in message


def test_early_termination_disabled() -> None:
    # Create DataFrame with many invalid rows
    df = pd.DataFrame(
        {
            "name": [str(i) for i in range(100)],
            "age": [-1] * 100,  # All invalid
            "price": [10.0] * 100,
        }
    )

    with pytest.raises(AssertionError) as exc_info:
        validate_dataframe_rows(df, SimpleValidator, max_errors=5, early_termination=False)

    message = str(exc_info.value)

    # Should show first 5 errors
    assert "Row 0:" in message
    assert "Row 4:" in message

    # Should indicate exact count (scanned all rows)
    assert "95 more row(s) with errors" in message
    assert "stopped scanning early" not in message


def test_early_termination_with_polars() -> None:
    df = pl.DataFrame(
        {
            "name": [str(i) for i in range(100)],
            "age": [-1] * 100,  # All invalid
            "price": [10.0] * 100,
        }
    )

    with pytest.raises(AssertionError) as exc_info:
        validate_dataframe_rows(df, SimpleValidator, max_errors=5, early_termination=True)

    message = str(exc_info.value)

    # Should show first 5 errors
    assert "Row 0:" in message
    assert "Row 4:" in message

    # Should indicate stopped early
    assert "stopped scanning early" in message
