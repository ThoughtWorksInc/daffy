"""Tests for df_in decorator with row validation."""

from typing import Any

import pandas as pd
import pytest
from pydantic import BaseModel, ConfigDict, Field

from daffy import df_in


class PersonValidator(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True)

    name: str
    age: int = Field(ge=0, le=120)


def test_df_in_with_valid_rows() -> None:
    @df_in(row_validator=PersonValidator)
    def process_people(df: Any) -> Any:
        return df

    df = pd.DataFrame(
        {
            "name": ["Alice", "Bob"],
            "age": [25, 30],
        }
    )

    result = process_people(df)
    assert result.equals(df)


def test_df_in_with_invalid_rows() -> None:
    @df_in(row_validator=PersonValidator)
    def process_people(df: Any) -> Any:
        return df

    df = pd.DataFrame(
        {
            "name": ["Alice", "Bob"],
            "age": [25, -5],
        }
    )

    with pytest.raises(AssertionError) as exc_info:
        process_people(df)

    message = str(exc_info.value)
    assert "Row validation failed" in message
    assert "Row 1:" in message
    assert "age" in message
    assert "function 'process_people' parameter 'df'" in message


def test_df_in_with_columns_and_row_validator() -> None:
    @df_in(columns=["name", "age"], row_validator=PersonValidator)
    def process_people(df: Any) -> Any:
        return df

    df = pd.DataFrame(
        {
            "name": ["Alice"],
        }
    )

    with pytest.raises(AssertionError, match="Missing columns"):
        process_people(df)

    df = pd.DataFrame(
        {
            "name": ["Alice"],
            "age": [150],
        }
    )

    with pytest.raises(AssertionError) as exc_info:
        process_people(df)

    message = str(exc_info.value)
    assert "Row validation failed" in message


def test_df_in_with_named_parameter() -> None:
    @df_in(name="people_df", row_validator=PersonValidator)
    def process(data: Any, people_df: Any, config: Any = None) -> Any:
        return people_df

    df = pd.DataFrame(
        {
            "name": ["Alice", "Bob"],
            "age": [25, -5],
        }
    )

    with pytest.raises(AssertionError) as exc_info:
        process("other", df, "config")

    message = str(exc_info.value)
    assert "people_df" in message


def test_df_in_without_row_validator() -> None:
    @df_in(columns=["name", "age"])
    def process_people(df: Any) -> Any:
        return df

    df = pd.DataFrame(
        {
            "name": ["Alice", "Bob"],
            "age": [25, -5],
        }
    )

    result = process_people(df)
    assert result.equals(df)
