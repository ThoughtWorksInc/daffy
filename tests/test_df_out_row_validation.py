"""Tests for df_out decorator with row validation."""

from typing import Any

import pandas as pd
import pytest
from pydantic import BaseModel, ConfigDict, Field

from daffy import df_out


class PersonValidator(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True)

    name: str
    age: int = Field(ge=0, le=120)


def test_df_out_with_valid_rows() -> None:
    @df_out(row_validator=PersonValidator)
    def create_people() -> Any:
        return pd.DataFrame(
            {
                "name": ["Alice", "Bob"],
                "age": [25, 30],
            }
        )

    result = create_people()
    assert len(result) == 2


def test_df_out_with_invalid_rows() -> None:
    @df_out(row_validator=PersonValidator)
    def create_people() -> Any:
        return pd.DataFrame(
            {
                "name": ["Alice", "Bob"],
                "age": [25, -5],
            }
        )

    with pytest.raises(AssertionError) as exc_info:
        create_people()

    message = str(exc_info.value)
    assert "Row validation failed" in message
    assert "Row 1:" in message
    assert "age" in message
    assert "function 'create_people' return value" in message


def test_df_out_with_columns_and_row_validator() -> None:
    @df_out(columns=["name", "age"], row_validator=PersonValidator)
    def create_people() -> Any:
        return pd.DataFrame(
            {
                "name": ["Alice"],
                "age": [150],
            }
        )

    with pytest.raises(AssertionError) as exc_info:
        create_people()

    message = str(exc_info.value)
    assert "Row validation failed" in message


def test_df_out_without_row_validator() -> None:
    @df_out(columns=["name", "age"])
    def create_people() -> Any:
        return pd.DataFrame(
            {
                "name": ["Alice", "Bob"],
                "age": [25, -5],
            }
        )

    result = create_people()
    assert len(result) == 2
