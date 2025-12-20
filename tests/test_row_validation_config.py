"""Tests for row validation configuration."""

from pathlib import Path
from typing import Any


def test_row_validation_config_defaults() -> None:
    from daffy.config import get_row_validation_max_errors

    assert get_row_validation_max_errors() == 5


def test_row_validation_config_from_file(tmp_path: Path, monkeypatch: Any) -> None:
    config_file = tmp_path / "pyproject.toml"
    config_file.write_text(
        """
[tool.daffy]
row_validation_max_errors = 10
"""
    )

    monkeypatch.chdir(tmp_path)

    from daffy.config import clear_config_cache, get_row_validation_max_errors

    clear_config_cache()

    assert get_row_validation_max_errors() == 10

    clear_config_cache()
