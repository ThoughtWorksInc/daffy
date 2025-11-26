"""Tests for row validation configuration."""

from pathlib import Path
from typing import Any


def test_row_validation_config_defaults() -> None:
    from daffy.config import get_row_validation_config

    config = get_row_validation_config()
    assert config["max_errors"] == 5
    assert config["convert_nans"] is True


def test_row_validation_config_from_file(tmp_path: Path, monkeypatch: Any) -> None:
    config_file = tmp_path / "pyproject.toml"
    config_file.write_text(
        """
[tool.daffy]
row_validation_max_errors = 10
row_validation_convert_nans = false
"""
    )

    monkeypatch.chdir(tmp_path)

    from daffy.config import clear_config_cache, get_row_validation_config

    clear_config_cache()

    cfg = get_row_validation_config()
    assert cfg["max_errors"] == 10
    assert cfg["convert_nans"] is False

    clear_config_cache()


def test_explicit_overrides_config() -> None:
    from daffy.config import (
        get_row_validation_convert_nans,
        get_row_validation_max_errors,
    )

    assert get_row_validation_max_errors(20) == 20
    assert get_row_validation_convert_nans(False) is False


def test_row_validation_max_errors_uses_default() -> None:
    from daffy.config import get_row_validation_max_errors

    result = get_row_validation_max_errors(None)
    assert result == 5


def test_row_validation_convert_nans_uses_default() -> None:
    from daffy.config import get_row_validation_convert_nans

    result = get_row_validation_convert_nans(None)
    assert result is True
