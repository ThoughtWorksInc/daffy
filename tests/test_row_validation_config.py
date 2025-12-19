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
