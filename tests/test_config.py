"""Tests for the daffy configuration system."""

import os
import tempfile
from unittest.mock import patch

from daffy.config import clear_config_cache, get_checks_max_samples, get_config, get_strict


def test_get_config_default() -> None:
    """Test that get_config returns default values when no config file is found."""
    with patch("daffy.config.find_config_file", return_value=None):
        config = get_config()
        assert config["strict"] is False


def test_get_strict_default() -> None:
    """Test that get_strict returns default value when no explicit value is provided."""
    with patch("daffy.config.get_config", return_value={"strict": False}):
        assert get_strict() is False

    with patch("daffy.config.get_config", return_value={"strict": True}):
        assert get_strict() is True


def test_get_strict_override() -> None:
    """Test that get_strict respects explicitly provided value."""
    with patch("daffy.config.get_config", return_value={"strict": False}):
        assert get_strict(True) is True

    with patch("daffy.config.get_config", return_value={"strict": True}):
        assert get_strict(False) is False


def test_config_from_pyproject() -> None:
    """Test loading configuration from pyproject.toml."""
    with tempfile.TemporaryDirectory() as tmpdir:
        # Create a mock pyproject.toml file
        with open(os.path.join(tmpdir, "pyproject.toml"), "w") as f:
            f.write("""
[tool.daffy]
strict = true
            """)

        # Test loading from this file
        with patch("daffy.config.os.getcwd", return_value=tmpdir):
            from daffy.config import load_config

            clear_config_cache()

            config = load_config()
            assert config["strict"] is True


def test_load_config_returns_default_when_file_not_found() -> None:
    with patch("daffy.config.find_config_file", return_value="/nonexistent/pyproject.toml"):
        config = get_config()
        assert config["strict"] is False


def test_load_config_returns_default_when_toml_malformed() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        with open(os.path.join(tmpdir, "pyproject.toml"), "w") as f:
            f.write("invalid toml [[[")

        with patch("daffy.config.os.getcwd", return_value=tmpdir):
            clear_config_cache()

            config = get_config()
            assert config["strict"] is False


def test_find_config_file_returns_none_when_no_pyproject_exists() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        with patch("daffy.config.os.getcwd", return_value=tmpdir):
            from daffy.config import find_config_file

            result = find_config_file()
            assert result is None


def test_load_config_without_strict_setting() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        with open(os.path.join(tmpdir, "pyproject.toml"), "w") as f:
            f.write("""
[tool.daffy]
other_setting = "value"
            """)

        with patch("daffy.config.os.getcwd", return_value=tmpdir):
            clear_config_cache()

            config = get_config()
            assert config["strict"] is False


def test_load_config_daffy_section_without_strict() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        with open(os.path.join(tmpdir, "pyproject.toml"), "w") as f:
            f.write("""
[tool.daffy]
some_other_setting = "value"
            """)

        with patch("daffy.config.os.getcwd", return_value=tmpdir):
            clear_config_cache()

            config = get_config()
            assert config["strict"] is False


def test_get_checks_max_samples_default() -> None:
    with patch("daffy.config.get_config", return_value={"checks_max_samples": 5}):
        assert get_checks_max_samples() == 5


def test_get_checks_max_samples_override() -> None:
    with patch("daffy.config.get_config", return_value={"checks_max_samples": 5}):
        assert get_checks_max_samples(10) == 10


def test_checks_max_samples_from_pyproject() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        with open(os.path.join(tmpdir, "pyproject.toml"), "w") as f:
            f.write("""
[tool.daffy]
checks_max_samples = 10
            """)

        with patch("daffy.config.os.getcwd", return_value=tmpdir):
            from daffy.config import load_config

            clear_config_cache()

            config = load_config()
            assert config["checks_max_samples"] == 10
