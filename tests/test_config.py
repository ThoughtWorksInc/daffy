"""Tests for the daffy configuration system."""

import os
import tempfile
from unittest.mock import patch

from daffy.config import get_config, get_strict


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
            # Reset the cache to force reloading
            from daffy.config import load_config

            globals()["_config_cache"] = None

            config = load_config()
            assert config["strict"] is True
