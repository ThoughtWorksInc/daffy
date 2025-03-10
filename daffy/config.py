"""Configuration handling for DAFFY."""

import os
from typing import Any, Dict, Optional

import tomli


def load_config() -> Dict[str, Any]:
    """
    Load daffy configuration from pyproject.toml.

    Returns:
        dict: Configuration dictionary with daffy settings
    """
    default_config = {"strict": False}

    # Try to find pyproject.toml in the current directory or parent directories
    config_path = find_config_file()
    if not config_path:
        return default_config

    try:
        with open(config_path, "rb") as f:
            pyproject = tomli.load(f)

        # Extract daffy configuration if it exists
        daffy_config = pyproject.get("tool", {}).get("daffy", {})

        # Update default config with values from pyproject.toml
        if "strict" in daffy_config:
            default_config["strict"] = bool(daffy_config["strict"])

        return default_config
    except (FileNotFoundError, tomli.TOMLDecodeError):
        return default_config


def find_config_file() -> Optional[str]:
    """
    Find pyproject.toml in the user's project directory.

    This searches only in the current working directory (where the user's code is running),
    not in daffy's installation directory.

    Returns:
        str or None: Path to pyproject.toml if found, None otherwise
    """
    # Only look for pyproject.toml in the current working directory,
    # which should be the user's project directory, not daffy's installation directory
    current_dir = os.getcwd()
    config_path = os.path.join(current_dir, "pyproject.toml")

    if os.path.isfile(config_path):
        return config_path

    return None


# Cache config to avoid reading the file multiple times
_config_cache = None


def get_config() -> Dict[str, Any]:
    """
    Get the daffy configuration, loading it if necessary.

    Returns:
        dict: Configuration dictionary with daffy settings
    """
    global _config_cache
    if _config_cache is None:
        _config_cache = load_config()
    return _config_cache


def get_strict(strict_param: Optional[bool] = None) -> bool:
    """
    Get the strict mode setting, with explicit parameter taking precedence over configuration.

    Args:
        strict_param: Explicitly provided strict parameter value, or None to use config

    Returns:
        bool: The effective strict mode setting
    """
    if strict_param is not None:
        return strict_param
    return bool(get_config()["strict"])
