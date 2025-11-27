"""Configuration handling for DAFFY."""

import os
from functools import lru_cache
from typing import Any, Optional

import tomli


def load_config() -> dict[str, Any]:
    """
    Load daffy configuration from pyproject.toml.

    Returns:
        dict: Configuration dictionary with daffy settings
    """
    default_config = {
        "strict": False,
        "row_validation_max_errors": 5,
        "row_validation_convert_nans": True,
    }

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
        if "row_validation_max_errors" in daffy_config:
            default_config["row_validation_max_errors"] = int(daffy_config["row_validation_max_errors"])
        if "row_validation_convert_nans" in daffy_config:
            default_config["row_validation_convert_nans"] = bool(daffy_config["row_validation_convert_nans"])

        return default_config
    except (FileNotFoundError, tomli.TOMLDecodeError):
        return default_config


def find_config_file() -> Optional[str]:
    """Find pyproject.toml in the current working directory."""
    path = os.path.join(os.getcwd(), "pyproject.toml")
    return path if os.path.isfile(path) else None


@lru_cache(maxsize=1)
def get_config() -> dict[str, Any]:
    """Get the daffy configuration, cached after first load."""
    return load_config()


def clear_config_cache() -> None:
    """Clear the configuration cache. Primarily for testing."""
    get_config.cache_clear()


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


def get_row_validation_config() -> dict[str, Any]:
    """Get all row validation configuration options."""
    config = get_config()
    return {
        "max_errors": config["row_validation_max_errors"],
        "convert_nans": config["row_validation_convert_nans"],
    }


def get_row_validation_max_errors(max_errors: Optional[int] = None) -> int:
    """Get max_errors setting for row validation."""
    if max_errors is not None:
        return max_errors
    return int(get_config()["row_validation_max_errors"])


def get_row_validation_convert_nans(convert_nans: Optional[bool] = None) -> bool:
    """Get convert_nans setting for row validation."""
    if convert_nans is not None:
        return convert_nans
    return bool(get_config()["row_validation_convert_nans"])
