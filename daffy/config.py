"""Configuration handling for DAFFY."""

import os
from functools import lru_cache
from typing import Any, Optional

import tomli

# Configuration keys
_KEY_STRICT = "strict"
_KEY_ROW_VALIDATION_MAX_ERRORS = "row_validation_max_errors"
_KEY_CHECKS_MAX_SAMPLES = "checks_max_samples"

# Default values
_DEFAULT_STRICT = False
_DEFAULT_MAX_ERRORS = 5
_DEFAULT_CHECKS_MAX_SAMPLES = 5


def load_config() -> dict[str, Any]:
    """
    Load daffy configuration from pyproject.toml.

    Returns:
        dict: Configuration dictionary with daffy settings
    """
    default_config = {
        _KEY_STRICT: _DEFAULT_STRICT,
        _KEY_ROW_VALIDATION_MAX_ERRORS: _DEFAULT_MAX_ERRORS,
        _KEY_CHECKS_MAX_SAMPLES: _DEFAULT_CHECKS_MAX_SAMPLES,
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
        if _KEY_STRICT in daffy_config:
            default_config[_KEY_STRICT] = bool(daffy_config[_KEY_STRICT])
        if _KEY_ROW_VALIDATION_MAX_ERRORS in daffy_config:
            default_config[_KEY_ROW_VALIDATION_MAX_ERRORS] = int(daffy_config[_KEY_ROW_VALIDATION_MAX_ERRORS])
        if _KEY_CHECKS_MAX_SAMPLES in daffy_config:
            default_config[_KEY_CHECKS_MAX_SAMPLES] = int(daffy_config[_KEY_CHECKS_MAX_SAMPLES])

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
    return bool(get_config()[_KEY_STRICT])


def get_row_validation_max_errors() -> int:
    """Get max_errors setting for row validation."""
    return int(get_config()[_KEY_ROW_VALIDATION_MAX_ERRORS])


def get_checks_max_samples(max_samples: Optional[int] = None) -> int:
    """Get max_samples setting for value checks."""
    if max_samples is not None:
        return max_samples
    return int(get_config()[_KEY_CHECKS_MAX_SAMPLES])
