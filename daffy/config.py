"""Configuration handling for DAFFY."""

from __future__ import annotations

import os
from functools import lru_cache
from types import MappingProxyType
from typing import Any

import tomli

# Configuration keys
_KEY_STRICT = "strict"
_KEY_LAZY = "lazy"
_KEY_ROW_VALIDATION_MAX_ERRORS = "row_validation_max_errors"
_KEY_CHECKS_MAX_SAMPLES = "checks_max_samples"

# Default values
_DEFAULT_STRICT = False
_DEFAULT_LAZY = False
_DEFAULT_MAX_ERRORS = 5
_DEFAULT_CHECKS_MAX_SAMPLES = 5


def _validate_bool_config(daffy_config: dict[str, Any], key: str) -> bool | None:
    """Validate and extract a boolean config value.

    Returns None if key not present. Raises TypeError if value is not a boolean.
    """
    if key not in daffy_config:
        return None
    value = daffy_config[key]
    if not isinstance(value, bool):
        raise TypeError(f"Config '{key}' must be a boolean, got {type(value).__name__}: {value!r}")
    return value


def _validate_int_config(daffy_config: dict[str, Any], key: str, min_value: int = 1) -> int | None:
    """Validate and extract an integer config value.

    Returns None if key not present. Raises TypeError/ValueError if invalid.
    """
    if key not in daffy_config:
        return None
    value = daffy_config[key]
    if not isinstance(value, int) or isinstance(value, bool):
        raise TypeError(f"Config '{key}' must be an integer, got {type(value).__name__}: {value!r}")
    if value < min_value:
        raise ValueError(f"Config '{key}' must be >= {min_value}, got {value}")
    return value


def load_config() -> dict[str, Any]:
    """
    Load daffy configuration from pyproject.toml.

    Returns:
        dict: Configuration dictionary with daffy settings
    """
    default_config = {
        _KEY_STRICT: _DEFAULT_STRICT,
        _KEY_LAZY: _DEFAULT_LAZY,
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

        # Update default config with validated values from pyproject.toml
        strict = _validate_bool_config(daffy_config, _KEY_STRICT)
        if strict is not None:
            default_config[_KEY_STRICT] = strict

        lazy = _validate_bool_config(daffy_config, _KEY_LAZY)
        if lazy is not None:
            default_config[_KEY_LAZY] = lazy

        max_errors = _validate_int_config(daffy_config, _KEY_ROW_VALIDATION_MAX_ERRORS)
        if max_errors is not None:
            default_config[_KEY_ROW_VALIDATION_MAX_ERRORS] = max_errors

        max_samples = _validate_int_config(daffy_config, _KEY_CHECKS_MAX_SAMPLES)
        if max_samples is not None:
            default_config[_KEY_CHECKS_MAX_SAMPLES] = max_samples

        return default_config
    except (FileNotFoundError, tomli.TOMLDecodeError):
        return default_config


def find_config_file() -> str | None:
    """Find pyproject.toml in the current working directory."""
    path = os.path.join(os.getcwd(), "pyproject.toml")
    return path if os.path.isfile(path) else None


@lru_cache(maxsize=1)
def get_config() -> MappingProxyType[str, Any]:
    """Get the daffy configuration, cached after first load.

    Returns an immutable view of the configuration to prevent accidental modification.
    """
    return MappingProxyType(load_config())


def clear_config_cache() -> None:
    """Clear the configuration cache. Primarily for testing."""
    get_config.cache_clear()


def get_strict(strict_param: bool | None = None) -> bool:
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


def get_lazy(lazy_param: bool | None = None) -> bool:
    """
    Get the lazy mode setting, with explicit parameter taking precedence over configuration.

    When lazy=True, validation collects all errors before raising instead of stopping at the first.

    Args:
        lazy_param: Explicitly provided lazy parameter value, or None to use config

    Returns:
        bool: The effective lazy mode setting
    """
    if lazy_param is not None:
        return lazy_param
    return bool(get_config()[_KEY_LAZY])


def get_row_validation_max_errors() -> int:
    """Get max_errors setting for row validation."""
    value = int(get_config()[_KEY_ROW_VALIDATION_MAX_ERRORS])
    if value < 1:
        raise ValueError(f"row_validation_max_errors must be >= 1, got {value}")
    return value


def get_checks_max_samples(max_samples: int | None = None) -> int:
    """Get max_samples setting for value checks."""
    if max_samples is not None:
        if max_samples < 1:
            raise ValueError(f"checks_max_samples must be >= 1, got {max_samples}")
        return max_samples
    value = int(get_config()[_KEY_CHECKS_MAX_SAMPLES])
    if value < 1:
        raise ValueError(f"checks_max_samples must be >= 1, got {value}")
    return value
