"""YAML configuration loading helpers."""

from pathlib import Path
from typing import Any

import yaml


def load_yaml(path: str | Path) -> dict[str, Any]:
    """Load a YAML file into a Python dictionary."""
    with Path(path).open("r", encoding="utf-8") as handle:
        return yaml.safe_load(handle) or {}


def deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    """Merge nested dictionaries, giving precedence to override values."""
    merged = dict(base)

    for key, value in override.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = deep_merge(merged[key], value)
            continue

        merged[key] = value

    return merged


def load_project_config(config_dir: str | Path = "config", env: str = "dev") -> dict[str, Any]:
    """Load and merge the project configuration for a target environment."""
    config_root = Path(config_dir)
    common_config = load_yaml(config_root / "common.yml")
    env_config = load_yaml(config_root / f"{env}.yml")
    symbols_config = load_yaml(config_root / "symbols.yml")

    merged = deep_merge(common_config, env_config)
    return deep_merge(merged, symbols_config)
