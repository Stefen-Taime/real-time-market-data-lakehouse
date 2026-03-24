"""Null value quality checks."""

from typing import Any


def null_ratio(values: list[Any]) -> float:
    """Return the ratio of null values in a sequence."""
    if not values:
        return 0.0

    null_count = sum(value is None for value in values)
    return null_count / len(values)
