"""Schema-oriented quality checks."""

from collections.abc import Iterable


def required_columns_present(columns: Iterable[str], required: Iterable[str]) -> bool:
    """Return True when all required columns are present."""
    return set(required).issubset(set(columns))
