"""Duplicate detection helpers."""


def duplicate_count(values: list[str]) -> int:
    """Count duplicate values in a collection."""
    return len(values) - len(set(values))
