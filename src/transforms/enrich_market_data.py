"""Helpers for enriching Silver market data."""

from typing import Any


def enrich_record(record: dict[str, Any], source: str) -> dict[str, Any]:
    """Attach lightweight metadata to a market data record."""
    enriched = record.copy()
    enriched["source"] = source
    return enriched
