"""Freshness and latency checks."""

from datetime import datetime, timezone


def age_in_seconds(event_time: datetime, reference_time: datetime | None = None) -> float:
    """Compute the age of an event in seconds."""
    if reference_time is None:
        reference_time = datetime.now(timezone.utc)

    return max((reference_time - event_time).total_seconds(), 0.0)
