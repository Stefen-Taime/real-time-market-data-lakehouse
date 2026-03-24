"""Notification helper functions."""


def build_notification_message(event_name: str, status: str, details: str = "") -> str:
    """Build a lightweight notification message."""
    suffix = f": {details}" if details else ""
    return f"[{status}] {event_name}{suffix}"
