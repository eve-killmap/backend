from __future__ import annotations

from datetime import datetime


def iso_to_epoch(value: str | None) -> int | None:
    """Convert an ESI ISO-8601 timestamp to a Unix epoch (seconds), or None."""
    if value is None:
        return None
    return int(datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp())
