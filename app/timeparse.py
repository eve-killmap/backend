from __future__ import annotations

from datetime import datetime


def iso_to_epoch(value: str | None) -> int | None:
    if value is None:
        return None
    return int(datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp())


def datetime_to_epoch(value: datetime | None) -> int | None:
    if value is None:
        return None
    return int(value.timestamp())
