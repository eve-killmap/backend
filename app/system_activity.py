import time
from datetime import datetime, timezone

from app.database import db
from app.models import SystemActivityResponse

WINDOW_SECONDS = 12 * 3600

_SQL = """
SELECT killmail_time
FROM kills
WHERE solar_system_id = $1 AND killmail_time >= $2 AND killmail_time < $3
"""


def bin_index(ts: float, start: int, bins: int) -> int:
    raw = int((ts - start) * bins // WINDOW_SECONDS)
    return min(bins - 1, max(0, raw))


async def fetch_system_activity(
    solar_system_id: int, bins: int, now: int | None = None
) -> SystemActivityResponse:
    computed_at = int(time.time()) if now is None else now
    start = computed_at - WINDOW_SECONDS
    rows = await db.fetch(
        _SQL,
        solar_system_id,
        datetime.fromtimestamp(start, tz=timezone.utc),
        datetime.fromtimestamp(computed_at, tz=timezone.utc),
    )
    counts = [0] * bins
    for r in rows:
        counts[bin_index(r["killmail_time"].timestamp(), start, bins)] += 1
    return SystemActivityResponse(computed_at=computed_at, counts=counts)
