import time
from datetime import date

from app.database import db
from app.filters import Filter, build_map_sql, build_system_sql
from app.models import SystemKillsResponse, SystemKillIdsResponse
from app import prometheus_metrics as pm


async def fetch_filtered_map(
    f: Filter, start: date | None = None, end: date | None = None
) -> SystemKillsResponse:
    pm.filter_conditions.observe(len(f.conditions))
    sql, params = build_map_sql(f, start, end)
    _start = time.perf_counter()
    try:
        rows = await db.fetch(sql, *params)
    finally:
        pm.facet_query_seconds.labels(query="map").observe(time.perf_counter() - _start)
    return SystemKillsResponse(
        system_ids=[r["solar_system_id"] for r in rows],
        counts=[r["kill_count"] for r in rows],
    )


async def fetch_filtered_system_kill_ids(
    solar_system_id: int, f: Filter
) -> SystemKillIdsResponse:
    pm.filter_conditions.observe(len(f.conditions))
    sql, params = build_system_sql(f, solar_system_id)
    _start = time.perf_counter()
    try:
        rows = await db.fetch(sql, *params)
    finally:
        pm.facet_query_seconds.labels(query="system").observe(
            time.perf_counter() - _start
        )
    ids = [r["killmail_id"] for r in rows]
    return SystemKillIdsResponse(count=len(ids), killmail_ids=ids)
