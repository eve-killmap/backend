import time

from app.database import db
from app.filters import Filter, build_map_sql, build_system_sql
from app.models import SystemKillsResponse, SystemKillIdsResponse
from app import prometheus_metrics as pm


async def fetch_filtered_map(f: Filter) -> SystemKillsResponse:
    pm.filter_conditions.observe(len(f.conditions))
    sql, params = build_map_sql(f)
    _start = time.perf_counter()
    try:
        rows = await db.fetch(sql, *params)
    finally:
        pm.facet_query_seconds.labels(query="map").observe(time.perf_counter() - _start)
    system_ids, all_c, day, week, month, six, year = ([] for _ in range(7))
    for row in rows:
        system_ids.append(row["solar_system_id"])
        all_c.append(row["all_count"])
        day.append(row["day_count"])
        week.append(row["week_count"])
        month.append(row["month_count"])
        six.append(row["six_months_count"])
        year.append(row["year_count"])
    return SystemKillsResponse(
        system_ids=system_ids, all=all_c, day=day, week=week,
        month=month, six_months=six, year=year,
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
        pm.facet_query_seconds.labels(query="system").observe(time.perf_counter() - _start)
    ids = [r["killmail_id"] for r in rows]
    return SystemKillIdsResponse(count=len(ids), killmail_ids=ids)
