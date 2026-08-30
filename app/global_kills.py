import time
from datetime import date

from app.database import db
from app.filters import Filter, build_global_kills_sql, _bin_expr
from app import prometheus_metrics as pm

EARLIEST_KILL_DATE = date(2015, 11, 3)

MAP_RANGES: dict[str, tuple[int, int]] = {
    "new-eden": (30000000, 31000000),
    "anoikis": (31000000, 32000000),
    "abyssal-deadspace": (32000000, 33000000),
    "tutorials": (34000000, 35000000),
}


async def fetch_global_kills(map_type: str, bins: int) -> list[int]:
    lo, hi = MAP_RANGES[map_type]  # KeyError -> caller maps to 400
    rows = await db.fetch(
        f"""
        SELECT {_bin_expr("day", "$3", "$4")} AS bin,
               SUM(kill_count) AS kill_count
        FROM mv_kills_per_system_daily
        WHERE solar_system_id >= $1 AND solar_system_id < $2
        GROUP BY bin
        """,
        lo,
        hi,
        bins,
        EARLIEST_KILL_DATE,
    )
    out = [0] * bins
    for r in rows:
        out[r["bin"]] = int(r["kill_count"])
    return out


async def fetch_filtered_global_kills(
    f: Filter, map_type: str, bins: int
) -> list[int]:
    lo, hi = MAP_RANGES[map_type]  # KeyError -> caller maps to 400
    pm.filter_conditions.observe(len(f.conditions))
    sql, params = build_global_kills_sql(f, lo, hi, bins, EARLIEST_KILL_DATE)
    _start = time.perf_counter()
    try:
        rows = await db.fetch(sql, *params)
    finally:
        pm.facet_query_seconds.labels(query="global_kills").observe(
            time.perf_counter() - _start
        )
    out = [0] * bins
    for r in rows:
        out[r["bin"]] = int(r["kill_count"])
    return out
