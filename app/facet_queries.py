from app.database import db
from app.filters import Filter, build_map_sql, build_system_sql
from app.models import SystemKillsResponse


async def fetch_filtered_map(f: Filter) -> SystemKillsResponse:
    sql, params = build_map_sql(f)
    rows = await db.fetch(sql, *params)
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
