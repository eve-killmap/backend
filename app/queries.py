import time
from datetime import datetime, timezone
from typing import Any

import redis.asyncio as aioredis

from app.config import config
from app.database import db
from app.schema import RawKillsColumns
from app.models import (
    RawKillDetailResponse,
    Attacker,
    Victim,
    KillDetail,
    RankSystem,
    TopSystems,
)

_redis: aioredis.Redis | None = None


def set_redis(redis_client: aioredis.Redis) -> None:
    global _redis
    _redis = redis_client


top_systems_views = {
    "all": "mv_kills_per_system_top",
    "24h": "mv_kills_per_system_top_24h",
    "7d": "mv_kills_per_system_top_7d",
    "30d": "mv_kills_per_system_top_30d",
    "6m": "mv_kills_per_system_top_6m",
    "1y": "mv_kills_per_system_top_1y",
}


async def fetch_kills_by_ids(killmail_ids: list[int]) -> RawKillDetailResponse:
    if not killmail_ids:
        return RawKillDetailResponse(count=0, kills=[])

    query = """
        SELECT
            k.killmail_id,
            k.killmail_time,
            k.position_x,
            k.position_y,
            k.position_z,
            k.victim_character_id,
            k.victim_corporation_id,
            k.victim_alliance_id,
            k.victim_faction_id,
            k.victim_damage_taken,
            k.victim_ship_type_id,
            k.war_id,
            k.inserted_time
        FROM kills k
        WHERE k.killmail_id = ANY($1)
    """
    kill_rows = await db.fetch(query, killmail_ids)

    if kill_rows:
        attacker_query = """
            SELECT
                killmail_id,
                character_id,
                corporation_id,
                alliance_id,
                faction_id,
                ship_type_id,
                weapon_type_id,
                damage_done,
                final_blow,
                security_status
            FROM kill_attackers
            WHERE killmail_id = ANY($1)
            ORDER BY killmail_id, attacker_index
        """
        attacker_rows = await db.fetch(attacker_query, killmail_ids)

        attackers_by_kill: dict[int, list[Attacker]] = {}
        for row in attacker_rows:
            kill_id = row["killmail_id"]
            if kill_id not in attackers_by_kill:
                attackers_by_kill[kill_id] = []
            attackers_by_kill[kill_id].append(
                Attacker(
                    character_id=row["character_id"],
                    corporation_id=row["corporation_id"],
                    alliance_id=row["alliance_id"],
                    faction_id=row["faction_id"],
                    ship_type_id=row["ship_type_id"],
                    weapon_type_id=row["weapon_type_id"],
                    damage_done=row["damage_done"],
                    final_blow=row["final_blow"],
                    security_status=row["security_status"],
                )
            )
    else:
        attackers_by_kill = {}

    kills = [
        KillDetail(
            killmail_id=row["killmail_id"],
            killmail_time=row["killmail_time"],
            position=(row["position_x"], row["position_y"], row["position_z"]),
            war_id=row["war_id"],
            victim=Victim(
                character_id=row["victim_character_id"],
                corporation_id=row["victim_corporation_id"],
                alliance_id=row["victim_alliance_id"],
                faction_id=row["victim_faction_id"],
                damage_taken=row["victim_damage_taken"],
                ship_type_id=row["victim_ship_type_id"],
            ),
            attackers=attackers_by_kill.get(row["killmail_id"], []),
            inserted_time=row["inserted_time"],
        )
        for row in kill_rows
    ]

    return RawKillDetailResponse(
        count=len(kills),
        kills=kills,
    )


async def fetch_system_latest_inserted(solar_system_id: int) -> int | None:
    return await db.fetchval(
        "SELECT EXTRACT(EPOCH FROM MAX(inserted_time))::BIGINT FROM kills WHERE solar_system_id = $1",
        solar_system_id,
    )


async def fetch_raw_kills(
    solar_system_id: int,
    since: int | None = None,
) -> RawKillsColumns:
    conditions = ["solar_system_id = $1"]
    params: list[Any] = [solar_system_id]

    if since is not None:
        conditions.append("inserted_time > $2")
        params.append(datetime.fromtimestamp(since, tz=timezone.utc))

    where_clause = " AND ".join(conditions)

    query = f"""
        SELECT
            killmail_id,
            position_x,
            position_y,
            position_z,
            EXTRACT(EPOCH FROM killmail_time)::BIGINT as killmail_time,
            victim_ship_type_id
        FROM kills
        WHERE {where_clause}
        ORDER BY killmail_time DESC
    """

    kill_rows = await db.fetch(query, *params)

    killmail_ids: list[int] = []
    x_list: list[float] = []
    y_list: list[float] = []
    z_list: list[float] = []
    killmail_time_list: list[int] = []
    ship_type_list: list[int] = []

    for row in kill_rows:
        killmail_ids.append(row["killmail_id"])
        x_list.append(row["position_x"])
        y_list.append(row["position_y"])
        z_list.append(row["position_z"])
        killmail_time_list.append(row["killmail_time"])
        ship_type_list.append(row["victim_ship_type_id"])

    return {
        "count": len(killmail_ids),
        "killmail_ids": killmail_ids,
        "x": x_list,
        "y": y_list,
        "z": z_list,
        "killmail_times": killmail_time_list,
        "ship_types": ship_type_list,
    }


async def fetch_top_systems(limit: int = 10) -> TopSystems:
    raw = {}

    for key, view in top_systems_views.items():
        query = f"""
            SELECT
                solar_system_id,
                kill_count
            FROM {view}
            ORDER BY kill_count DESC
            LIMIT $1
        """
        rows = await db.fetch(query, limit)

        data = [
            RankSystem(
                solar_system_id=row["solar_system_id"], kill_count=row["kill_count"]
            )
            for row in rows
        ]

        raw[key] = data

    result = TopSystems(
        all=raw["all"],
        day=raw["24h"],
        week=raw["7d"],
        month=raw["30d"],
        six_months=raw["6m"],
        year=raw["1y"],
    )

    return result


async def fetch_bottom_systems(limit: int = 10) -> list[RankSystem]:
    query = """
        SELECT
            solar_system_id,
            kill_count
        FROM mv_kills_per_system_bottom
        ORDER BY kill_count ASC
        LIMIT $1
    """
    rows = await db.fetch(query, limit)

    return [
        RankSystem(solar_system_id=row["solar_system_id"], kill_count=row["kill_count"])
        for row in rows
    ]


async def fetch_farthest_kill(solar_system_id: int) -> float | None:
    query = """
        SELECT
            farthest_kill
        FROM mv_farthest_kill_per_system
        WHERE solar_system_id = $1
    """
    return await db.fetchval(query, solar_system_id)


def normalize_farthest_kill(value: float | None) -> int:
    """Map a farthest-kill distance to the API value (-1 when the system has no kills)."""
    if value is None:
        return -1
    return int(value)


async def fetch_db_stats() -> dict:
    row = await db.fetchrow("""
        SELECT numbackends, xact_commit, xact_rollback, blks_read, blks_hit,
               tup_returned, tup_fetched, tup_inserted, tup_updated, tup_deleted,
               deadlocks, temp_files,
               EXTRACT(EPOCH FROM stats_reset)::BIGINT AS stats_reset_epoch
        FROM pg_stat_database
        WHERE datname = current_database()
        """)
    if row is None:
        return {}
    blks_hit = row["blks_hit"]
    blks_read = row["blks_read"]
    total_blks = blks_hit + blks_read
    return {
        "numbackends": row["numbackends"],
        "xact_commit": row["xact_commit"],
        "xact_rollback": row["xact_rollback"],
        "blks_hit": blks_hit,
        "blks_read": blks_read,
        "cache_hit_ratio": round(blks_hit / total_blks, 4) if total_blks else None,
        "tup_returned": row["tup_returned"],
        "tup_fetched": row["tup_fetched"],
        "tup_inserted": row["tup_inserted"],
        "tup_updated": row["tup_updated"],
        "tup_deleted": row["tup_deleted"],
        "deadlocks": row["deadlocks"],
        "temp_files": row["temp_files"],
        "stats_reset_epoch": row["stats_reset_epoch"],
    }


async def fetch_domain_stats() -> dict:
    row = await db.fetchrow("""
        SELECT
            (SELECT reltuples::BIGINT FROM pg_class WHERE relname = 'kills') AS total_kills_estimate,
            (SELECT reltuples::BIGINT FROM pg_class WHERE relname = 'kills_no_positions') AS no_position_estimate,
            (SELECT COUNT(*) FROM kills WHERE killmail_time >= NOW() - INTERVAL '1 hour') AS kills_1h,
            (SELECT COUNT(*) FROM kills WHERE killmail_time >= NOW() - INTERVAL '24 hours') AS kills_24h,
            (SELECT EXTRACT(EPOCH FROM MAX(killmail_time))::BIGINT FROM kills) AS latest_killmail_epoch,
            pg_total_relation_size('kills') AS kills_bytes,
            pg_total_relation_size('kill_attackers') AS attackers_bytes
        """)
    if row is None:
        return {}
    latest = row["latest_killmail_epoch"]
    lag = int(time.time()) - latest if latest is not None else None
    return {
        "total_kills_estimate": row["total_kills_estimate"],
        "no_position_estimate": row["no_position_estimate"],
        "kills_last_1h": row["kills_1h"],
        "kills_last_24h": row["kills_24h"],
        "latest_killmail_epoch": latest,
        "ingestion_lag_seconds": lag,
        "kills_table_bytes": row["kills_bytes"],
        "attackers_table_bytes": row["attackers_bytes"],
    }


async def fetch_top_statements(limit: int = 5) -> list[dict]:
    """Top queries by total time from pg_stat_statements. Best-effort: returns []
    if the extension is not installed (the query raises UndefinedTable)."""
    try:
        rows = await db.fetch(
            """
            SELECT query, calls, total_exec_time, mean_exec_time, rows
            FROM pg_stat_statements
            ORDER BY total_exec_time DESC
            LIMIT $1
            """,
            limit,
        )
    except Exception:
        return []
    return [
        {
            "query": r["query"][:200],
            "calls": r["calls"],
            "total_exec_time_ms": round(r["total_exec_time"], 2),
            "mean_exec_time_ms": round(r["mean_exec_time"], 2),
            "rows": r["rows"],
        }
        for r in rows
    ]


def merge_kill_details(found: list[KillDetail]) -> RawKillDetailResponse:
    return RawKillDetailResponse(count=len(found), kills=found)


async def get_kill_details_cached(killmail_ids: list[int]) -> RawKillDetailResponse:
    """Per-killmail-id cached raw details. Kills are immutable, so each is cached
    individually under `killdetail:{id}` with a long TTL; overlapping requests reuse
    entries. Only cache-missing ids hit the DB."""
    if not killmail_ids:
        return RawKillDetailResponse(count=0, kills=[])

    found: list[KillDetail] = []
    misses: list[int] = killmail_ids

    if _redis is not None:
        cached_values = await _redis.mget(
            *[f"killdetail:{kid}" for kid in killmail_ids]
        )
        misses = []
        for kid, raw in zip(killmail_ids, cached_values):
            if raw is not None:
                found.append(KillDetail.model_validate_json(raw))
            else:
                misses.append(kid)

    if misses:
        fetched = await fetch_kills_by_ids(misses)
        if _redis is not None and fetched.kills:
            pipe = _redis.pipeline()
            for kill in fetched.kills:
                pipe.set(
                    f"killdetail:{kill.killmail_id}",
                    kill.model_dump_json(),
                    ex=config.cache.kill_detail_ttl,
                )
            await pipe.execute()
        found.extend(fetched.kills)

    return merge_kill_details(found)


async def get_type_names(ids: set[int]) -> dict[int, str]:
    if not ids:
        return {}

    result: dict[int, str] = {}
    uncached: list[int] = []

    if _redis is not None:
        cached_values = await _redis.mget(
            *[f"db:type_name:{type_id}" for type_id in ids]
        )
        for type_id, val in zip(ids, cached_values):
            if val is not None:
                # mget may return bytes when decode_responses=False; normalise.
                result[type_id] = val.decode() if isinstance(val, bytes) else val
            else:
                uncached.append(type_id)
    else:
        uncached = list(ids)

    if uncached:
        rows = await db.fetch(
            "SELECT id, name FROM types WHERE id = ANY($1::int[])",
            uncached,
        )
        if _redis is not None:
            pipe = _redis.pipeline()
            for row in rows:
                pipe.set(
                    f"db:type_name:{row['id']}",
                    row["name"],
                    ex=config.cache.type_name_ttl,
                )
                result[row["id"]] = row["name"]
            await pipe.execute()
        else:
            for row in rows:
                result[row["id"]] = row["name"]

    return result
