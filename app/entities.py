from __future__ import annotations

import asyncio

import asyncpg
import redis.asyncio as aioredis
from redis.exceptions import RedisError

from app.config import config
from app.database import db
from app import prometheus_metrics as pm
from app.models import WarParticipant, WarProcessed, WarSummary

_redis: aioredis.Redis | None = None


def set_redis(redis_client: aioredis.Redis) -> None:
    global _redis
    _redis = redis_client

_CHAR_SQL = "SELECT character_id AS id, name FROM characters WHERE character_id = ANY($1::bigint[])"
_CORP_SQL = "SELECT corporation_id AS id, name, ticker FROM corporations WHERE corporation_id = ANY($1::int[])"
_ALLIANCE_SQL = "SELECT alliance_id AS id, name, ticker FROM alliances WHERE alliance_id = ANY($1::int[])"
_FACTION_SQL = "SELECT faction_id AS id, name FROM factions WHERE faction_id = ANY($1::int[])"


async def _fetch(sql: str, ids: set[int]) -> list[asyncpg.Record]:
    if not ids:
        return []
    return await db.fetch(sql, list(ids))


def _count(kind: str, requested: set[int], resolved: dict) -> None:
    for entity_id in requested:
        result = "found" if entity_id in resolved else "missing"
        pm.entity_lookups.labels(kind=kind, result=result).inc()


async def fetch_entity_names(
    character_ids: set[int],
    corp_ids: set[int],
    alliance_ids: set[int],
    faction_ids: set[int],
    *,
    emit_metrics: bool = True,
) -> tuple[dict[int, str], dict[int, tuple[str, str]], dict[int, tuple[str, str]], dict[int, str]]:
    """Resolve entity ids to names/tickers from the DB reference tables.

    Returns (character_names, corp_info{id:(name,ticker)}, alliance_info{id:(name,ticker)},
    faction_names). Rows with a NULL name are omitted (caller's .get(...) yields the
    existing "Unknown"/None). Emits entity_lookups per requested id (found|missing)
    unless emit_metrics=False (e.g. the ambiguous-id probe on /universe/names, which
    would otherwise skew the metric with intentional misses).
    """
    char_rows, corp_rows, alliance_rows, faction_rows = await asyncio.gather(
        _fetch(_CHAR_SQL, character_ids),
        _fetch(_CORP_SQL, corp_ids),
        _fetch(_ALLIANCE_SQL, alliance_ids),
        _fetch(_FACTION_SQL, faction_ids),
    )

    character_names = {r["id"]: r["name"] for r in char_rows if r["name"] is not None}
    corp_info = {r["id"]: (r["name"], r["ticker"]) for r in corp_rows if r["name"] is not None}
    alliance_info = {r["id"]: (r["name"], r["ticker"]) for r in alliance_rows if r["name"] is not None}
    faction_names = {r["id"]: r["name"] for r in faction_rows if r["name"] is not None}

    if emit_metrics:
        _count("character", character_ids, character_names)
        _count("corporation", corp_ids, corp_info)
        _count("alliance", alliance_ids, alliance_info)
        _count("faction", faction_ids, faction_names)

    return character_names, corp_info, alliance_info, faction_names


async def fetch_war(war_id: int) -> asyncpg.Record | None:
    """Return the war row only when present AND resolved (resolved_at IS NOT NULL);
    None for a stub (resolved_at NULL) or an absent row. Emits war_lookups."""
    row = await db.fetchrow("SELECT * FROM wars WHERE war_id = $1", war_id)
    if row is None:
        pm.war_lookups.labels(result="absent").inc()
        return None
    if row["resolved_at"] is None:
        pm.war_lookups.labels(result="stub").inc()
        return None
    pm.war_lookups.labels(result="resolved").inc()
    return row


def _epoch(value) -> int | None:
    return int(value.timestamp()) if value is not None else None


def _participant(corp_id, alliance_id, ships_killed, corp_info, alliance_info) -> WarParticipant:
    corp = corp_info.get(corp_id) if corp_id is not None else None
    alliance = alliance_info.get(alliance_id) if alliance_id is not None else None
    return WarParticipant(
        alliance=alliance[0] if alliance else None,
        alliance_ticker=alliance[1] if alliance else None,
        corporation=corp[0] if corp else None,
        corporation_ticker=corp[1] if corp else None,
        ships_killed=ships_killed or 0,
    )


_WAR_COLUMNS = (
    "war_id, declared, started, finished, retracted, mutual, "
    "aggressor_corporation_id, aggressor_alliance_id, "
    "defender_corporation_id, defender_alliance_id, "
    "ally_corporation_ids, ally_alliance_ids"
)


async def search_wars(
    aggressor: tuple[str, int] | None,
    defender: tuple[str, int] | None,
    limit: int,
) -> list[asyncpg.Record]:
    """Wars where `aggressor` is on the aggressor side and/or `defender` is on
    the defender/ally side. Each side is (kind, id) with kind in
    {'alliance','corporation'}. At least one side must be given (caller enforces)."""
    clauses: list[str] = []
    params: list = []

    if aggressor is not None:
        kind, aid = aggressor
        col = "aggressor_alliance_id" if kind == "alliance" else "aggressor_corporation_id"
        params.append(aid)
        clauses.append(f"{col} = ${len(params)}")

    if defender is not None:
        kind, did = defender
        dcol = "defender_alliance_id" if kind == "alliance" else "defender_corporation_id"
        acol = "ally_alliance_ids" if kind == "alliance" else "ally_corporation_ids"
        params.append(did)
        dclause = f"{dcol} = ${len(params)}"
        params.append([did])
        aclause = f"{acol} @> ${len(params)}::int[]"
        clauses.append(f"({dclause} OR {aclause})")

    params.append(limit)
    sql = (
        f"SELECT {_WAR_COLUMNS} FROM wars WHERE {' AND '.join(clauses)} "
        f"ORDER BY declared DESC NULLS LAST LIMIT ${len(params)}"
    )
    return await db.fetch(sql, *params)


def war_summary_from_row(row) -> WarSummary:
    """Map a wars row (carrying the _WAR_COLUMNS fields) to a WarSummary.
    Shared by /wars/search and /wars/details so both emit an identical shape."""
    return WarSummary(
        war_id=row["war_id"],
        declared=_epoch(row["declared"]),
        started=_epoch(row["started"]),
        finished=_epoch(row["finished"]),
        retracted=_epoch(row["retracted"]),
        mutual=bool(row["mutual"]),
        aggressor_corporation_id=row["aggressor_corporation_id"],
        aggressor_alliance_id=row["aggressor_alliance_id"],
        defender_corporation_id=row["defender_corporation_id"],
        defender_alliance_id=row["defender_alliance_id"],
        ally_corporation_ids=list(row["ally_corporation_ids"] or []),
        ally_alliance_ids=list(row["ally_alliance_ids"] or []),
    )


async def _fetch_war_rows(ids: list[int]) -> list[asyncpg.Record]:
    if not ids:
        return []
    # resolved_at is fetched (beyond _WAR_COLUMNS) only to decide cacheability.
    return await db.fetch(
        f"SELECT {_WAR_COLUMNS}, resolved_at FROM wars WHERE war_id = ANY($1)",
        ids,
    )


async def get_war_details(war_ids: list[int]) -> list[WarSummary]:
    """Resolve a batch of war ids to WarSummary rows (same shape as search_wars).

    Per-war-id Redis-cached with ``war_details_ttl``. Absent ids are omitted.
    Unenriched stubs (resolved_at IS NULL) are returned but NOT cached, so a war
    mid-enrichment picks up its participants on the next request instead of
    serving a bare label for the whole TTL. Degrades to a live DB read on any
    Redis error or when no cache is configured."""
    if not war_ids:
        return []
    ids = list(dict.fromkeys(war_ids))  # dedupe, preserve request order

    if _redis is None:
        rows = await _fetch_war_rows(ids)
        by_id = {r["war_id"]: war_summary_from_row(r) for r in rows}
        return [by_id[i] for i in ids if i in by_id]

    try:
        cached = await _redis.mget(*[f"war:details:{i}" for i in ids])
    except RedisError:
        pm.errors.labels(component="cache").inc()
        cached = [None] * len(ids)

    result: dict[int, WarSummary] = {}
    misses: list[int] = []
    for war_id, raw in zip(ids, cached):
        if raw is not None:
            result[war_id] = WarSummary.model_validate_json(raw)
        else:
            misses.append(war_id)

    if misses:
        rows = await _fetch_war_rows(misses)
        pipe = _redis.pipeline()
        to_cache = 0
        for r in rows:
            summary = war_summary_from_row(r)
            result[r["war_id"]] = summary
            if r["resolved_at"] is not None:
                pipe.set(
                    f"war:details:{r['war_id']}",
                    summary.model_dump_json(),
                    ex=config.cache.war_details_ttl,
                )
                to_cache += 1
        if to_cache:
            try:
                await pipe.execute()
            except RedisError:
                pm.errors.labels(component="cache").inc()

    return [result[i] for i in ids if i in result]


def build_war_processed(war_row, corp_info, alliance_info) -> WarProcessed:
    """Build WarProcessed from a resolved war row + already-resolved corp/alliance dicts."""
    return WarProcessed(
        aggressor=_participant(
            war_row["aggressor_corporation_id"], war_row["aggressor_alliance_id"],
            war_row["aggressor_ships_killed"], corp_info, alliance_info,
        ),
        defender=_participant(
            war_row["defender_corporation_id"], war_row["defender_alliance_id"],
            war_row["defender_ships_killed"], corp_info, alliance_info,
        ),
        declared=_epoch(war_row["declared"]),
        finished=_epoch(war_row["finished"]),
        mutual=bool(war_row["mutual"]),
        retracted=_epoch(war_row["retracted"]),
        started=_epoch(war_row["started"]),
    )
