from __future__ import annotations

import asyncio

import asyncpg

from app.database import db
from app import prometheus_metrics as pm
from app.models import WarParticipant, WarProcessed

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
) -> tuple[dict[int, str], dict[int, tuple[str, str]], dict[int, tuple[str, str]], dict[int, str]]:
    """Resolve entity ids to names/tickers from the DB reference tables.

    Returns (character_names, corp_info{id:(name,ticker)}, alliance_info{id:(name,ticker)},
    faction_names). Rows with a NULL name are omitted (caller's .get(...) yields the
    existing "Unknown"/None). Emits entity_lookups per requested id (found|missing).
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
