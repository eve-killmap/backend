from app.database import db
from app.eve_images import image_url
from app.models import EntityCandidate, TypeCandidate

_ENTITY_SQL = {
    "character": (
        "SELECT character_id AS id, name, NULL::text AS ticker FROM characters "
        "WHERE name ILIKE '%'||$1||'%' ESCAPE '\\' ORDER BY name LIMIT $2"
    ),
    "corporation": (
        "SELECT corporation_id AS id, name, ticker FROM corporations "
        "WHERE name ILIKE '%'||$1||'%' ESCAPE '\\' OR ticker ILIKE '%'||$1||'%' ESCAPE '\\' "
        "ORDER BY name LIMIT $2"
    ),
    "alliance": (
        "SELECT alliance_id AS id, name, ticker FROM alliances "
        "WHERE name ILIKE '%'||$1||'%' ESCAPE '\\' OR ticker ILIKE '%'||$1||'%' ESCAPE '\\' "
        "ORDER BY name LIMIT $2"
    ),
    "faction": (
        "SELECT faction_id AS id, name, NULL::text AS ticker FROM factions "
        "WHERE name ILIKE '%'||$1||'%' ESCAPE '\\' ORDER BY name LIMIT $2"
    ),
}


def _escape_like(q: str) -> str:
    return q.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


async def autocomplete_entities(kind: str, q: str, limit: int) -> list[EntityCandidate]:
    rows = await db.fetch(_ENTITY_SQL[kind], _escape_like(q), limit)
    return [
        EntityCandidate(
            id=r["id"],
            name=r["name"],
            ticker=r["ticker"],
            image_url=image_url(kind, r["id"]),
        )
        for r in rows
    ]


async def autocomplete_types(q: str, limit: int) -> list[TypeCandidate]:
    rows = await db.fetch(
        "SELECT id, name FROM types WHERE published AND name ILIKE '%'||$1||'%' ESCAPE '\\' "
        "ORDER BY name LIMIT $2",
        _escape_like(q),
        limit,
    )
    return [
        TypeCandidate(id=r["id"], name=r["name"], image_url=image_url("type", r["id"]))
        for r in rows
    ]


async def autocomplete_weapons(q: str, limit: int) -> list[TypeCandidate]:
    rows = await db.fetch(
        "SELECT type_id AS id, name FROM mv_weapon_search "
        "WHERE name ILIKE '%'||$1||'%' ESCAPE '\\' ORDER BY name LIMIT $2",
        _escape_like(q),
        limit,
    )
    return [
        TypeCandidate(id=r["id"], name=r["name"], image_url=image_url("type", r["id"]))
        for r in rows
    ]
