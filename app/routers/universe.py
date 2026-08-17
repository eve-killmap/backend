import asyncio
import time
from typing import Annotated

from fastapi import APIRouter, Body, Header, HTTPException

from app.config import config
from app.security import validate_id_list
from app.queries import get_type_names
from app import entities
from app.id_ranges import (
    classify_id,
    CATEGORY_TYPE,
    CATEGORY_FACTION,
    CATEGORY_CHARACTER,
    CATEGORY_CORPORATION,
    CATEGORY_ALLIANCE,
)
from app.eve_images import image_url
from app.models import NameResolution, SovereigntyMapResponse
from app.cache import query_cache, single_flight
from app.esi import esi_client
from app.http_cache import json_cache_response

router = APIRouter()

_ALLIANCE, _CORP, _FACTION = 0, 1, 2
_ADM_FALLBACK = 3.0
_ADM_PER_SYSTEM_DEFAULT = 1.0


def _owner_of(record: dict) -> tuple[int, int] | None:
    """(owner_kind, owner_id) with precedence alliance -> corporation -> faction,
    or None if the record carries none of the three."""
    aid = record.get("alliance_id")
    if aid is not None:
        return (_ALLIANCE, aid)
    cid = record.get("corporation_id")
    if cid is not None:
        return (_CORP, cid)
    fid = record.get("faction_id")
    if fid is not None:
        return (_FACTION, fid)
    return None


def build_sovereignty_response(
    sov_map: dict[int, dict],
    adm_by_system: dict[int, float] | None,
    name_lookups: dict[tuple[int, int], tuple[str | None, str | None]],
    updated_at: int,
) -> SovereigntyMapResponse:
    adm_available = adm_by_system is not None

    owned = [
        (sid, owner)
        for sid, rec in sov_map.items()
        if (owner := _owner_of(rec)) is not None
    ]
    owned.sort(key=lambda t: t[0])

    owner_index: dict[tuple[int, int], int] = {}
    owner_kinds: list[int] = []
    owner_ids: list[int] = []
    owner_names: list[str | None] = []
    owner_tickers: list[str | None] = []
    system_ids: list[int] = []
    owner_idx: list[int] = []
    adm: list[float] = []

    for sid, key in owned:
        if key not in owner_index:
            owner_index[key] = len(owner_ids)
            name, ticker = name_lookups.get(key, (None, None))
            owner_kinds.append(key[0])
            owner_ids.append(key[1])
            owner_names.append(name)
            owner_tickers.append(ticker)
        system_ids.append(sid)
        owner_idx.append(owner_index[key])
        if adm_available:
            adm.append(float(adm_by_system.get(sid, _ADM_PER_SYSTEM_DEFAULT)))
        else:
            adm.append(_ADM_FALLBACK)

    return SovereigntyMapResponse(
        updated_at=updated_at,
        adm_available=adm_available,
        owner_kinds=owner_kinds,
        owner_ids=owner_ids,
        owner_names=owner_names,
        owner_tickers=owner_tickers,
        system_ids=system_ids,
        owner_idx=owner_idx,
        adm=adm,
    )


_ESI_FALLBACK_CONCURRENCY = 8


async def _resolve_owner_names(
    sov_map: dict[int, dict],
) -> dict[tuple[int, int], tuple[str | None, str | None]]:
    winners = [o for o in (_owner_of(r) for r in sov_map.values()) if o is not None]
    alliance_ids = {oid for (k, oid) in winners if k == _ALLIANCE}
    corp_ids = {oid for (k, oid) in winners if k == _CORP}
    faction_ids = {oid for (k, oid) in winners if k == _FACTION}

    _chars, corp_info, alliance_info, faction_names = await entities.fetch_entity_names(
        set(), corp_ids, alliance_ids, faction_ids
    )

    lookups: dict[tuple[int, int], tuple[str | None, str | None]] = {}
    for aid, (name, ticker) in alliance_info.items():
        lookups[(_ALLIANCE, aid)] = (name, ticker)
    for cid, (name, ticker) in corp_info.items():
        lookups[(_CORP, cid)] = (name, ticker)
    for fid, name in faction_names.items():
        lookups[(_FACTION, fid)] = (name, None)

    missing = [(_ALLIANCE, a) for a in alliance_ids if (_ALLIANCE, a) not in lookups]
    missing += [(_CORP, c) for c in corp_ids if (_CORP, c) not in lookups]
    if missing:
        sem = asyncio.Semaphore(_ESI_FALLBACK_CONCURRENCY)

        async def _one(kind: int, oid: int) -> None:
            async with sem:
                try:
                    if kind == _ALLIANCE:
                        name, ticker = await esi_client.get_alliance_info(oid)
                    else:
                        name, ticker = await esi_client.get_corporation_info(oid)
                    lookups[(kind, oid)] = (name, ticker)
                except Exception:
                    pass  # leave null; frontend labels by ticker/id

        await asyncio.gather(*[_one(k, oid) for (k, oid) in missing])

    return lookups


@router.post("/universe/names")
async def resolve_universe_names(
    ids: Annotated[list[int], Body()],
) -> dict[int, NameResolution]:
    """Resolve a mixed list of ids (types, characters, corporations, alliances,
    factions) to names/tickers/image URLs by routing each id to its reference
    table(s) via `classify_id`. Ids in the shared legacy id range are ambiguous
    and get probed against all three entity tables; unresolvable ids are omitted."""
    error = validate_id_list(ids, config.limits.max_name_ids)
    if error is not None:
        raise HTTPException(status_code=400, detail=error)

    type_ids: set[int] = set()
    faction_ids: set[int] = set()
    char_ids: set[int] = set()
    corp_ids: set[int] = set()
    alliance_ids: set[int] = set()
    for i in ids:
        cat = classify_id(i)
        if cat == CATEGORY_TYPE:
            type_ids.add(i)
        elif cat == CATEGORY_FACTION:
            faction_ids.add(i)
        elif cat == CATEGORY_CHARACTER:
            char_ids.add(i)
        elif cat == CATEGORY_CORPORATION:
            corp_ids.add(i)
        elif cat == CATEGORY_ALLIANCE:
            alliance_ids.add(i)
        else:  # ambiguous -> probe all three entity tables
            char_ids.add(i)
            corp_ids.add(i)
            alliance_ids.add(i)

    type_names, (char_names, corp_info, alliance_info, faction_names) = (
        await asyncio.gather(
            get_type_names(type_ids),
            entities.fetch_entity_names(
                char_ids, corp_ids, alliance_ids, faction_ids, emit_metrics=False
            ),
        )
    )

    out: dict[int, NameResolution] = {}
    for i, name in type_names.items():
        out[i] = NameResolution(
            category="type", name=name, image_url=image_url("type", i)
        )
    for i, name in char_names.items():
        out[i] = NameResolution(
            category="character", name=name, image_url=image_url("character", i)
        )
    for i, (name, ticker) in corp_info.items():
        out[i] = NameResolution(
            category="corporation",
            name=name,
            ticker=ticker,
            image_url=image_url("corporation", i),
        )
    for i, (name, ticker) in alliance_info.items():
        out[i] = NameResolution(
            category="alliance",
            name=name,
            ticker=ticker,
            image_url=image_url("alliance", i),
        )
    for i, name in faction_names.items():
        out[i] = NameResolution(
            category="faction", name=name, image_url=image_url("faction", i)
        )
    return out


@router.get("/universe/sov", response_model=None)
async def get_sovereignty_map(
    if_none_match: Annotated[str | None, Header(alias="If-None-Match")] = None,
):
    """Columnar snapshot of every claimed system's owner + ADM for the influence
    overlay. Cache-once via query_cache; 503 until the leader's first sov refresh."""
    cache_params: dict = {}
    res = await query_cache.get("sov_map", cache_params)
    if res is None:
        async with single_flight.lock("sov_map"):
            res = await query_cache.get("sov_map", cache_params)
            if res is None:
                sov_map = await esi_client.get_sov_map_cached()
                if sov_map is None:
                    raise HTTPException(
                        status_code=503, detail="Sovereignty data warming up"
                    )
                adm_by_system = await esi_client.get_sov_structures_cached()
                name_lookups = await _resolve_owner_names(sov_map)
                result = build_sovereignty_response(
                    sov_map, adm_by_system, name_lookups, int(time.time())
                )
                res = await query_cache.set(
                    "sov_map",
                    cache_params,
                    result.model_dump_json(),
                    ttl=config.cache.sov_map_ttl,
                )
    etag, gzipped, body = res
    return json_cache_response(
        body, gzipped, etag, config.cache.sov_map_ttl, if_none_match
    )
