import asyncio
from typing import Annotated

from fastapi import APIRouter, Body, HTTPException

from app.config import config
from app.security import validate_id_list
from app.queries import get_type_names
from app import entities
from app.id_ranges import (
    classify_id, CATEGORY_TYPE, CATEGORY_FACTION, CATEGORY_CHARACTER,
    CATEGORY_CORPORATION, CATEGORY_ALLIANCE,
)
from app.eve_images import image_url
from app.models import NameResolution

router = APIRouter()


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

    type_names, (char_names, corp_info, alliance_info, faction_names) = await asyncio.gather(
        get_type_names(type_ids),
        entities.fetch_entity_names(char_ids, corp_ids, alliance_ids, faction_ids),
    )

    out: dict[int, NameResolution] = {}
    for i, name in type_names.items():
        out[i] = NameResolution(category="type", name=name, image_url=image_url("type", i))
    for i, name in char_names.items():
        out[i] = NameResolution(category="character", name=name, image_url=image_url("character", i))
    for i, (name, ticker) in corp_info.items():
        out[i] = NameResolution(category="corporation", name=name, ticker=ticker, image_url=image_url("corporation", i))
    for i, (name, ticker) in alliance_info.items():
        out[i] = NameResolution(category="alliance", name=name, ticker=ticker, image_url=image_url("alliance", i))
    for i, name in faction_names.items():
        out[i] = NameResolution(category="faction", name=name, image_url=image_url("faction", i))
    return out
