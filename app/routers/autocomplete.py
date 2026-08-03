from enum import Enum
from typing import Annotated

from fastapi import APIRouter, Query, Response

from app.config import config
from app.autocomplete import autocomplete_entities, autocomplete_types
from app.models import EntityCandidate, TypeCandidate

router = APIRouter()


class EntityKind(str, Enum):
    character = "character"
    corporation = "corporation"
    alliance = "alliance"
    faction = "faction"


@router.get("/autocomplete/entities", response_model=list[EntityCandidate])
async def autocomplete_entities_endpoint(
    kind: EntityKind,
    q: Annotated[str, Query(min_length=0)],
    response: Response,
    limit: Annotated[int, Query(ge=1, le=50)] = 20,
):
    response.headers["Cache-Control"] = f"public, max-age={config.cache.autocomplete_ttl}"
    if len(q.strip()) < config.limits.autocomplete_min_length:
        return []
    # EntityKind subclasses str, so plain strings (as used by tests that call
    # this function directly, bypassing FastAPI's enum coercion) work here too.
    return await autocomplete_entities(kind, q.strip(), limit)


@router.get("/autocomplete/types", response_model=list[TypeCandidate])
async def autocomplete_types_endpoint(
    q: Annotated[str, Query(min_length=0)],
    response: Response,
    limit: Annotated[int, Query(ge=1, le=50)] = 20,
):
    response.headers["Cache-Control"] = f"public, max-age={config.cache.autocomplete_ttl}"
    if len(q.strip()) < config.limits.autocomplete_min_length:
        return []
    return await autocomplete_types(q.strip(), limit)
