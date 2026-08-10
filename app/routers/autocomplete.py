from enum import Enum
from typing import Annotated

from fastapi import APIRouter, Query, Response

from app.config import config
from app.autocomplete import (
    autocomplete_entities,
    autocomplete_types,
    autocomplete_weapons,
)
from app.models import EntityCandidate, TypeCandidate
from app import prometheus_metrics as pm

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
    response.headers["Cache-Control"] = (
        f"public, max-age={config.cache.autocomplete_ttl}"
    )
    kind_label = getattr(kind, "value", kind)
    if len(q.strip()) < config.limits.autocomplete_min_length:
        pm.autocomplete_requests.labels(kind=kind_label, outcome="short_circuit").inc()
        return []
    pm.autocomplete_requests.labels(kind=kind_label, outcome="served").inc()
    # EntityKind subclasses str, so plain strings (as used by tests that call
    # this function directly, bypassing FastAPI's enum coercion) work here too.
    return await autocomplete_entities(kind, q.strip(), limit)


@router.get("/autocomplete/types", response_model=list[TypeCandidate])
async def autocomplete_types_endpoint(
    q: Annotated[str, Query(min_length=0)],
    response: Response,
    limit: Annotated[int, Query(ge=1, le=50)] = 20,
):
    response.headers["Cache-Control"] = (
        f"public, max-age={config.cache.autocomplete_ttl}"
    )
    if len(q.strip()) < config.limits.autocomplete_min_length:
        pm.autocomplete_requests.labels(kind="type", outcome="short_circuit").inc()
        return []
    pm.autocomplete_requests.labels(kind="type", outcome="served").inc()
    return await autocomplete_types(q.strip(), limit)


@router.get("/autocomplete/weapons", response_model=list[TypeCandidate])
async def autocomplete_weapons_endpoint(
    q: Annotated[str, Query(min_length=0)],
    response: Response,
    limit: Annotated[int, Query(ge=1, le=50)] = 20,
):
    response.headers["Cache-Control"] = (
        f"public, max-age={config.cache.autocomplete_ttl}"
    )
    if len(q.strip()) < config.limits.autocomplete_min_length:
        pm.autocomplete_requests.labels(kind="weapon", outcome="short_circuit").inc()
        return []
    pm.autocomplete_requests.labels(kind="weapon", outcome="served").inc()
    return await autocomplete_weapons(q.strip(), limit)
