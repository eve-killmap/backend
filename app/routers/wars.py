from typing import Annotated

from fastapi import APIRouter, Body, HTTPException, Query, Response

from app.config import config
from app.entities import get_war_details, search_wars, war_summary_from_row
from app.models import WarSummary
from app.security import validate_id_list

router = APIRouter()

_KINDS = {"alliance", "corporation"}


def _parse_side(value: str | None) -> tuple[str, int] | None:
    if value is None:
        return None
    try:
        kind, raw_id = value.split(":", 1)
        entity_id = int(raw_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="side must be '<kind>:<id>'")
    if kind not in _KINDS or entity_id <= 0:
        raise HTTPException(status_code=400, detail="kind must be alliance|corporation")
    return kind, entity_id


@router.get("/wars/search", response_model=list[WarSummary])
async def war_search(
    response: Response,
    aggressor: Annotated[str | None, Query()] = None,
    defender: Annotated[str | None, Query()] = None,
):
    agg = _parse_side(aggressor)
    dfn = _parse_side(defender)
    if agg is None and dfn is None:
        raise HTTPException(
            status_code=400, detail="at least one of aggressor/defender required"
        )
    response.headers["Cache-Control"] = f"public, max-age={config.cache.war_search_ttl}"
    rows = await search_wars(agg, dfn, config.limits.max_war_results)
    return [war_summary_from_row(r) for r in rows]


@router.post("/wars/details", response_model=list[WarSummary])
async def war_details(ids: Annotated[list[int], Body()]):
    error = validate_id_list(ids, config.limits.max_war_ids)
    if error is not None:
        raise HTTPException(status_code=400, detail=error)
    return await get_war_details(ids)
