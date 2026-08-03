from typing import Annotated

from fastapi import APIRouter, HTTPException, Query, Response

from app.config import config
from app.entities import search_wars
from app.models import WarSummary

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


def _epoch(value) -> int | None:
    return int(value.timestamp()) if value is not None else None


@router.get("/wars/search", response_model=list[WarSummary])
async def war_search(
    response: Response,
    aggressor: Annotated[str | None, Query()] = None,
    defender: Annotated[str | None, Query()] = None,
):
    agg = _parse_side(aggressor)
    dfn = _parse_side(defender)
    if agg is None and dfn is None:
        raise HTTPException(status_code=400, detail="at least one of aggressor/defender required")
    response.headers["Cache-Control"] = f"public, max-age={config.cache.war_search_ttl}"
    rows = await search_wars(agg, dfn, config.limits.max_war_results)
    return [
        WarSummary(
            war_id=r["war_id"],
            declared=_epoch(r["declared"]),
            started=_epoch(r["started"]),
            finished=_epoch(r["finished"]),
            retracted=_epoch(r["retracted"]),
            mutual=bool(r["mutual"]),
            aggressor_corporation_id=r["aggressor_corporation_id"],
            aggressor_alliance_id=r["aggressor_alliance_id"],
            defender_corporation_id=r["defender_corporation_id"],
            defender_alliance_id=r["defender_alliance_id"],
            ally_corporation_ids=list(r["ally_corporation_ids"] or []),
            ally_alliance_ids=list(r["ally_alliance_ids"] or []),
        )
        for r in rows
    ]
