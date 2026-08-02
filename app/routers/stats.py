from typing import Annotated

from fastapi import APIRouter, Query, Header

from app.config import config
from app.cache import query_cache, single_flight
from app.http_cache import json_cache_response
from app.models import RankSystemsResponse
from app.queries import fetch_top_systems, fetch_bottom_systems, fetch_system_kills

router = APIRouter()


@router.get("/stats/system-rankings", response_model=None)
async def get_system_rankings(
    limit: Annotated[
        int, Query(ge=1, le=50, description="Number of systems to return")
    ] = 10,
    if_none_match: Annotated[str | None, Header(alias="If-None-Match")] = None,
):
    """Get rank list of solar systems by highest/lowest number of kills."""
    cache_params = {"limit": limit}
    res = await query_cache.get("system_rankings", cache_params)
    if res is None:
        async with single_flight.lock(f"system_rankings:{limit}"):
            res = await query_cache.get("system_rankings", cache_params)
            if res is None:
                top = await fetch_top_systems(limit=limit)
                bottom = await fetch_bottom_systems(limit=limit)
                result = RankSystemsResponse(top=top, bottom=bottom)
                res = await query_cache.set(
                    "system_rankings",
                    cache_params,
                    result.model_dump_json(),
                    ttl=config.cache.rankings_ttl,
                )
    etag, gzipped, body = res
    return json_cache_response(
        body, gzipped, etag, config.cache.rankings_ttl, if_none_match
    )


@router.get("/stats/system-kills", response_model=None)
async def get_system_kills_stats(
    if_none_match: Annotated[str | None, Header(alias="If-None-Match")] = None,
):
    """Per-system kill counts across time windows as index-aligned columns:
    index i of every array (all/day/week/month/six_months/year) belongs to
    system_ids[i]. Cached like /stats/system-rankings (same TTL)."""
    cache_params: dict = {}
    res = await query_cache.get("system_kills", cache_params)
    if res is None:
        async with single_flight.lock("system_kills"):
            res = await query_cache.get("system_kills", cache_params)
            if res is None:
                result = await fetch_system_kills()
                res = await query_cache.set(
                    "system_kills",
                    cache_params,
                    result.model_dump_json(),
                    ttl=config.cache.rankings_ttl,
                )
    etag, gzipped, body = res
    return json_cache_response(
        body, gzipped, etag, config.cache.rankings_ttl, if_none_match
    )
