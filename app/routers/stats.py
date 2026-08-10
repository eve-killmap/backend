from typing import Annotated

from fastapi import APIRouter, Query, Header, Depends

from app.config import config
from app.cache import query_cache, single_flight
from app.http_cache import json_cache_response
from app.models import RankSystemsResponse
from app.queries import fetch_top_systems, fetch_bottom_systems, fetch_system_kills
from app.routers.dependencies import get_filter
from app.filters import Filter
from app.facet_queries import fetch_filtered_map

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
    flt: Filter = Depends(get_filter),
    if_none_match: Annotated[str | None, Header(alias="If-None-Match")] = None,
):
    """Per-system kill counts across time windows as index-aligned columns:
    index i of every array (all/day/week/month/six_months/year) belongs to
    system_ids[i]. Unfiltered requests serve from the pre-computed MVs
    (cached like /stats/system-rankings, same TTL). Filtered requests
    (``f=`` params) compute from ``kill_facets`` and cache under a separate
    prefix with their own TTL."""
    if flt.is_empty:
        prefix, ttl, lock, params = (
            "system_kills",
            config.cache.rankings_ttl,
            "system_kills",
            {},
        )
        builder = fetch_system_kills
    else:
        key = flt.canonical()
        prefix, ttl, lock, params = (
            "system_kills_filtered",
            config.cache.filtered_map_ttl,
            f"system_kills_filtered:{key}",
            {"filter": key},
        )

        async def builder():
            return await fetch_filtered_map(flt)

    res = await query_cache.get(prefix, params)
    if res is None:
        async with single_flight.lock(lock):
            res = await query_cache.get(prefix, params)
            if res is None:
                result = await builder()
                res = await query_cache.set(
                    prefix, params, result.model_dump_json(), ttl=ttl
                )
    etag, gzipped, body = res
    return json_cache_response(body, gzipped, etag, ttl, if_none_match)
