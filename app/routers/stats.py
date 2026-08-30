import json
from datetime import date, datetime
from typing import Annotated

from fastapi import APIRouter, Query, Header, Depends, HTTPException

from app.config import config
from app.cache import query_cache, single_flight
from app.global_kills import fetch_global_kills, fetch_filtered_global_kills, MAP_RANGES
from app.http_cache import json_cache_response
from app.models import RankSystemsResponse
from app.queries import fetch_top_systems, fetch_bottom_systems, fetch_system_kills
from app.routers.dependencies import get_filter
from app.filters import Filter
from app.facet_queries import fetch_filtered_map

router = APIRouter()


def _parse_day(s: str | None) -> date | None:
    if s is None:
        return None
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except ValueError:
        raise HTTPException(status_code=400, detail="dates must be YYYY-MM-DD")


async def build_system_rankings(limit: int) -> tuple[str, bool, bytes]:
    """Get-or-build-and-cache the system-rankings response for ``limit``.

    Shared by the endpoint and the leader's cache-warm cycle; both must resolve
    to the exact same cache key, so this must stay the sole owner of the
    ``system_rankings`` prefix + params shape.
    """
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
    return res


@router.get("/stats/system-rankings", response_model=None)
async def get_system_rankings(
    limit: Annotated[
        int, Query(ge=1, le=50, description="Number of systems to return")
    ] = config.limits.system_rankings_default_limit,
    if_none_match: Annotated[str | None, Header(alias="If-None-Match")] = None,
):
    """Get rank list of solar systems by highest/lowest number of kills."""
    etag, gzipped, body = await build_system_rankings(limit)
    return json_cache_response(
        body, gzipped, etag, config.cache.rankings_ttl, if_none_match
    )


async def build_system_kills(
    start: str | None, end: str | None, flt: Filter | None
) -> tuple[str, bool, bytes]:
    """Get-or-build-and-cache the system-kills response for ``start``/``end``/``flt``.

    Shared by the endpoint and the leader's cache-warm cycle; both must resolve
    to the exact same cache key, so this must stay the sole owner of the
    ``system_kills``/``system_kills_filtered`` prefixes + params shapes.
    ``flt`` may be ``None`` (treated as unfiltered) so the warm cycle can call
    this with no ``Filter`` instance in hand.
    """
    s, e = _parse_day(start), _parse_day(end)
    if s is not None and e is not None and e <= s:
        raise HTTPException(status_code=400, detail="end must be after start")

    if flt is None or flt.is_empty:
        prefix, ttl, lock, params = (
            "system_kills",
            config.cache.rankings_ttl,
            "system_kills",
            {"start": start, "end": end},
        )

        async def builder():
            return await fetch_system_kills(s, e)

    else:
        key = flt.canonical()
        prefix, ttl, lock, params = (
            "system_kills_filtered",
            config.cache.filtered_map_ttl,
            f"system_kills_filtered:{key}",
            {"filter": key, "start": start, "end": end},
        )

        async def builder():
            return await fetch_filtered_map(flt, s, e)

    res = await query_cache.get(prefix, params)
    if res is None:
        async with single_flight.lock(lock):
            res = await query_cache.get(prefix, params)
            if res is None:
                result = await builder()
                res = await query_cache.set(
                    prefix, params, result.model_dump_json(), ttl=ttl
                )
    return res


@router.get("/stats/system-kills", response_model=None)
async def get_system_kills_stats(
    flt: Filter = Depends(get_filter),
    start: Annotated[
        str | None, Query(description="UTC day, YYYY-MM-DD; inclusive")
    ] = None,
    end: Annotated[
        str | None, Query(description="UTC day, YYYY-MM-DD; exclusive")
    ] = None,
    if_none_match: Annotated[str | None, Header(alias="If-None-Match")] = None,
):
    """Per-system kill counts as index-aligned columns: counts[i] belongs to
    system_ids[i]. All-time by default; ``start``/``end`` restrict to a
    day-aligned, half-open UTC window ``[start, end)`` (either independently
    optional). Unfiltered requests serve from the pre-computed MVs (cached
    like /stats/system-rankings, same TTL). Filtered requests (``f=`` params)
    compute from ``kill_facets`` and cache under a separate prefix with their
    own TTL."""
    etag, gzipped, body = await build_system_kills(start, end, flt)
    ttl = config.cache.rankings_ttl if flt.is_empty else config.cache.filtered_map_ttl
    return json_cache_response(body, gzipped, etag, ttl, if_none_match)


async def build_global_kills(
    map: str, bins: int, flt: Filter | None
) -> tuple[str, bool, bytes]:
    """Get-or-build-and-cache the global-kills histogram for ``map``/``bins``.

    Empty/absent ``flt`` serves the warmed ``global_kills`` rollup path (sole
    owner of that prefix + params shape, shared with the leader's warm cycle).
    A non-empty ``flt`` computes a live ``kill_facets`` histogram cached under
    ``global_kills_filtered`` with its own TTL, mirroring ``build_system_kills``.
    Caller must have already validated ``map`` against ``MAP_RANGES``.
    """
    if flt is None or flt.is_empty:
        prefix, ttl, lock, params = (
            "global_kills",
            config.cache.rankings_ttl,
            f"global_kills:{map}:{bins}",
            {"bins": bins, "map": map},
        )

        async def builder():
            return await fetch_global_kills(map, bins)

    else:
        key = flt.canonical()
        prefix, ttl, lock, params = (
            "global_kills_filtered",
            config.cache.filtered_map_ttl,
            f"global_kills_filtered:{key}:{map}:{bins}",
            {"filter": key, "map": map, "bins": bins},
        )

        async def builder():
            return await fetch_filtered_global_kills(flt, map, bins)

    res = await query_cache.get(prefix, params)
    if res is None:
        async with single_flight.lock(lock):
            res = await query_cache.get(prefix, params)
            if res is None:
                counts = await builder()
                res = await query_cache.set(
                    prefix, params, json.dumps(counts), ttl=ttl
                )
    return res


@router.get("/stats/global-kills", response_model=None)
async def get_global_kills(
    map: Annotated[str, Query()],
    flt: Filter = Depends(get_filter),
    bins: Annotated[int | None, Query(ge=1, le=2000)] = None,
    if_none_match: Annotated[str | None, Header(alias="If-None-Match")] = None,
):
    """Per-map kill-count histogram over the fixed global time axis
    (EARLIEST_KILL_DATE..CURRENT_DATE), bucketed into ``bins`` equal-width bins
    (default ``config.limits.global_kills_default_bins``). Returns a bare,
    zero-filled, dense array of ``bins`` ints, oldest to newest. Without ``f=``
    this serves the warmed rollup; with ``f=`` facet filters it counts only
    matching kills (same axis) from ``kill_facets``, cached separately."""
    if map not in MAP_RANGES:
        raise HTTPException(status_code=400, detail="unknown map type")
    n = bins if bins is not None else config.limits.global_kills_default_bins
    etag, gzipped, body = await build_global_kills(map, n, flt)
    ttl = config.cache.rankings_ttl if flt.is_empty else config.cache.filtered_map_ttl
    return json_cache_response(body, gzipped, etag, ttl, if_none_match)
