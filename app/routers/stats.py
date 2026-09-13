import asyncio
from datetime import date, datetime
from typing import Annotated, Awaitable, Callable

from fastapi import APIRouter, Query, Header, Depends, HTTPException

from app.config import config
from app.cache import query_cache, single_flight
from app.esi import SYSTEM_JUMPS, esi_client
from app.global_kills import fetch_global_kills, fetch_filtered_global_kills, MAP_RANGES
from app.http_cache import json_cache_response
from app.models import RankSystemsResponse, SystemJumpsResponse
from app.queries import (
    fetch_top_systems,
    fetch_bottom_systems,
    fetch_system_kills,
    fetch_rollup_watermark,
)
from app.routers.dependencies import get_filter
from app.filters import Filter
from app.facet_queries import fetch_filtered_map

router = APIRouter()


async def _get_or_build(
    prefix: str,
    params: dict,
    lock: str,
    ttl: int,
    build: Callable[[], Awaitable[str]],
) -> tuple[str, bool, bytes]:
    """Serve ``prefix``/``params`` from query_cache, else run ``build`` once under
    a per-key single-flight lock and cache its JSON body for ``ttl``. A build
    that raises caches nothing and releases the lock for the next caller."""
    res = await query_cache.get(prefix, params)
    if res is None:
        async with single_flight.lock(lock):
            res = await query_cache.get(prefix, params)
            if res is None:
                res = await query_cache.set(prefix, params, await build(), ttl=ttl)
    return res


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

    async def build() -> str:
        top, bottom, computed_at = await asyncio.gather(
            fetch_top_systems(limit=limit),
            fetch_bottom_systems(limit=limit),
            fetch_rollup_watermark(),
        )
        return RankSystemsResponse(
            computed_at=computed_at, top=top, bottom=bottom
        ).model_dump_json(exclude_none=True)

    return await _get_or_build(
        "system_rankings",
        {"limit": limit},
        f"system_rankings:{limit}",
        config.cache.rankings_ttl,
        build,
    )


@router.get("/stats/system-rankings", response_model=None)
async def get_system_rankings(
    limit: Annotated[
        int, Query(ge=1, le=50, description="Number of systems to return")
    ] = config.limits.system_rankings_default_limit,
    if_none_match: Annotated[str | None, Header(alias="If-None-Match")] = None,
):
    """Get rank list of solar systems by highest/lowest number of kills.
    `computed_at` is the epoch of process-kills' rollup watermark (omitted before
    the first rollup)."""
    etag, gzipped, body = await build_system_rankings(limit)
    return json_cache_response(
        body, gzipped, etag, config.cache.rankings_ttl, if_none_match, revalidate=True
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

        async def build() -> str:
            return (await fetch_system_kills(s, e)).model_dump_json(
                exclude_none=True
            )

    else:
        key = flt.canonical()
        prefix, ttl, lock, params = (
            "system_kills_filtered",
            config.cache.filtered_map_ttl,
            f"system_kills_filtered:{key}",
            {"filter": key, "start": start, "end": end},
        )

        async def build() -> str:
            return (await fetch_filtered_map(flt, s, e)).model_dump_json(
                exclude_none=True
            )

    return await _get_or_build(prefix, params, lock, ttl, build)


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
    """Per-system kill counts as index-aligned columns: kills[i] belongs to
    system_ids[i]. All-time by default; ``start``/``end`` restrict to a
    day-aligned, half-open UTC window ``[start, end)`` (either independently
    optional). Unfiltered requests serve from the pre-computed MVs (cached
    like /stats/system-rankings, same TTL). Filtered requests (``f=`` params)
    compute from ``kill_facets`` and cache under a separate prefix with their
    own TTL. `computed_at` is the rollup watermark for unfiltered requests and
    the build time for filtered ones."""
    etag, gzipped, body = await build_system_kills(start, end, flt)
    ttl = config.cache.rankings_ttl if flt.is_empty else config.cache.filtered_map_ttl
    return json_cache_response(body, gzipped, etag, ttl, if_none_match, revalidate=True)


async def build_system_jumps() -> tuple[str, bool, bytes]:
    """Get-or-build-and-cache the global jumps response.

    Sole owner of the ``system_jumps`` prefix + params shape."""

    async def build() -> str:
        jumps = await esi_client.get_cached(SYSTEM_JUMPS)
        if jumps is None:
            raise HTTPException(status_code=503, detail="jump data warming up")
        ordered = sorted(jumps.items())
        return SystemJumpsResponse(
            system_ids=[sid for sid, _ in ordered],
            jumps=[n for _, n in ordered],
        ).model_dump_json()

    return await _get_or_build(
        "system_jumps", {}, "system_jumps", config.cache.system_jumps_ttl, build
    )


@router.get("/stats/system-jumps", response_model=None)
async def get_system_jumps_stats(
    if_none_match: Annotated[str | None, Header(alias="If-None-Match")] = None,
):
    """Ship jumps per system over the past hour, as index-aligned columns:
    jumps[i] belongs to system_ids[i]. Only systems ESI reported are included;
    treat a missing system as 0."""
    etag, gzipped, body = await build_system_jumps()
    return json_cache_response(
        body, gzipped, etag, config.cache.system_jumps_max_age, if_none_match
    )


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

        async def build() -> str:
            return (await fetch_global_kills(map, bins)).model_dump_json(
                exclude_none=True
            )

    else:
        key = flt.canonical()
        prefix, ttl, lock, params = (
            "global_kills_filtered",
            config.cache.filtered_map_ttl,
            f"global_kills_filtered:{key}:{map}:{bins}",
            {"filter": key, "map": map, "bins": bins},
        )

        async def build() -> str:
            return (await fetch_filtered_global_kills(flt, map, bins)).model_dump_json(
                exclude_none=True
            )

    return await _get_or_build(prefix, params, lock, ttl, build)


@router.get("/stats/global-kills", response_model=None)
async def get_global_kills(
    map: Annotated[str, Query()],
    flt: Filter = Depends(get_filter),
    bins: Annotated[int | None, Query(ge=1, le=2000)] = None,
    if_none_match: Annotated[str | None, Header(alias="If-None-Match")] = None,
):
    """Per-map kill-count histogram over the fixed global time axis
    (EARLIEST_KILL_DATE..CURRENT_DATE), bucketed into ``bins`` equal-width bins
    (default ``config.limits.global_kills_default_bins``). Returns
    ``{"computed_at": N, "counts": [...]}`` where ``counts`` is a zero-filled,
    dense array of ``bins`` ints, oldest to newest, and ``computed_at`` is the
    rollup watermark (unfiltered) or the build time (filtered). Without ``f=``
    this serves the warmed rollup; with ``f=`` facet filters it counts only
    matching kills (same axis) from ``kill_facets``, cached separately."""
    if map not in MAP_RANGES:
        raise HTTPException(status_code=400, detail="unknown map type")
    n = bins if bins is not None else config.limits.global_kills_default_bins
    etag, gzipped, body = await build_global_kills(map, n, flt)
    ttl = config.cache.rankings_ttl if flt.is_empty else config.cache.filtered_map_ttl
    return json_cache_response(body, gzipped, etag, ttl, if_none_match, revalidate=True)
