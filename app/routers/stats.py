import asyncio
from datetime import date, datetime
from typing import Annotated

from fastapi import APIRouter, Query, Header, Depends, HTTPException

from app.config import config
from app.cache import get_or_build
from app.esi import SYSTEM_JUMPS, esi_client
from app.global_kills import fetch_global_kills, fetch_filtered_global_kills, MAP_RANGES
from app.http_cache import json_cache_response
from app.leaderboards import Role, Window, Scope, fetch_leaderboards
from app.models import RankSystemsResponse, SystemJumpsResponse
from app.queries import fetch_top_systems, fetch_system_kills, fetch_rollup_watermark
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
    async def build() -> str:
        top, computed_at = await asyncio.gather(
            fetch_top_systems(limit=limit), fetch_rollup_watermark()
        )
        return RankSystemsResponse(computed_at=computed_at, top=top).model_dump_json(
            exclude_none=True
        )

    return await get_or_build(
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
    etag, gzipped, body = await build_system_rankings(limit)
    return json_cache_response(
        body, gzipped, etag, config.cache.rankings_ttl, if_none_match, revalidate=True
    )


async def build_system_kills(
    start: str | None, end: str | None, flt: Filter | None
) -> tuple[str, bool, bytes]:
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
            return (await fetch_system_kills(s, e)).model_dump_json(exclude_none=True)

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

    return await get_or_build(prefix, params, lock, ttl, build)


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
    etag, gzipped, body = await build_system_kills(start, end, flt)
    ttl = config.cache.rankings_ttl if flt.is_empty else config.cache.filtered_map_ttl
    return json_cache_response(body, gzipped, etag, ttl, if_none_match, revalidate=True)


async def build_system_jumps() -> tuple[str, bool, bytes]:
    async def build() -> str:
        jumps = await esi_client.get_cached(SYSTEM_JUMPS)
        if jumps is None:
            raise HTTPException(status_code=503, detail="jump data warming up")
        ordered = sorted(jumps.items())
        return SystemJumpsResponse(
            system_ids=[sid for sid, _ in ordered],
            jumps=[n for _, n in ordered],
        ).model_dump_json()

    return await get_or_build(
        "system_jumps", {}, "system_jumps", config.cache.system_jumps_ttl, build
    )


@router.get("/stats/system-jumps", response_model=None)
async def get_system_jumps_stats(
    if_none_match: Annotated[str | None, Header(alias="If-None-Match")] = None,
):
    etag, gzipped, body = await build_system_jumps()
    return json_cache_response(
        body, gzipped, etag, config.cache.system_jumps_max_age, if_none_match
    )


async def build_global_kills(
    map: str, bins: int, flt: Filter | None
) -> tuple[str, bool, bytes]:
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

    return await get_or_build(prefix, params, lock, ttl, build)


@router.get("/stats/global-kills", response_model=None)
async def get_global_kills(
    map: Annotated[str, Query()],
    flt: Filter = Depends(get_filter),
    bins: Annotated[int | None, Query(ge=1, le=2000)] = None,
    if_none_match: Annotated[str | None, Header(alias="If-None-Match")] = None,
):
    if map not in MAP_RANGES:
        raise HTTPException(status_code=400, detail="unknown map type")
    n = bins if bins is not None else config.limits.global_kills_default_bins
    etag, gzipped, body = await build_global_kills(map, n, flt)
    ttl = config.cache.rankings_ttl if flt.is_empty else config.cache.filtered_map_ttl
    return json_cache_response(body, gzipped, etag, ttl, if_none_match, revalidate=True)


async def build_leaderboards(
    window: str, role: str, scope: str, limit: int
) -> tuple[str, bool, bytes]:
    async def build() -> str:
        return (await fetch_leaderboards(window, role, scope, limit)).model_dump_json(
            exclude_none=True
        )

    return await get_or_build(
        "leaderboards",
        {"window": window, "role": role, "scope": scope, "limit": limit},
        f"leaderboards:{window}:{role}:{scope}:{limit}",
        config.cache.rankings_ttl,
        build,
    )


@router.get("/stats/leaderboards", response_model=None)
async def get_leaderboards(
    window: Annotated[Window, Query(description="all, 1d, 7d, 30d, 6m or 1y")],
    role: Annotated[
        Role, Query(description="victim (kills lost) or attacker (kills made)")
    ],
    scope: Annotated[
        Scope, Query(description="all, or players (NPC entities removed)")
    ],
    limit: Annotated[
        int, Query(ge=1, le=50, description="Entries per board")
    ] = config.limits.leaderboards_default_limit,
    if_none_match: Annotated[str | None, Header(alias="If-None-Match")] = None,
):
    etag, gzipped, body = await build_leaderboards(window, role, scope, limit)
    return json_cache_response(
        body, gzipped, etag, config.cache.rankings_ttl, if_none_match, revalidate=True
    )
