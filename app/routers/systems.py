import asyncio
import logging
import time
from typing import Annotated, cast

from fastapi import APIRouter, Query, Header, HTTPException
from fastapi.responses import Response

from app.config import config
from app.cache import (
    query_cache,
    kills_binary_cache,
    single_flight,
    should_short_circuit,
    get_system_latest,
)
from app.http_cache import json_cache_response, binary_cache_response
from app.binary_encoder import encode_kills_binary
from app.esi import esi_client
from app import prometheus_metrics
from app.queries import fetch_raw_kills, fetch_farthest_kill, normalize_farthest_kill
from app.models import SovResponse, GroupInfo, FarthestKillResponse

logger = logging.getLogger(__name__)
router = APIRouter()


@router.get("/systems/{solar_system_id}/kills", response_model=None)
async def get_system_kills(
    solar_system_id: int,
    since: Annotated[
        int | None,
        Query(
            description="Only return kills inserted after this Unix epoch timestamp (seconds)"
        ),
    ] = None,
):
    """Get binary-encoded kills in a solar system.

    Full payload (since=None) is cached once per TTL, single-flighted, and served
    pre-compressed with a build-time X-Kills-Fresh-To boundary. Poll and
    short-circuit responses are live and no-store.
    """
    if since is not None:
        latest = await get_system_latest(solar_system_id)
        fresh_to = latest if latest is not None else int(time.time())
        if should_short_circuit(since, latest):
            prometheus_metrics.since_short_circuits.inc()
            payload = encode_kills_binary([], [], [], [], [], [])
        else:
            result = await fetch_raw_kills(solar_system_id=solar_system_id, since=since)
            payload = encode_kills_binary(
                killmail_ids=result["killmail_ids"],
                killmail_times=result["killmail_times"],
                x=[int(v) for v in result["x"]],
                y=[int(v) for v in result["y"]],
                z=[int(v) for v in result["z"]],
                ship_types=result["ship_types"],
            )
        prometheus_metrics.kills_binary_response_bytes.observe(len(payload))
        return Response(
            content=payload,
            media_type="application/octet-stream",
            headers={"Cache-Control": "no-store", "X-Kills-Fresh-To": str(fresh_to)},
        )

    cache_params = {"solar_system_id": solar_system_id}
    res = await kills_binary_cache.get(cache_params)
    if res is None:
        async with single_flight.lock(f"kills_binary:{solar_system_id}"):
            res = await kills_binary_cache.get(cache_params)
            if res is None:
                # Capture the freshness boundary BEFORE fetching so fresh_to never
                # exceeds the payload's completeness edge (design §3).
                latest = await get_system_latest(solar_system_id)
                fresh_to = latest if latest is not None else int(time.time())
                result = await fetch_raw_kills(solar_system_id=solar_system_id, since=None)
                encoded = encode_kills_binary(
                    killmail_ids=result["killmail_ids"],
                    killmail_times=result["killmail_times"],
                    x=[int(v) for v in result["x"]],
                    y=[int(v) for v in result["y"]],
                    z=[int(v) for v in result["z"]],
                    ship_types=result["ship_types"],
                )
                res = await kills_binary_cache.set(cache_params, encoded, fresh_to)
    fresh_to, gzipped, body = res
    prometheus_metrics.kills_binary_response_bytes.observe(len(body))
    return binary_cache_response(
        body, gzipped=gzipped, max_age=config.cache.binary_ttl, fresh_to=fresh_to
    )


@router.get("/systems/{solar_system_id}/sov", response_model=None)
async def get_system_sov(
    solar_system_id: int,
    if_none_match: Annotated[str | None, Header(alias="If-None-Match")] = None,
):
    """Get the current sovereignty of a solar system."""
    cache_params = {"solar_system_id": solar_system_id}
    res = await query_cache.get("sov", cache_params)
    if res is None:
        async with single_flight.lock(f"sov:{solar_system_id}"):
            res = await query_cache.get("sov", cache_params)
            if res is None:
                sov_map = await esi_client.get_sov_map_cached()
                if sov_map is None:
                    raise HTTPException(
                        status_code=503, detail="Sovereignty data warming up"
                    )
                system = sov_map.get(solar_system_id)
                if system is None:
                    result = SovResponse(claimed=False)
                else:
                    alliance_id: int | None = system.get("alliance_id")
                    corporation_id: int | None = system.get("corporation_id")
                    if alliance_id is None and corporation_id is None:
                        result = SovResponse(claimed=False)
                    else:
                        try:
                            alliance_info, corporation_info = await asyncio.gather(
                                (
                                    esi_client.get_alliance_info(alliance_id)
                                    if alliance_id
                                    else asyncio.sleep(0, result=None)
                                ),
                                (
                                    esi_client.get_corporation_info(corporation_id)
                                    if corporation_id
                                    else asyncio.sleep(0, result=None)
                                ),
                            )
                        except RuntimeError:
                            logger.exception("ESI upstream call failed")
                            raise HTTPException(
                                status_code=502, detail="Upstream service unavailable"
                            )
                        result = SovResponse(
                            claimed=True,
                            # alliance_id/corporation_id are int when the corresponding info tuple is truthy;
                            # cast() is a zero-cost type hint, no runtime effect.
                            alliance=(
                                GroupInfo(
                                    id=cast(int, alliance_id),
                                    name=alliance_info[0],
                                    ticker=alliance_info[1],
                                )
                                if alliance_info
                                else None
                            ),
                            corporation=(
                                GroupInfo(
                                    id=cast(int, corporation_id),
                                    name=corporation_info[0],
                                    ticker=corporation_info[1],
                                )
                                if corporation_info
                                else None
                            ),
                        )
                res = await query_cache.set(
                    "sov",
                    cache_params,
                    result.model_dump_json(exclude_none=True),
                    ttl=config.cache.sov_ttl,
                )
    etag, gzipped, body = res
    return json_cache_response(
        body, gzipped, etag, config.cache.sov_ttl, if_none_match
    )


@router.get("/systems/{solar_system_id}/farthest_kill", response_model=None)
async def get_farthest_kill(
    solar_system_id: int,
    if_none_match: Annotated[str | None, Header(alias="If-None-Match")] = None,
):
    """Get the distance from (0, 0, 0) to the farthest kill in the solar system.
    Returns -1 if the solar system has no kills."""
    cache_params = {"solar_system_id": solar_system_id}
    res = await query_cache.get("farthest_kill", cache_params)
    if res is None:
        async with single_flight.lock(f"farthest_kill:{solar_system_id}"):
            res = await query_cache.get("farthest_kill", cache_params)
            if res is None:
                value = normalize_farthest_kill(
                    await fetch_farthest_kill(solar_system_id)
                )
                result = FarthestKillResponse(farthest_kill=value)
                res = await query_cache.set(
                    "farthest_kill",
                    cache_params,
                    result.model_dump_json(),
                    ttl=config.cache.farthest_kill_ttl,
                )
    etag, gzipped, body = res
    return json_cache_response(
        body, gzipped, etag, config.cache.farthest_kill_ttl, if_none_match
    )
