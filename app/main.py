import asyncio
import json
import logging
from contextlib import asynccontextmanager
from typing import Annotated, cast

import redis.asyncio as aioredis
from fastapi import FastAPI, Query, HTTPException, Body, WebSocket, Header
from fastapi.responses import Response
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.gzip import GZipMiddleware

from app.config import config, setup_logging
from app.metrics import metrics
from app.security import validate_id_list, security_headers, origin_allowed, at_capacity
from app.database import db
import app.health as health
from app.cache import (
    query_cache,
    kills_binary_cache,
    single_flight,
    should_short_circuit,
    get_system_latest,
)
from app.http_cache import json_cache_response, binary_cache_response
from app.binary_encoder import encode_kills_binary
from app.esi import esi_client, EsiNotFoundError, EsiRateLimitError
from app.redis_client import broadcaster
from app import prometheus_metrics
import app.queries as queries
import app.invalidation as invalidation
from app.timeparse import iso_to_epoch
from app.models import (
    RawKillDetailResponse,
    ProcessedKillDetailResponse,
    RankSystemsResponse,
    FarthestKillResponse,
    VictimProcessed,
    AttackerProcessed,
    WarProcessed,
    WarParticipant,
    SovResponse,
    GroupInfo,
    HealthDetailResponse,
    WorkersSummary,
)
from app.queries import (
    fetch_raw_kills,
    fetch_top_systems,
    fetch_bottom_systems,
    fetch_farthest_kill,
    fetch_kills_by_ids,
    get_type_names,
    normalize_farthest_kill,
    fetch_db_stats,
    fetch_domain_stats,
    fetch_top_statements,
    get_kill_details_cached,
)

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(_app: FastAPI):
    setup_logging(config)
    for name in ("uvicorn", "uvicorn.error", "uvicorn.access"):
        uv_log = logging.getLogger(name)
        uv_log.handlers = []
        uv_log.propagate = True
    await db.connect()
    if config.redis_cache_url:
        cache_redis = aioredis.from_url(config.redis_cache_url, decode_responses=True)
        cache_redis_bytes = aioredis.from_url(
            config.redis_cache_url, decode_responses=False
        )
        await esi_client.startup(cache_redis)
        queries.set_redis(cache_redis)
        query_cache.set_redis(cache_redis)
        kills_binary_cache.set_redis(cache_redis_bytes)
        health.set_redis(cache_redis)
        heartbeat_task = asyncio.create_task(
            health.heartbeat_loop(
                cache_redis,
                config.health.heartbeat_interval,
                config.health.heartbeat_ttl,
            )
        )
        # Invalidation messages travel on the shared bus (the stream Redis), which
        # every worker and publisher can reach; the keys to evict live on this
        # worker's cache Redis. Subscribe on the bus, delete on the cache.
        if config.redis_url:
            invalidate_bus = aioredis.from_url(config.redis_url, decode_responses=True)
            invalidation_task = asyncio.create_task(
                invalidation.subscriber_loop(
                    invalidate_bus, cache_redis, config.streaming.invalidate_channel
                )
            )
        else:
            invalidate_bus = None
            invalidation_task = None
    else:
        cache_redis = None
        cache_redis_bytes = None
        invalidate_bus = None
        heartbeat_task = None
        invalidation_task = None
    health.set_bus_redis(invalidate_bus)
    prometheus_metrics.start_exporter(config)
    await broadcaster.start()
    yield
    await broadcaster.stop()
    for task in (heartbeat_task, invalidation_task):
        if task is not None:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
    await db.disconnect()
    await esi_client.close()
    if cache_redis is not None:
        await cache_redis.aclose()
    if cache_redis_bytes is not None:
        await cache_redis_bytes.aclose()
    if invalidate_bus is not None:
        await invalidate_bus.aclose()


app = FastAPI(
    title="EVE Killmap API",
    description="Backend API for EVE Killmap",
    version="1.0.0",
    lifespan=lifespan,
    docs_url=None,
    redoc_url=None,
    openapi_url=None,
)


@app.middleware("http")
async def _request_middleware(request, call_next):
    metrics.requests += 1
    response = await call_next(request)
    for key, value in security_headers().items():
        response.headers.setdefault(key, value)
    if "*" in config.cors.allow_origins:
        response.headers.setdefault("Access-Control-Allow-Origin", "*")
    return response


app.add_middleware(GZipMiddleware, minimum_size=1000, compresslevel=6)

app.add_middleware(
    CORSMiddleware,
    allow_origins=config.cors.allow_origins,
    allow_methods=config.cors.allow_methods,
    allow_headers=config.cors.allow_headers,
)

if config.metrics.enabled:
    prometheus_metrics.instrument_app(app)


def _parse_killmail_ids(killmail_ids_str: str) -> list[int]:
    try:
        return [int(x.strip()) for x in killmail_ids_str.split(",")]
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail="Invalid killmail_ids format. Expected comma-separated integers.",
        )


@app.get("/")
async def root():
    return {"message": "EVE Killmap API", "version": "1.0.0"}


@app.get("/health")
async def health_check():
    """Public liveness probe: 200 if this worker's DB + Redis are reachable."""
    db_ok = await db.is_healthy()
    cache_ok = await health.redis_ok()
    if db_ok and cache_ok:
        return {"status": "ok"}
    raise HTTPException(status_code=503, detail={"status": "unavailable"})


@app.get("/health/detail", response_model=HealthDetailResponse)
async def health_detail(authorization: Annotated[str | None, Header()] = None):
    """Detailed, token-gated health: worker fleet + DB stats + domain stats."""
    if config.health_token is None:
        raise HTTPException(status_code=404, detail="Not found")
    if not health.health_token_ok(
        health.extract_bearer(authorization), config.health_token
    ):
        raise HTTPException(status_code=401, detail="Unauthorized")

    payloads = await health.read_worker_heartbeats()
    workers = health.aggregate_workers(payloads, config.health.expected_workers)

    db_stats: dict = {}
    domain: dict = {}
    try:
        db_stats = await fetch_db_stats()
        db_stats["top_statements"] = await fetch_top_statements()
    except Exception:
        logger.exception("health detail db_stats failed")
        db_stats = {"error": "db_stats_unavailable"}
    try:
        domain = await fetch_domain_stats()
    except Exception:
        logger.exception("health detail domain stats failed")
        domain = {"error": "domain_stats_unavailable"}

    return HealthDetailResponse(
        status="ok",
        workers=WorkersSummary(
            worker_count=workers["worker_count"],
            degraded=workers["degraded"],
            cache_hit_rate=workers["cache_hit_rate"],
            totals=workers["totals"],
            workers=workers["workers"],
        ),
        database={**db_stats, "pool": db.pool_stats()},
        domain=domain,
        redis=await health.redis_info(),
    )


@app.get(
    "/kills/details/raw",
    response_model=RawKillDetailResponse,
)
async def get_kill_details(
    killmail_ids: Annotated[
        str,
        Query(description="Comma-separated list of killmail IDs to fetch details for"),
    ],
):
    """Get raw, unprocessed kill details for a list of killmail IDs."""
    ids_list = _parse_killmail_ids(killmail_ids)
    error = validate_id_list(ids_list, config.limits.max_killmail_ids)
    if error is not None:
        raise HTTPException(status_code=400, detail=error)
    return await get_kill_details_cached(ids_list)


@app.get(
    "/kills/details/processed",
    response_model=ProcessedKillDetailResponse,
    response_model_exclude_none=True,
)
async def get_kill_details_processed(
    killmail_id: Annotated[int, Query(description="Killmail ID to fetch details for")],
):
    """Get processed kill details for a killmail. Processed data includes character names,
    corporations, alliances, war information, etc."""
    cache_params = {"killmail_id": killmail_id}
    cached = await query_cache.get("kill_details_processed", cache_params)
    if cached is not None:
        return ProcessedKillDetailResponse.model_validate_json(cached)

    raw = await fetch_kills_by_ids([killmail_id])
    if not raw.kills:
        raise HTTPException(status_code=404, detail=f"Killmail {killmail_id} not found")

    kill = raw.kills[0]
    victim = kill.victim
    attackers = kill.attackers

    if not attackers:
        raise HTTPException(
            status_code=500, detail="Killmail data is malformed: no attackers"
        )

    final_blow_idx = next((i for i, a in enumerate(attackers) if a.final_blow), None)
    top_damage_idx = max(range(len(attackers)), key=lambda i: attackers[i].damage_done)

    if final_blow_idx is None:
        raise HTTPException(
            status_code=500, detail="Killmail data is malformed: no final blow attacker"
        )

    final_blow_attacker = attackers[final_blow_idx]
    top_damage_attacker = attackers[top_damage_idx]
    final_blow_is_top_damage = final_blow_idx == top_damage_idx

    ids_to_resolve: set[int] = set()
    type_ids_to_resolve: set[int] = set()

    if victim.character_id is None:
        type_ids_to_resolve.add(victim.ship_type_id)
    else:
        ids_to_resolve.add(victim.character_id)
    if victim.faction_id is not None:
        ids_to_resolve.add(victim.faction_id)

    for atk in [final_blow_attacker, top_damage_attacker]:
        if atk.character_id is None:
            if atk.ship_type_id is not None:
                type_ids_to_resolve.add(atk.ship_type_id)
        else:
            ids_to_resolve.add(atk.character_id)
            if atk.ship_type_id is not None:
                type_ids_to_resolve.add(atk.ship_type_id)
        if atk.faction_id is not None:
            ids_to_resolve.add(atk.faction_id)
        if atk.weapon_type_id is not None:
            type_ids_to_resolve.add(atk.weapon_type_id)

    corp_id_set: set[int] = {
        id_
        for id_ in [
            victim.corporation_id,
            final_blow_attacker.corporation_id,
            top_damage_attacker.corporation_id,
        ]
        if id_ is not None
    }
    alliance_id_set: set[int] = {
        id_
        for id_ in [
            victim.alliance_id,
            final_blow_attacker.alliance_id,
            top_damage_attacker.alliance_id,
        ]
        if id_ is not None
    }

    raw_war: dict | None = None
    if kill.war_id is not None:
        try:
            raw_war = await esi_client.get_war_info(kill.war_id)
            for side in ("aggressor", "defender"):
                p = raw_war[side]
                if p.get("corporation_id") is not None:
                    corp_id_set.add(p["corporation_id"])
                if p.get("alliance_id") is not None:
                    alliance_id_set.add(p["alliance_id"])
        except (EsiRateLimitError, EsiNotFoundError):
            pass
        except RuntimeError:
            logger.exception("ESI upstream call failed")
            raise HTTPException(status_code=502, detail="Upstream service unavailable")

    corp_ids = list(corp_id_set)
    alliance_ids = list(alliance_id_set)

    try:
        all_results = await asyncio.gather(
            esi_client.resolve_names(ids_to_resolve),
            get_type_names(type_ids_to_resolve),
            *[esi_client.get_corporation_info(cid) for cid in corp_ids],
            *[esi_client.get_alliance_info(aid) for aid in alliance_ids],
        )
    except RuntimeError:
        logger.exception("ESI upstream call failed")
        raise HTTPException(status_code=502, detail="Upstream service unavailable")

    names: dict[int, str] = {**all_results[0], **all_results[1]}  # type: ignore[arg-type]
    corp_info: dict[int, tuple[str, str]] = {  # type: ignore[misc]
        corp_ids[i]: all_results[2 + i] for i in range(len(corp_ids))
    }
    alliance_info: dict[int, tuple[str, str]] = {  # type: ignore[misc]
        alliance_ids[i]: all_results[2 + len(corp_ids) + i]
        for i in range(len(alliance_ids))
    }

    def build_attacker(atk) -> AttackerProcessed:
        if atk.character_id is None:
            character = (
                names.get(atk.ship_type_id, "Unknown")
                if atk.ship_type_id is not None
                else "Unknown"
            )
            ship = None
        else:
            character = names.get(atk.character_id, "Unknown")
            ship = names.get(atk.ship_type_id) if atk.ship_type_id is not None else None
        corp = (
            corp_info.get(atk.corporation_id)
            if atk.corporation_id is not None
            else None
        )
        alliance = (
            alliance_info.get(atk.alliance_id) if atk.alliance_id is not None else None
        )
        return AttackerProcessed(
            character=character,
            character_corporation=corp[0] if corp else None,
            character_corporation_ticker=corp[1] if corp else None,
            character_alliance=alliance[0] if alliance else None,
            character_alliance_ticker=alliance[1] if alliance else None,
            character_faction=(
                names.get(atk.faction_id) if atk.faction_id is not None else None
            ),
            ship=ship,
            weapon=(
                names.get(atk.weapon_type_id)
                if atk.weapon_type_id is not None
                else None
            ),
            damage_done=atk.damage_done,
            security_status=atk.security_status,
        )

    victim_corp = (
        corp_info.get(victim.corporation_id)
        if victim.corporation_id is not None
        else None
    )
    victim_alliance = (
        alliance_info.get(victim.alliance_id)
        if victim.alliance_id is not None
        else None
    )

    if victim.character_id is None:
        victim_name = names.get(victim.ship_type_id, "Unknown")
    else:
        victim_name = names.get(victim.character_id, "Unknown")

    victim_processed = VictimProcessed(
        character=victim_name,
        character_corporation=victim_corp[0] if victim_corp else None,
        character_corporation_ticker=victim_corp[1] if victim_corp else None,
        character_alliance=victim_alliance[0] if victim_alliance else None,
        character_alliance_ticker=victim_alliance[1] if victim_alliance else None,
        character_faction=(
            names.get(victim.faction_id) if victim.faction_id is not None else None
        ),
        damage_taken=victim.damage_taken,
    )

    def build_war_participant(p: dict) -> WarParticipant:
        corp_id = p.get("corporation_id")
        alliance_id = p.get("alliance_id")
        corp = corp_info.get(corp_id) if corp_id is not None else None
        alliance = alliance_info.get(alliance_id) if alliance_id is not None else None
        return WarParticipant(
            alliance=alliance[0] if alliance else None,
            alliance_ticker=alliance[1] if alliance else None,
            corporation=corp[0] if corp else None,
            corporation_ticker=corp[1] if corp else None,
            ships_killed=p["ships_killed"],
        )

    war_info: WarProcessed | None = None
    if raw_war is not None:
        _declared = iso_to_epoch(raw_war["declared"])
        assert _declared is not None, "ESI war.declared is always a timestamp string"
        war_info = WarProcessed(
            aggressor=build_war_participant(raw_war["aggressor"]),
            defender=build_war_participant(raw_war["defender"]),
            declared=_declared,
            finished=iso_to_epoch(raw_war.get("finished")),
            mutual=raw_war["mutual"],
            retracted=iso_to_epoch(raw_war.get("retracted")),
            started=iso_to_epoch(raw_war.get("started")),
        )

    result = ProcessedKillDetailResponse(
        victim=victim_processed,
        final_blow=build_attacker(final_blow_attacker),
        top_damage=build_attacker(top_damage_attacker),
        war_id=kill.war_id,
        war_info=war_info,
        final_blow_is_top_damage=final_blow_is_top_damage,
        attackers=len(attackers),
    )

    await query_cache.set(
        "kill_details_processed",
        cache_params,
        result.model_dump_json(),
        ttl=config.cache.kill_detail_processed_ttl,
    )
    return result


@app.get("/systems/{solar_system_id}/kills", response_model=None)
async def get_system_kills(
    solar_system_id: int,
    since: Annotated[
        int | None,
        Query(
            description="Only return kills inserted after this Unix epoch timestamp (seconds)"
        ),
    ] = None,
    if_none_match: Annotated[str | None, Header(alias="If-None-Match")] = None,
):
    """Get binary-encoded kills in a solar system."""
    if since is not None:
        latest = await get_system_latest(solar_system_id)
        if should_short_circuit(since, latest):
            prometheus_metrics.since_short_circuits.inc()
            payload = encode_kills_binary([], [], [], [], [], [])
            prometheus_metrics.kills_binary_response_bytes.observe(len(payload))
            return Response(
                content=payload,
                media_type="application/octet-stream",
            )

    cache_params = {"solar_system_id": solar_system_id, "since": since}
    cached = await kills_binary_cache.get(cache_params)
    if cached is None:
        result = await fetch_raw_kills(solar_system_id=solar_system_id, since=since)
        cached = encode_kills_binary(
            killmail_ids=result["killmail_ids"],
            killmail_times=result["killmail_times"],
            x=[int(v) for v in result["x"]],
            y=[int(v) for v in result["y"]],
            z=[int(v) for v in result["z"]],
            ship_types=result["ship_types"],
        )
        await kills_binary_cache.set(cache_params, cached)
    prometheus_metrics.kills_binary_response_bytes.observe(len(cached))
    if since is None:
        return binary_cache_response(
            cached, max_age=config.cache.binary_ttl, if_none_match=if_none_match
        )
    return Response(content=cached, media_type="application/octet-stream")


@app.get("/systems/{solar_system_id}/sov", response_model=None)
async def get_system_sov(
    solar_system_id: int,
    if_none_match: Annotated[str | None, Header(alias="If-None-Match")] = None,
):
    """Get the current sovereignty of a solar system."""
    cache_params = {"solar_system_id": solar_system_id}
    cached = await query_cache.get("sov", cache_params)
    if cached is None:
        sov_map = await esi_client.get_sov_map_cached()
        if sov_map is None:
            raise HTTPException(status_code=503, detail="Sovereignty data warming up")
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
        cached = result.model_dump_json(exclude_none=True)
        await query_cache.set("sov", cache_params, cached, ttl=config.cache.sov_ttl)
    assert cached is not None
    return json_cache_response(
        cached, max_age=config.cache.sov_ttl, if_none_match=if_none_match
    )


@app.get("/stats/system-rankings", response_model=None)
async def get_system_rankings(
    limit: Annotated[
        int, Query(ge=1, le=50, description="Number of systems to return")
    ] = 10,
    if_none_match: Annotated[str | None, Header(alias="If-None-Match")] = None,
):
    """Get rank list of solar systems by highest/lowest number of kills."""
    cache_params = {"limit": limit}
    cached = await query_cache.get("system_rankings", cache_params)
    if cached is None:
        async with single_flight.lock(f"system_rankings:{limit}"):
            cached = await query_cache.get("system_rankings", cache_params)
            if cached is None:
                top = await fetch_top_systems(limit=limit)
                bottom = await fetch_bottom_systems(limit=limit)
                result = RankSystemsResponse(top=top, bottom=bottom)
                cached = result.model_dump_json()
                await query_cache.set(
                    "system_rankings",
                    cache_params,
                    cached,
                    ttl=config.cache.rankings_ttl,
                )
    return json_cache_response(
        cached, max_age=config.cache.rankings_ttl, if_none_match=if_none_match
    )


@app.get("/systems/{solar_system_id}/farthest_kill", response_model=None)
async def get_farthest_kill(
    solar_system_id: int,
    if_none_match: Annotated[str | None, Header(alias="If-None-Match")] = None,
):
    """Get the distance from (0, 0, 0) to the farthest kill in the solar system.
    Returns -1 if the solar system has no kills."""
    cache_params = {"solar_system_id": solar_system_id}
    cached = await query_cache.get("farthest_kill", cache_params)
    if cached is None:
        async with single_flight.lock(f"farthest_kill:{solar_system_id}"):
            cached = await query_cache.get("farthest_kill", cache_params)
            if cached is None:
                value = normalize_farthest_kill(
                    await fetch_farthest_kill(solar_system_id)
                )
                cached = FarthestKillResponse(farthest_kill=value).model_dump_json()
                await query_cache.set(
                    "farthest_kill",
                    cache_params,
                    cached,
                    ttl=config.cache.farthest_kill_ttl,
                )
    assert cached is not None
    return json_cache_response(
        cached, max_age=config.cache.farthest_kill_ttl, if_none_match=if_none_match
    )


@app.post("/universe/names")
async def resolve_universe_names(
    ids: Annotated[list[int], Body()],
) -> dict[int, str]:
    """Resolve names for a list of IDs."""
    error = validate_id_list(ids, config.limits.max_name_ids)
    if error is not None:
        raise HTTPException(status_code=400, detail=error)
    try:
        return await esi_client.resolve_names(set(ids))
    except EsiNotFoundError:
        raise HTTPException(status_code=404, detail="One or more IDs not found")
    except RuntimeError:
        logger.exception("ESI resolve_names failed")
        raise HTTPException(status_code=502, detail="Upstream service unavailable")


async def _ws_stream(websocket: WebSocket, q: asyncio.Queue) -> None:
    """Accept a WebSocket and stream kills from q until the client disconnects."""
    await websocket.accept()

    async def _send() -> None:
        while True:
            kill = await q.get()
            await websocket.send_text(json.dumps(kill))
            await asyncio.sleep(0.25 if q.qsize() > 10 else 0.5)

    send_task = asyncio.create_task(_send())
    try:
        while True:
            msg = await websocket.receive()
            if msg["type"] == "websocket.disconnect":
                break
    except Exception:
        pass
    finally:
        send_task.cancel()
        await asyncio.gather(send_task, return_exceptions=True)


async def _ws_guard(websocket: WebSocket) -> bool:
    """Reject the socket (returns False) if the Origin is not allowed, the server
    is over its connection cap, or the broadcaster is not running.
    Otherwise returns True WITHOUT accepting; the caller accepts via _ws_stream.
    On rejection the socket is already accepted+closed."""
    origin = websocket.headers.get("origin")
    if not origin_allowed(origin, config.cors.allow_origins):
        await websocket.accept()
        await websocket.close(code=1008, reason="Origin not allowed")
        prometheus_metrics.ws_connections.labels(
            transport="ws", outcome="rejected_origin"
        ).inc()
        return False
    current = metrics.ws_global_connections + metrics.ws_system_connections
    if at_capacity(current, config.limits.max_ws_connections):
        await websocket.accept()
        await websocket.close(code=1013, reason="Server at capacity")
        prometheus_metrics.ws_connections.labels(
            transport="ws", outcome="rejected_capacity"
        ).inc()
        return False
    if not broadcaster.is_running:
        await websocket.accept()
        await websocket.close(code=1011, reason="Live kill streaming unavailable")
        prometheus_metrics.ws_connections.labels(
            transport="ws", outcome="unavailable"
        ).inc()
        return False
    prometheus_metrics.ws_connections.labels(transport="ws", outcome="accepted").inc()
    return True


@app.websocket("/ws/global/kills")
async def ws_kills_live(websocket: WebSocket):
    """Stream every new kill across all solar systems."""
    if not await _ws_guard(websocket):
        return
    q = broadcaster.subscribe_global()
    try:
        await _ws_stream(websocket, q)
    finally:
        broadcaster.unsubscribe_global(q)


@app.websocket("/ws/systems/{solar_system_id}/kills")
async def ws_system_kills(websocket: WebSocket, solar_system_id: int):
    """Stream new kills for a specific solar system."""
    if not await _ws_guard(websocket):
        return
    q = broadcaster.subscribe_system(solar_system_id)
    try:
        await _ws_stream(websocket, q)
    finally:
        broadcaster.unsubscribe_system(solar_system_id, q)
