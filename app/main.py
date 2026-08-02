import asyncio
from contextlib import asynccontextmanager

import redis.asyncio as aioredis
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.gzip import GZipMiddleware

from app.config import config, setup_logging
from app.metrics import metrics
from app.security import security_headers
from app.database import db
import app.health as health
from app.cache import query_cache, kills_binary_cache
from app.esi import esi_client
from app.redis_client import broadcaster
from app import prometheus_metrics
import app.queries as queries
import app.invalidation as invalidation


def _build_cache_client(decode_responses: bool) -> aioredis.Redis:
    pool = aioredis.BlockingConnectionPool.from_url(
        config.redis_cache_url,
        decode_responses=decode_responses,
        max_connections=config.cache_redis.max_connections,
        timeout=config.cache_redis.pool_timeout,
        socket_connect_timeout=config.cache_redis.socket_connect_timeout,
        socket_timeout=config.cache_redis.socket_timeout,
        socket_keepalive=config.cache_redis.socket_keepalive,
        health_check_interval=config.cache_redis.health_check_interval,
    )
    return aioredis.Redis(connection_pool=pool)


@asynccontextmanager
async def lifespan(_app: FastAPI):
    setup_logging(config)  # also re-points uvicorn's loggers at our handlers/level
    await db.connect()
    if config.redis_cache_url:
        cache_redis = _build_cache_client(decode_responses=True)
        cache_redis_bytes = _build_cache_client(decode_responses=False)
        await esi_client.startup(cache_redis)
        queries.set_redis(cache_redis)
        query_cache.set_redis(cache_redis_bytes)
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
    expose_headers=config.cors.expose_headers,
)

if config.metrics.enabled:
    prometheus_metrics.instrument_app(app)

from app.routers import health as health_router
app.include_router(health_router.router)

from app.routers import kills as kills_router
app.include_router(kills_router.router)

from app.routers import systems as systems_router
app.include_router(systems_router.router)

from app.routers import stats as stats_router
app.include_router(stats_router.router)

from app.routers import universe as universe_router
app.include_router(universe_router.router)

from app.routers import ws as ws_router
app.include_router(ws_router.router)
