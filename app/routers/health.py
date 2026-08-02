import logging
from typing import Annotated

from fastapi import APIRouter, Header, HTTPException

from app.config import config
from app.database import db
import app.health as health
from app.models import HealthDetailResponse, WorkersSummary
from app.queries import fetch_db_stats, fetch_domain_stats, fetch_top_statements

logger = logging.getLogger(__name__)
router = APIRouter()


@router.get("/")
async def root():
    return {"message": "EVE Killmap API", "version": "1.0.0"}


@router.get("/health")
async def health_check():
    """Public liveness probe: 200 if this worker's DB + Redis are reachable."""
    db_ok = await db.is_healthy()
    cache_ok = await health.redis_ok()
    if db_ok and cache_ok:
        return {"status": "ok"}
    raise HTTPException(status_code=503, detail={"status": "unavailable"})


@router.get("/health/detail", response_model=HealthDetailResponse)
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
