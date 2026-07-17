import asyncio
import json
import time
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

import aiohttp
import redis.asyncio as aioredis

from app.config import config
from app import prometheus_metrics as pm

ESI_BASE = "https://esi.evetech.net/latest"


def ttl_from_expires(expires_header: str | None, fallback_seconds: int) -> int:
    if not expires_header:
        return fallback_seconds
    try:
        expires_dt = parsedate_to_datetime(expires_header)
        now = datetime.now(timezone.utc)
        ttl = int((expires_dt - now).total_seconds())
        return max(ttl, 60)
    except Exception:
        return fallback_seconds


class EsiClient:

    def __init__(self):
        self._session: aiohttp.ClientSession | None = None
        self._session_lock = asyncio.Lock()
        self._redis: aioredis.Redis | None = None

    async def startup(self, redis: aioredis.Redis) -> None:
        self._redis = redis

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            async with self._session_lock:
                if self._session is None or self._session.closed:
                    self._session = aiohttp.ClientSession(
                        headers={"User-Agent": config.user_agent},
                    )
        return self._session

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()

    async def get_corporation_info(self, corporation_id: int) -> tuple[str, str]:
        """Returns (name, ticker)."""
        if self._redis is not None:
            cached = await self._redis.get(f"esi:corp:{corporation_id}")
            if cached is not None:
                pm.esi_cache_hits.labels(entity="corporation").inc()
                return tuple(json.loads(cached))  # type: ignore[return-value]
            pm.esi_cache_misses.labels(entity="corporation").inc()

        session = await self._get_session()
        _start = time.perf_counter()
        try:
            async with session.get(
                f"{ESI_BASE}/corporations/{corporation_id}/"
            ) as resp:
                resp.raise_for_status()
                data = await resp.json()
        except aiohttp.ClientError as exc:
            pm.esi_requests.labels(endpoint="corporation", outcome="error").inc()
            pm.errors.labels(component="esi").inc()
            raise RuntimeError(
                f"ESI corporations/{corporation_id} request failed: {exc!r}"
            ) from exc
        finally:
            pm.esi_request_seconds.labels(endpoint="corporation").observe(
                time.perf_counter() - _start
            )
        pm.esi_requests.labels(endpoint="corporation", outcome="ok").inc()

        result = (data["name"], data["ticker"])
        if self._redis is not None:
            await self._redis.set(
                f"esi:corp:{corporation_id}",
                json.dumps(list(result)),
                ex=config.cache.esi_corp_ttl,
            )
        return result

    async def get_alliance_info(self, alliance_id: int) -> tuple[str, str]:
        """Returns (name, ticker)."""
        if self._redis is not None:
            cached = await self._redis.get(f"esi:alliance:{alliance_id}")
            if cached is not None:
                pm.esi_cache_hits.labels(entity="alliance").inc()
                return tuple(json.loads(cached))  # type: ignore[return-value]
            pm.esi_cache_misses.labels(entity="alliance").inc()

        session = await self._get_session()
        _start = time.perf_counter()
        try:
            async with session.get(f"{ESI_BASE}/alliances/{alliance_id}/") as resp:
                resp.raise_for_status()
                data = await resp.json()
        except aiohttp.ClientError as exc:
            pm.esi_requests.labels(endpoint="alliance", outcome="error").inc()
            pm.errors.labels(component="esi").inc()
            raise RuntimeError(
                f"ESI alliances/{alliance_id} request failed: {exc!r}"
            ) from exc
        finally:
            pm.esi_request_seconds.labels(endpoint="alliance").observe(
                time.perf_counter() - _start
            )
        pm.esi_requests.labels(endpoint="alliance", outcome="ok").inc()

        result = (data["name"], data["ticker"])
        if self._redis is not None:
            await self._redis.set(
                f"esi:alliance:{alliance_id}",
                json.dumps(list(result)),
                ex=config.cache.esi_alliance_ttl,
            )
        return result

    async def _fetch_sov_map(self) -> tuple[dict[int, dict], str | None]:
        session = await self._get_session()
        _start = time.perf_counter()
        try:
            async with session.get(f"{ESI_BASE}/sovereignty/map/") as resp:
                resp.raise_for_status()
                data = await resp.json()
                expires_header = resp.headers.get("Expires")
        except aiohttp.ClientError as exc:
            pm.esi_requests.labels(endpoint="sov", outcome="error").inc()
            pm.errors.labels(component="esi").inc()
            raise RuntimeError(f"ESI sovereignty/map request failed: {exc!r}") from exc
        finally:
            pm.esi_request_seconds.labels(endpoint="sov").observe(
                time.perf_counter() - _start
            )
        pm.esi_requests.labels(endpoint="sov", outcome="ok").inc()
        sov_map = {item["system_id"]: item for item in data}
        return sov_map, expires_header

    async def refresh_sov_map(self) -> int:
        """Fetch the sov map from ESI and store it in Redis. Returns the TTL used."""
        sov_map, expires_header = await self._fetch_sov_map()
        ttl = ttl_from_expires(
            expires_header, fallback_seconds=config.cache.esi_sov_fallback_ttl
        )
        if self._redis is not None:
            await self._redis.set(
                "esi:sov_map",
                json.dumps({str(k): v for k, v in sov_map.items()}),
                ex=ttl,
            )
        return ttl

    async def get_sov_map_cached(self) -> dict[int, dict] | None:
        """Read the sov map from Redis only. None if absent (or no redis configured)."""
        if self._redis is None:
            return None
        cached = await self._redis.get("esi:sov_map")
        if cached is None:
            pm.esi_cache_misses.labels(entity="sov").inc()
            return None
        pm.esi_cache_hits.labels(entity="sov").inc()
        raw: dict[str, dict] = json.loads(cached)
        return {int(k): v for k, v in raw.items()}


esi_client = EsiClient()
