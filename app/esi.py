import asyncio
import json
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any, Callable

import aiohttp
import redis.asyncio as aioredis

from app.config import config
from app.timeparse import iso_to_epoch
from app import prometheus_metrics as pm

logger = logging.getLogger(__name__)

ESI_BASE = "https://esi.evetech.net/latest"

# ESI's error-limit window is 60s, so a larger X-ESI-Error-Limit-Reset is bogus. It is
# clamped because the cooldown is client-wide: an unbounded value stalls every feed.
ESI_ERROR_LIMIT_MAX_RESET = 60


class EsiTransientError(RuntimeError):
    """ESI was transiently unavailable (5xx / connection / timeout) -- e.g. during
    EVE's daily downtime. A RuntimeError subclass so existing handlers still catch
    it, while callers can special-case it as an expected, self-healing condition."""


def _is_transient_esi_error(exc: BaseException) -> bool:
    if isinstance(exc, aiohttp.ClientResponseError):
        return exc.status >= 500
    return isinstance(exc, aiohttp.ClientConnectionError)


def _esi_error_brief(exc: BaseException) -> str:
    if isinstance(exc, aiohttp.ClientResponseError):
        return f"{exc.status} {exc.message}"
    return type(exc).__name__


def ttl_from_expires(
    expires_header: str | None, fallback_seconds: int, floor: int
) -> int:
    if not expires_header:
        return fallback_seconds
    try:
        expires_dt = parsedate_to_datetime(expires_header)
        now = datetime.now(timezone.utc)
        return max(int((expires_dt - now).total_seconds()), floor)
    except Exception:
        return fallback_seconds


def _reduce_sov_structures(data: list[dict]) -> dict[int, dict]:
    out: dict[int, dict] = {}
    for item in data:
        level = item.get("vulnerability_occupancy_level")
        if level is None:
            continue
        sid = item["solar_system_id"]
        level = float(level)
        existing = out.get(sid)
        if existing is None or level > existing["adm"]:
            out[sid] = {
                "adm": level,
                "start": iso_to_epoch(item.get("vulnerable_start_time")),
                "end": iso_to_epoch(item.get("vulnerable_end_time")),
            }
    return out


class EsiFeedRefreshError(RuntimeError):
    """A feed refresh produced no value; the caller should back off and retry."""


@dataclass(frozen=True)
class EsiFeed:
    name: str
    path: str
    redis_key: str
    fallback_ttl: Callable[[], int]
    ttl_floor: int
    transform: Callable[[Any], Any]
    decode: Callable[[Any], Any]
    sleep_skew: int
    sleep_min: int
    sleep_max: int
    store_ttl: Callable[[], int] | None = None
    invalidate_targets: tuple[str, ...] = ()
    offline_value: Any | None = None
    required: bool = True


def _int_keyed(raw: dict[str, Any]) -> dict[int, Any]:
    return {int(k): v for k, v in raw.items()}


class EsiClient:

    def __init__(self):
        self._session: aiohttp.ClientSession | None = None
        self._session_lock = asyncio.Lock()
        self._redis: aioredis.Redis | None = None
        self._error_limit_reset_at: float = 0.0

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

    async def _fetch_json(self, feed: EsiFeed) -> tuple[Any, str | None]:
        now = time.monotonic()
        if now < self._error_limit_reset_at:
            await asyncio.sleep(self._error_limit_reset_at - now)
        session = await self._get_session()
        _start = time.perf_counter()
        try:
            async with session.get(f"{ESI_BASE}{feed.path}") as resp:
                self._record_error_limit(resp.headers)
                resp.raise_for_status()
                data, expires = await resp.json(), resp.headers.get("Expires")
        except aiohttp.ClientError as exc:
            pm.esi_requests.labels(endpoint=feed.name, outcome="error").inc()
            pm.errors.labels(component="esi").inc()
            if _is_transient_esi_error(exc):
                raise EsiTransientError(
                    f"ESI {feed.path} transiently unavailable: {_esi_error_brief(exc)}"
                ) from exc
            raise RuntimeError(f"ESI {feed.path} request failed: {exc!r}") from exc
        finally:
            pm.esi_request_seconds.labels(endpoint=feed.name).observe(
                time.perf_counter() - _start
            )
        pm.esi_requests.labels(endpoint=feed.name, outcome="ok").inc()
        return data, expires

    def _record_error_limit(self, headers) -> None:
        remain = headers.get("X-ESI-Error-Limit-Remain")
        if remain is None:
            return
        try:
            remain_i = int(remain)
        except (TypeError, ValueError):
            return
        pm.esi_error_limit_remain.set(remain_i)
        if remain_i > 10:
            return
        try:
            reset_i = int(headers.get("X-ESI-Error-Limit-Reset"))
        except (TypeError, ValueError):
            return
        reset_i = min(max(reset_i, 0), ESI_ERROR_LIMIT_MAX_RESET)
        if reset_i == 0:
            return
        now = time.monotonic()
        if now >= self._error_limit_reset_at:
            logger.warning(
                "ESI error limit engaged: %s errors remaining, "
                "pausing ESI requests for %ss",
                remain_i,
                reset_i,
            )
        self._error_limit_reset_at = max(self._error_limit_reset_at, now + reset_i)

    async def _store(self, feed: EsiFeed, value: Any, ttl: int) -> None:
        if self._redis is None:
            return
        await self._redis.set(
            feed.redis_key,
            json.dumps(value),
            ex=feed.store_ttl() if feed.store_ttl else ttl,
        )

    async def refresh(self, feed: EsiFeed) -> tuple[int, Any]:
        """Fetch, transform and store one feed. Returns (esi_ttl, stored_value).

        The ESI TTL drives the caller's cadence; retention is feed.store_ttl.
        Raises EsiFeedRefreshError when no value could be produced.
        """
        try:
            data, expires = await self._fetch_json(feed)
        except Exception as exc:
            transient = isinstance(exc, EsiTransientError)
            if feed.offline_value is not None and transient:
                ttl = feed.fallback_ttl()
                await self._store(feed, feed.offline_value, ttl)
                pm.esi_feed_refreshes.labels(feed=feed.name, outcome="offline").inc()
                logger.info("ESI feed %s reporting offline: %s", feed.name, exc)
                return ttl, feed.offline_value
            if not feed.required and transient:
                pm.esi_feed_refreshes.labels(feed=feed.name, outcome="degraded").inc()
                logger.info("ESI feed %s degraded: %s", feed.name, exc)
            else:
                pm.esi_feed_refreshes.labels(feed=feed.name, outcome="error").inc()
                logger.warning("ESI feed %s refresh failed: %s", feed.name, exc)
            raise EsiFeedRefreshError(feed.name) from exc
        ttl = ttl_from_expires(expires, feed.fallback_ttl(), feed.ttl_floor)
        value = feed.transform(data)
        await self._store(feed, value, ttl)
        pm.esi_feed_refreshes.labels(feed=feed.name, outcome="ok").inc()
        return ttl, value

    async def get_cached(self, feed: EsiFeed) -> Any | None:
        if self._redis is None:
            return None
        cached = await self._redis.get(feed.redis_key)
        if cached is None:
            pm.esi_cache_misses.labels(entity=feed.name).inc()
            return None
        pm.esi_cache_hits.labels(entity=feed.name).inc()
        return feed.decode(json.loads(cached))


SOV_MAP = EsiFeed(
    name="sov",
    path="/sovereignty/map/",
    redis_key="esi:sov_map",
    fallback_ttl=lambda: config.cache.esi_sov_fallback_ttl,
    ttl_floor=60,
    transform=lambda data: {str(i["system_id"]): i for i in data},
    decode=_int_keyed,
    sleep_skew=60,
    sleep_min=60,
    sleep_max=3600,
    store_ttl=lambda: 7200,
    invalidate_targets=("sov", "sov_map"),
)

SOV_STRUCTURES = EsiFeed(
    name="sov_structures",
    path="/sovereignty/structures/",
    redis_key="esi:sov_structures",
    fallback_ttl=lambda: config.cache.esi_sov_structures_fallback_ttl,
    ttl_floor=60,
    transform=lambda data: {str(k): v for k, v in _reduce_sov_structures(data).items()},
    decode=_int_keyed,
    sleep_skew=60,
    sleep_min=60,
    sleep_max=3600,
    store_ttl=lambda: 7200,
    invalidate_targets=("sov", "sov_map"),
    required=False,
)

SYSTEM_JUMPS = EsiFeed(
    name="system_jumps",
    path="/universe/system_jumps/",
    redis_key="esi:system_jumps",
    fallback_ttl=lambda: config.cache.esi_system_jumps_fallback_ttl,
    ttl_floor=60,
    transform=lambda data: {str(i["system_id"]): i["ship_jumps"] for i in data},
    decode=_int_keyed,
    sleep_skew=60,
    sleep_min=60,
    sleep_max=3600,
    store_ttl=lambda: 7200,
    invalidate_targets=("system_jumps",),
)

# store_ttl is deliberately far longer than the ~28s refresh cadence: an `ex` close to
# the cadence would let the key lapse between refreshes and read back as a cold cache.
STATUS = EsiFeed(
    name="status",
    path="/status/",
    redis_key="esi:status",
    fallback_ttl=lambda: config.cache.esi_status_fallback_ttl,
    ttl_floor=15,
    transform=lambda data: {"online": True, "players": data["players"]},
    decode=lambda raw: raw,
    sleep_skew=2,
    sleep_min=15,
    sleep_max=60,
    store_ttl=lambda: 600,
    offline_value={"online": False},
)


esi_client = EsiClient()
