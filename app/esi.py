import asyncio
import json
import time
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

import aiohttp
import redis.asyncio as aioredis

from app.config import config

ESI_BASE = "https://esi.evetech.net/latest"


class EsiNotFoundError(Exception):
    pass


class EsiRateLimitError(Exception):
    pass


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
                return tuple(json.loads(cached))  # type: ignore[return-value]

        session = await self._get_session()
        try:
            async with session.get(
                f"{ESI_BASE}/corporations/{corporation_id}/"
            ) as resp:
                resp.raise_for_status()
                data = await resp.json()
        except aiohttp.ClientError as exc:
            raise RuntimeError(
                f"ESI corporations/{corporation_id} request failed: {exc!r}"
            ) from exc

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
                return tuple(json.loads(cached))  # type: ignore[return-value]

        session = await self._get_session()
        try:
            async with session.get(f"{ESI_BASE}/alliances/{alliance_id}/") as resp:
                resp.raise_for_status()
                data = await resp.json()
        except aiohttp.ClientError as exc:
            raise RuntimeError(
                f"ESI alliances/{alliance_id} request failed: {exc!r}"
            ) from exc

        result = (data["name"], data["ticker"])
        if self._redis is not None:
            await self._redis.set(
                f"esi:alliance:{alliance_id}",
                json.dumps(list(result)),
                ex=config.cache.esi_alliance_ttl,
            )
        return result

    async def resolve_names(self, ids: set[int]) -> dict[int, str]:
        if not ids:
            return {}

        result: dict[int, str] = {}
        uncached: list[int] = []

        if self._redis is not None:
            cached_values = await self._redis.mget(
                *[f"esi:character:{entity_id}" for entity_id in ids]
            )
            for entity_id, val in zip(ids, cached_values):
                if val is not None:
                    # mget may return bytes when decode_responses=False; normalise.
                    result[entity_id] = val.decode() if isinstance(val, bytes) else val
                else:
                    uncached.append(entity_id)
        else:
            uncached = list(ids)

        if not uncached:
            return result

        session = await self._get_session()
        for i in range(0, len(uncached), 1000):
            batch = uncached[i : i + 1000]
            try:
                async with session.post(
                    f"{ESI_BASE}/universe/names/", json=batch
                ) as resp:
                    if resp.status == 404:
                        try:
                            body = await resp.json()
                            esi_msg = body.get("error", str(batch))
                        except Exception:
                            esi_msg = str(batch)
                        raise EsiNotFoundError(
                            f"One or more invalid IDs in request: {esi_msg}"
                        )
                    resp.raise_for_status()
                    data = await resp.json()
            except EsiNotFoundError:
                raise
            except aiohttp.ClientError as exc:
                raise RuntimeError(
                    f"ESI universe/names request failed: {exc!r}"
                ) from exc

            if self._redis is not None:
                pipe = self._redis.pipeline()
                for item in data:
                    pipe.set(
                        f"esi:character:{item['id']}",
                        item["name"],
                        ex=config.cache.esi_name_ttl,
                    )
                await pipe.execute()

            for item in data:
                result[item["id"]] = item["name"]

        return result

    async def get_war_info(self, war_id: int) -> dict:
        if self._redis is not None:
            cached = await self._redis.get(f"esi:war:{war_id}")
            if cached is not None:
                return json.loads(cached)

        if self._redis is not None:
            blocked_until_raw, remaining_raw = await asyncio.gather(
                self._redis.get("esi:ratelimit:war:blocked_until"),
                self._redis.get("esi:ratelimit:war:remaining"),
            )
            if blocked_until_raw is not None:
                blocked_until = float(blocked_until_raw)
                if blocked_until > time.time():
                    secs = blocked_until - time.time()
                    raise EsiRateLimitError(
                        f"War endpoint rate limited, retry after {secs:.0f}s"
                    )
            if remaining_raw is not None:
                remaining = int(remaining_raw)
                if remaining < config.esi.war_ratelimit_low_water:
                    raise EsiRateLimitError(
                        f"War endpoint rate limit low ({remaining} tokens remaining), skipping fetch"
                    )

        session = await self._get_session()
        try:
            async with session.get(f"{ESI_BASE}/wars/{war_id}/") as resp:
                remaining_header = resp.headers.get("X-Ratelimit-Remaining")
                if remaining_header is not None and self._redis is not None:
                    try:
                        await self._redis.set(
                            "esi:ratelimit:war:remaining",
                            str(int(float(remaining_header))),
                        )
                    except ValueError:
                        pass

                if resp.status == 429:
                    retry_after = resp.headers.get("Retry-After", "60")
                    if self._redis is not None:
                        try:
                            blocked_until = time.time() + float(retry_after)
                            await self._redis.set(
                                "esi:ratelimit:war:blocked_until", str(blocked_until)
                            )
                        except ValueError:
                            await self._redis.set(
                                "esi:ratelimit:war:blocked_until",
                                str(time.time() + 60.0),
                            )
                    raise EsiRateLimitError(
                        f"War endpoint returned 429, blocked for {retry_after}s"
                    )

                if resp.status == 404:
                    raise EsiNotFoundError(f"War {war_id} not found")

                resp.raise_for_status()
                data = await resp.json()
                expires_header = resp.headers.get("Expires")

        except (EsiRateLimitError, EsiNotFoundError):
            raise
        except aiohttp.ClientError as exc:
            raise RuntimeError(f"ESI wars/{war_id} request failed: {exc!r}") from exc

        if self._redis is not None:
            ttl = ttl_from_expires(
                expires_header, fallback_seconds=config.cache.esi_war_fallback_ttl
            )
            await self._redis.set(f"esi:war:{war_id}", json.dumps(data), ex=ttl)
        return data

    async def _fetch_sov_map(self) -> tuple[dict[int, dict], str | None]:
        session = await self._get_session()
        try:
            async with session.get(f"{ESI_BASE}/sovereignty/map/") as resp:
                resp.raise_for_status()
                data = await resp.json()
                expires_header = resp.headers.get("Expires")
        except aiohttp.ClientError as exc:
            raise RuntimeError(f"ESI sovereignty/map request failed: {exc!r}") from exc
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
            return None
        raw: dict[str, dict] = json.loads(cached)
        return {int(k): v for k, v in raw.items()}


esi_client = EsiClient()
