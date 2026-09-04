import asyncio
import logging
import time
from contextlib import asynccontextmanager
from typing import AsyncGenerator

import asyncpg

from app.config import config, require_database_url
from app.metrics import metrics

logger = logging.getLogger(__name__)

_TRANSIENT_CONNECT_ERRORS = (
    asyncpg.exceptions.CannotConnectNowError,
    OSError,
    asyncio.TimeoutError,
)


class Database:

    def __init__(self):
        self._pool: asyncpg.Pool | None = None

    async def connect(self) -> None:
        self._pool = await self._create_pool_with_retry()

    async def _create_pool_with_retry(self) -> asyncpg.Pool:
        dsn = require_database_url(config)  # non-transient: fail fast if unset
        budget = config.database.connect_max_retry_seconds
        max_delay = config.database.connect_retry_max_delay_seconds
        deadline = time.monotonic() + budget
        delay = 1.0
        attempt = 0
        while True:
            attempt += 1
            try:
                return await asyncpg.create_pool(
                    dsn=dsn,
                    min_size=config.database.pool_min_size,
                    max_size=config.database.pool_max_size,
                )
            except _TRANSIENT_CONNECT_ERRORS as exc:
                if time.monotonic() >= deadline:
                    logger.error(
                        "Database still unreachable after ~%ds (%d attempts); "
                        "giving up: %s",
                        budget,
                        attempt,
                        exc,
                    )
                    raise
                logger.warning(
                    "Database not ready (attempt %d), retrying in %.0fs: %s",
                    attempt,
                    delay,
                    exc,
                )
                await asyncio.sleep(delay)
                delay = min(delay * 2, max_delay)

    async def disconnect(self) -> None:
        if self._pool:
            await self._pool.close()
            self._pool = None

    @asynccontextmanager
    async def connection(
        self,
    ) -> AsyncGenerator[asyncpg.pool.PoolConnectionProxy, None]:
        if not self._pool:
            raise RuntimeError("Database not connected")
        async with self._pool.acquire() as conn:
            yield conn

    async def fetch(self, query: str, *args) -> list[asyncpg.Record]:
        metrics.db_queries += 1
        async with self.connection() as conn:
            return await conn.fetch(query, *args)

    async def fetchrow(self, query: str, *args) -> asyncpg.Record | None:
        metrics.db_queries += 1
        async with self.connection() as conn:
            return await conn.fetchrow(query, *args)

    async def fetchval(self, query: str, *args):
        metrics.db_queries += 1
        async with self.connection() as conn:
            return await conn.fetchval(query, *args)

    def pool_stats(self) -> dict:
        if self._pool is None:
            return {"connected": False}
        return {
            "connected": True,
            "size": self._pool.get_size(),
            "idle": self._pool.get_idle_size(),
            "min_size": self._pool.get_min_size(),
            "max_size": self._pool.get_max_size(),
        }

    async def is_healthy(self) -> bool:
        try:
            await self.fetchval("SELECT 1")
            return True
        except Exception:
            return False


db = Database()
