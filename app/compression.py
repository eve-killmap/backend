import asyncio
import gzip

GZIP_MIN_SIZE = 1000


async def gzip_if_large(raw: bytes) -> tuple[bytes, bool]:
    if len(raw) < GZIP_MIN_SIZE:
        return raw, False
    return await asyncio.to_thread(gzip.compress, raw, 6), True
