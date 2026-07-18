import asyncio
import gzip

GZIP_MIN_SIZE = 1000


async def gzip_if_large(raw: bytes) -> tuple[bytes, bool]:
    """Gzip off the event loop when raw is >= GZIP_MIN_SIZE.

    Returns (body, gzipped). zlib releases the GIL, so to_thread genuinely
    unblocks the event loop while compressing large payloads.
    """
    if len(raw) < GZIP_MIN_SIZE:
        return raw, False
    return await asyncio.to_thread(gzip.compress, raw, 6), True
