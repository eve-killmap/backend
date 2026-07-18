import asyncio
import gzip

from app.compression import GZIP_MIN_SIZE, gzip_if_large


def test_large_payload_is_gzipped_and_roundtrips():
    raw = b"x" * (GZIP_MIN_SIZE + 500)
    body, gzipped = asyncio.run(gzip_if_large(raw))
    assert gzipped is True
    assert body != raw
    assert gzip.decompress(body) == raw


def test_small_payload_passes_through():
    raw = b"y" * (GZIP_MIN_SIZE - 1)
    body, gzipped = asyncio.run(gzip_if_large(raw))
    assert gzipped is False
    assert body == raw


def test_threshold_boundary_is_inclusive():
    raw = b"z" * GZIP_MIN_SIZE
    body, gzipped = asyncio.run(gzip_if_large(raw))
    assert gzipped is True
    assert gzip.decompress(body) == raw
