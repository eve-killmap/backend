from __future__ import annotations

import hashlib

from fastapi.responses import Response


def compute_etag(body: bytes) -> str:
    return '"' + hashlib.md5(body).hexdigest() + '"'


def not_modified(if_none_match: str | None, etag: str) -> bool:
    return if_none_match is not None and if_none_match.strip() == etag


def json_cache_response(
    body: bytes,
    gzipped: bool,
    etag: str,
    max_age: int,
    if_none_match: str | None,
) -> Response:
    headers = {
        "ETag": etag,
        "Cache-Control": f"public, max-age={max_age}",
        "Vary": "Accept-Encoding",
    }
    if not_modified(if_none_match, etag):
        return Response(status_code=304, headers=headers)
    if gzipped:
        headers["Content-Encoding"] = "gzip"
    return Response(content=body, media_type="application/json", headers=headers)


def binary_cache_response(
    body: bytes,
    gzipped: bool,
    max_age: int,
    fresh_to: int,
) -> Response:
    """Kills full payload: cache-once, pre-compressed, no ETag (see design §4)."""
    headers = {
        "Cache-Control": f"public, max-age={max_age}",
        "Vary": "Accept-Encoding",
        "X-Kills-Fresh-To": str(fresh_to),
    }
    if gzipped:
        headers["Content-Encoding"] = "gzip"
    return Response(content=body, media_type="application/octet-stream", headers=headers)
