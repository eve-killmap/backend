from __future__ import annotations

import hashlib

from fastapi.responses import Response


def compute_etag(body: bytes) -> str:
    return '"' + hashlib.md5(body).hexdigest() + '"'


def not_modified(if_none_match: str | None, etag: str) -> bool:
    return if_none_match is not None and if_none_match.strip() == etag


def _headers(etag: str, max_age: int) -> dict[str, str]:
    return {
        "ETag": etag,
        "Cache-Control": f"public, max-age={max_age}",
    }


def json_cache_response(body: str, max_age: int, if_none_match: str | None) -> Response:
    raw = body.encode("utf-8")
    etag = compute_etag(raw)
    if not_modified(if_none_match, etag):
        return Response(status_code=304, headers=_headers(etag, max_age))
    return Response(
        content=raw,
        media_type="application/json",
        headers=_headers(etag, max_age),
    )


def binary_cache_response(
    body: bytes, max_age: int, if_none_match: str | None
) -> Response:
    etag = compute_etag(body)
    if not_modified(if_none_match, etag):
        return Response(status_code=304, headers=_headers(etag, max_age))
    return Response(
        content=body,
        media_type="application/octet-stream",
        headers=_headers(etag, max_age),
    )
