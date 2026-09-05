import gzip

from app.http_cache import (
    binary_cache_response,
    compute_etag,
    json_cache_response,
    not_modified,
)


def test_compute_etag_stable_and_quoted():
    e1 = compute_etag(b"hello")
    assert e1 == compute_etag(b"hello")
    assert e1.startswith('"') and e1.endswith('"')
    assert compute_etag(b"other") != e1


def test_not_modified_matches():
    etag = compute_etag(b"x")
    assert not_modified(etag, etag) is True
    assert not_modified(None, etag) is False
    assert not_modified('"nope"', etag) is False


def test_not_modified_weak_comparison():
    # RFC 7232 §3.2: If-None-Match uses *weak* comparison. A reverse proxy that
    # gzips a response weakens the ETag (nginx sends W/"..."), and the browser
    # then revalidates with that weak tag -- it must still match our strong one.
    assert not_modified('W/"abc"', '"abc"') is True
    assert not_modified('"abc"', '"abc"') is True
    assert not_modified('W/"abc"', 'W/"abc"') is True
    assert not_modified('"nope"', '"abc"') is False


def test_not_modified_list_and_star():
    assert not_modified('"x", W/"abc"', '"abc"') is True  # any tag in the list matches
    assert not_modified("*", '"abc"') is True  # * matches any current representation
    assert not_modified(None, '"abc"') is False


def test_json_cache_response_304_on_weak_if_none_match():
    # End-to-end: the weakened conditional nginx forwards still yields a 304.
    body = b'{"a":1}'
    etag = compute_etag(body)  # strong: '"<md5>"'
    resp = json_cache_response(
        body,
        gzipped=True,
        etag=etag,
        max_age=60,
        if_none_match="W/" + etag,
        revalidate=True,
    )
    assert resp.status_code == 304
    assert resp.headers["ETag"] == etag


# Vary is set by the helper ONLY when it sets Content-Encoding itself. For an
# uncompressed body, GZipMiddleware compresses it downstream and adds its own
# Vary: Accept-Encoding — setting it here too would duplicate the header.
def test_json_cache_response_uncompressed_omits_vary():
    body = b'{"a":1}'
    etag = compute_etag(body)
    resp = json_cache_response(
        body, gzipped=False, etag=etag, max_age=60, if_none_match=None
    )
    assert resp.status_code == 200
    assert resp.body == body
    assert resp.headers["ETag"] == etag
    assert resp.headers["Cache-Control"] == "public, max-age=60"
    assert "Vary" not in resp.headers
    assert "Content-Encoding" not in resp.headers


def test_json_cache_response_gzipped_sets_content_encoding_and_vary():
    raw = b'{"a":1}'
    gz = gzip.compress(raw, 6)
    resp = json_cache_response(
        gz, gzipped=True, etag=compute_etag(raw), max_age=60, if_none_match=None
    )
    assert resp.headers["Content-Encoding"] == "gzip"
    assert resp.headers["Vary"] == "Accept-Encoding"
    assert resp.body == gz


def test_json_cache_response_304_uncompressed_omits_vary():
    body = b'{"a":1}'
    etag = compute_etag(body)
    resp = json_cache_response(
        body, gzipped=False, etag=etag, max_age=60, if_none_match=etag
    )
    assert resp.status_code == 304
    assert resp.headers["ETag"] == etag
    assert resp.headers["Cache-Control"] == "public, max-age=60"
    assert "Vary" not in resp.headers
    assert "Content-Encoding" not in resp.headers


def test_json_cache_response_304_gzipped_carries_vary():
    body = b'{"a":1}'
    etag = compute_etag(body)
    resp = json_cache_response(
        body, gzipped=True, etag=etag, max_age=60, if_none_match=etag
    )
    assert resp.status_code == 304
    assert resp.headers["Vary"] == "Accept-Encoding"
    assert "Content-Encoding" not in resp.headers  # 304 has no body


def test_json_cache_response_revalidate_sends_no_cache():
    # Invalidation-driven endpoints must force the browser to revalidate via
    # ETag, so a server-side cache flush reaches it immediately. max_age is
    # ignored in this mode (a browser max-age would hide the invalidation).
    body = b'{"a":1}'
    etag = compute_etag(body)
    resp = json_cache_response(
        body,
        gzipped=False,
        etag=etag,
        max_age=3600,
        if_none_match=None,
        revalidate=True,
    )
    assert resp.status_code == 200
    assert resp.body == body
    assert resp.headers["ETag"] == etag
    assert resp.headers["Cache-Control"] == "public, no-cache"


def test_json_cache_response_revalidate_304_still_no_cache():
    body = b'{"a":1}'
    etag = compute_etag(body)
    resp = json_cache_response(
        body,
        gzipped=False,
        etag=etag,
        max_age=3600,
        if_none_match=etag,
        revalidate=True,
    )
    assert resp.status_code == 304
    assert resp.headers["Cache-Control"] == "public, no-cache"


def test_binary_cache_response_uncompressed_omits_vary():
    body = b"\x00\x01\x02"
    resp = binary_cache_response(body, gzipped=False, max_age=300, fresh_to=1700000000)
    assert resp.status_code == 200
    assert resp.body == body
    assert resp.headers["X-Kills-Fresh-To"] == "1700000000"
    assert resp.headers["Cache-Control"] == "public, max-age=300"
    assert "Vary" not in resp.headers
    assert "ETag" not in resp.headers
    assert "Content-Encoding" not in resp.headers


def test_binary_cache_response_gzipped_sets_content_encoding_and_vary():
    gz = gzip.compress(b"payload", 6)
    resp = binary_cache_response(gz, gzipped=True, max_age=300, fresh_to=5)
    assert resp.headers["Content-Encoding"] == "gzip"
    assert resp.headers["Vary"] == "Accept-Encoding"
    assert resp.headers["X-Kills-Fresh-To"] == "5"
