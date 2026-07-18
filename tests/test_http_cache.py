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


def test_json_cache_response_headers_uncompressed():
    body = b'{"a":1}'
    etag = compute_etag(body)
    resp = json_cache_response(body, gzipped=False, etag=etag, max_age=60, if_none_match=None)
    assert resp.status_code == 200
    assert resp.body == body
    assert resp.headers["ETag"] == etag
    assert resp.headers["Cache-Control"] == "public, max-age=60"
    assert resp.headers["Vary"] == "Accept-Encoding"
    assert "Content-Encoding" not in resp.headers


def test_json_cache_response_sets_content_encoding_when_gzipped():
    raw = b'{"a":1}'
    gz = gzip.compress(raw, 6)
    resp = json_cache_response(gz, gzipped=True, etag=compute_etag(raw), max_age=60, if_none_match=None)
    assert resp.headers["Content-Encoding"] == "gzip"
    assert resp.body == gz


def test_json_cache_response_304_carries_cache_headers():
    body = b'{"a":1}'
    etag = compute_etag(body)
    resp = json_cache_response(body, gzipped=False, etag=etag, max_age=60, if_none_match=etag)
    assert resp.status_code == 304
    assert resp.headers["ETag"] == etag
    assert resp.headers["Cache-Control"] == "public, max-age=60"
    assert resp.headers["Vary"] == "Accept-Encoding"
    assert "Content-Encoding" not in resp.headers


def test_binary_cache_response_has_fresh_to_and_no_etag():
    body = b"\x00\x01\x02"
    resp = binary_cache_response(body, gzipped=False, max_age=300, fresh_to=1700000000)
    assert resp.status_code == 200
    assert resp.body == body
    assert resp.headers["X-Kills-Fresh-To"] == "1700000000"
    assert resp.headers["Cache-Control"] == "public, max-age=300"
    assert resp.headers["Vary"] == "Accept-Encoding"
    assert "ETag" not in resp.headers
    assert "Content-Encoding" not in resp.headers


def test_binary_cache_response_content_encoding_when_gzipped():
    gz = gzip.compress(b"payload", 6)
    resp = binary_cache_response(gz, gzipped=True, max_age=300, fresh_to=5)
    assert resp.headers["Content-Encoding"] == "gzip"
    assert resp.headers["X-Kills-Fresh-To"] == "5"
