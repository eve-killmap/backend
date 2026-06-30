from app.http_cache import compute_etag, not_modified, json_cache_response


def test_compute_etag_stable_and_quoted():
    e1 = compute_etag(b"hello")
    e2 = compute_etag(b"hello")
    assert e1 == e2
    assert e1.startswith('"') and e1.endswith('"')
    assert compute_etag(b"other") != e1


def test_not_modified_matches():
    etag = compute_etag(b"x")
    assert not_modified(etag, etag) is True
    assert not_modified(None, etag) is False
    assert not_modified('"nope"', etag) is False


def test_json_cache_response_304(monkeypatch):
    body = '{"a":1}'
    etag = compute_etag(body.encode())
    resp = json_cache_response(body, max_age=60, if_none_match=etag)
    assert resp.status_code == 304
    full = json_cache_response(body, max_age=60, if_none_match=None)
    assert full.status_code == 200
    assert full.headers["Cache-Control"] == "public, max-age=60"
    assert full.headers["Vary"] == "Accept-Encoding"
    assert full.headers["ETag"] == etag
