import asyncio
import hashlib

import pytest
from fastapi import HTTPException

import app.routers.universe as uni


class _FakeQueryCache:
    """Stores serialized responses like the real QueryCache (no gzip, no Redis)."""

    def __init__(self):
        self.store: dict[str, tuple[str, bool, bytes]] = {}

    async def get(self, prefix, params):
        return self.store.get(prefix)

    async def set(self, prefix, params, value, ttl=None):
        raw = value.encode("utf-8")
        etag = '"' + hashlib.md5(raw).hexdigest() + '"'
        res = (etag, False, raw)
        self.store[prefix] = res
        return res


class _FakeEsi:
    def __init__(self, sov_map, adm):
        self._sov_map = sov_map
        self._adm = adm
        self.map_calls = 0

    async def get_sov_map_cached(self):
        self.map_calls += 1
        return self._sov_map

    async def get_sov_structures_cached(self):
        return self._adm


async def _names(chars, corp_ids, alliance_ids, faction_ids, **k):
    # Resolve every requested alliance from the "DB" so no ESI fallback fires here.
    return {}, {}, {aid: (f"Alliance {aid}", "TCK") for aid in alliance_ids}, {}


def _patch(monkeypatch, *, sov_map, adm, query_cache=None):
    qc = query_cache or _FakeQueryCache()
    esi = _FakeEsi(sov_map, adm)
    monkeypatch.setattr(uni, "query_cache", qc)
    monkeypatch.setattr(uni, "esi_client", esi)
    monkeypatch.setattr(uni.entities, "fetch_entity_names", _names)
    return qc, esi


def test_warming_up_returns_503(monkeypatch):
    _patch(monkeypatch, sov_map=None, adm=None)
    with pytest.raises(HTTPException) as e:
        asyncio.run(uni.get_sovereignty_map(if_none_match=None))
    assert e.value.status_code == 503


def test_adm_feed_absent_yields_adm_available_false(monkeypatch):
    _patch(monkeypatch, sov_map={10: {"alliance_id": 99}}, adm=None)
    resp = asyncio.run(uni.get_sovereignty_map(if_none_match=None))
    assert resp.status_code == 200
    assert b'"adm_available":false' in resp.body
    assert (
        resp.headers["Cache-Control"]
        == f"public, max-age={uni.config.cache.sov_map_ttl}"
    )


def test_cache_hit_does_not_touch_esi(monkeypatch):
    qc = _FakeQueryCache()
    qc.store["sov_map"] = ('"deadbeef"', False, b'{"updated_at":1}')
    _, esi = _patch(
        monkeypatch, sov_map={10: {"alliance_id": 99}}, adm={}, query_cache=qc
    )
    resp = asyncio.run(uni.get_sovereignty_map(if_none_match=None))
    assert resp.status_code == 200
    assert esi.map_calls == 0  # served from cache


def test_single_flight_collapses_concurrent_misses(monkeypatch):
    _, esi = _patch(monkeypatch, sov_map={10: {"alliance_id": 99}}, adm={})

    async def go():
        return await asyncio.gather(
            *[uni.get_sovereignty_map(if_none_match=None) for _ in range(8)]
        )

    resps = asyncio.run(go())
    assert all(r.status_code == 200 for r in resps)
    assert esi.map_calls == 1  # only one build under the single-flight lock


def test_if_none_match_returns_304(monkeypatch):
    _patch(monkeypatch, sov_map={10: {"alliance_id": 99}}, adm={})
    first = asyncio.run(uni.get_sovereignty_map(if_none_match=None))
    etag = first.headers["ETag"]
    second = asyncio.run(uni.get_sovereignty_map(if_none_match=etag))
    assert second.status_code == 304
    assert second.headers["ETag"] == etag
    assert "Cache-Control" in second.headers


def test_resolve_owner_names_db_then_esi_fallback(monkeypatch):
    # DB resolves the corp; the alliance is missing from the DB and falls back to ESI.
    async def fake_fetch(chars, corp_ids, alliance_ids, faction_ids, **k):
        return {}, {98: ("Some Corp", "SCORP")}, {}, {}  # corp only, alliance missing

    monkeypatch.setattr(uni.entities, "fetch_entity_names", fake_fetch)

    class _EsiNames:
        async def get_alliance_info(self, oid):
            return ("Goonswarm Federation", "CONDI")

        async def get_corporation_info(self, oid):
            raise AssertionError("corp already resolved from DB; no ESI expected")

    monkeypatch.setattr(uni, "esi_client", _EsiNames())

    sov_map = {10: {"alliance_id": 99}, 20: {"corporation_id": 98}}
    lookups = asyncio.run(uni._resolve_owner_names(sov_map))
    assert lookups[(0, 99)] == ("Goonswarm Federation", "CONDI")  # ESI fallback
    assert lookups[(1, 98)] == ("Some Corp", "SCORP")  # from DB
