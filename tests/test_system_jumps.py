import asyncio

import pytest
from fastapi import HTTPException

import app.routers.stats as stats
import app.routers.systems as systems
from app.config import config


def _patch_feed(monkeypatch, value):
    async def fake_get_cached(_feed):
        return value

    monkeypatch.setattr(systems.esi_client, "get_cached", fake_get_cached)
    monkeypatch.setattr(stats.esi_client, "get_cached", fake_get_cached)


def test_system_jumps_returns_count(monkeypatch):
    _patch_feed(monkeypatch, {30000142: 42})
    resp = asyncio.run(systems.get_system_jumps(30000142, if_none_match=None))
    assert resp.status_code == 200
    assert resp.body == b'{"jumps":42}'
    assert (
        resp.headers["Cache-Control"]
        == f"public, max-age={config.cache.system_jumps_max_age}"
    )


def test_system_jumps_absent_system_returns_zero(monkeypatch):
    _patch_feed(monkeypatch, {30000142: 42})
    resp = asyncio.run(systems.get_system_jumps(30002187, if_none_match=None))
    assert resp.body == b'{"jumps":0}'


def test_system_jumps_warming_returns_503(monkeypatch):
    _patch_feed(monkeypatch, None)
    with pytest.raises(HTTPException) as e:
        asyncio.run(systems.get_system_jumps(30000142, if_none_match=None))
    assert e.value.status_code == 503


def test_global_jumps_is_index_aligned_and_sorted(monkeypatch):
    _patch_feed(monkeypatch, {30002187: 4, 30000142: 9})

    async def fake_get(prefix, params):
        return None

    captured = {}

    async def fake_set(prefix, params, value, ttl=None):
        captured["prefix"] = prefix
        captured["value"] = value
        return ('"e"', False, value.encode())

    monkeypatch.setattr(stats.query_cache, "get", fake_get)
    monkeypatch.setattr(stats.query_cache, "set", fake_set)
    resp = asyncio.run(stats.get_system_jumps_stats(if_none_match=None))

    assert resp.status_code == 200
    assert captured["prefix"] == "system_jumps"
    assert '"system_ids":[30000142,30002187]' in captured["value"]
    assert '"jumps":[9,4]' in captured["value"]


def test_global_jumps_warming_returns_503(monkeypatch):
    _patch_feed(monkeypatch, None)

    async def fake_get(prefix, params):
        return None

    monkeypatch.setattr(stats.query_cache, "get", fake_get)
    with pytest.raises(HTTPException) as e:
        asyncio.run(stats.get_system_jumps_stats(if_none_match=None))
    assert e.value.status_code == 503
