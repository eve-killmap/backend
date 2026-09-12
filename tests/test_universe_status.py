import asyncio

import pytest
from fastapi import HTTPException

import app.routers.universe as universe
from app.esi import STATUS


def _patch_feed(monkeypatch, value):
    async def fake_get_cached(feed):
        assert feed is STATUS
        return value

    monkeypatch.setattr(universe.esi_client, "get_cached", fake_get_cached)


def test_status_returns_online_payload(monkeypatch):
    _patch_feed(monkeypatch, {"online": True, "players": 27412})
    resp = asyncio.run(universe.get_universe_status(if_none_match=None))
    assert resp.status_code == 200
    assert resp.body == b'{"online":true,"players":27412}'
    assert resp.headers["Cache-Control"] == "public, max-age=15"


def test_status_returns_offline_payload_without_players(monkeypatch):
    _patch_feed(monkeypatch, {"online": False})
    resp = asyncio.run(universe.get_universe_status(if_none_match=None))
    assert resp.body == b'{"online":false}'


def test_status_warming_returns_503_not_offline(monkeypatch):
    """A cold cache is "not known yet", not "the cluster is down". Rendering it
    as {"online": false} would show every user "EVE is offline" for as long as a
    restarted worker takes to see the leader's first refresh."""
    _patch_feed(monkeypatch, None)
    with pytest.raises(HTTPException) as e:
        asyncio.run(universe.get_universe_status(if_none_match=None))
    assert e.value.status_code == 503


def test_status_repeat_request_revalidates_to_304(monkeypatch):
    _patch_feed(monkeypatch, {"online": True, "players": 27412})
    first = asyncio.run(universe.get_universe_status(if_none_match=None))
    etag = first.headers["ETag"]

    second = asyncio.run(universe.get_universe_status(if_none_match=etag))

    assert second.status_code == 304
    assert second.body == b""
    assert second.headers["ETag"] == etag
    assert second.headers["Cache-Control"] == "public, max-age=15"


def test_status_etag_changes_with_the_player_count(monkeypatch):
    _patch_feed(monkeypatch, {"online": True, "players": 27412})
    etag = asyncio.run(universe.get_universe_status(if_none_match=None)).headers["ETag"]

    _patch_feed(monkeypatch, {"online": True, "players": 27413})
    stale = asyncio.run(universe.get_universe_status(if_none_match=etag))

    assert stale.status_code == 200
    assert stale.body == b'{"online":true,"players":27413}'
