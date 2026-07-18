import asyncio
import json

import pytest
from fastapi import HTTPException

import app.main as main
from app.models import RawKillDetailResponse


def test_processed_cached_bytes_have_no_null_fields(monkeypatch):
    captured: dict = {}

    async def fake_get(prefix, params):
        return None

    async def fake_set(prefix, params, value, ttl=None):
        captured["value"] = value
        return '"e"', False, value.encode()

    # Minimal raw kill with a character victim so name resolution has work but
    # war_info/faction fields stay None → must be excluded.
    from app.models import KillDetail, Victim, Attacker
    from datetime import datetime, timezone

    kill = KillDetail(
        killmail_id=9,
        killmail_time=datetime(2026, 1, 1, tzinfo=timezone.utc),
        position=(0.0, 0.0, 0.0),
        war_id=None,
        victim=Victim(
            character_id=1, corporation_id=None, alliance_id=None, faction_id=None,
            damage_taken=10, ship_type_id=587,
        ),
        attackers=[
            Attacker(
                character_id=2, corporation_id=None, alliance_id=None, faction_id=None,
                ship_type_id=587, weapon_type_id=None, damage_done=10,
                final_blow=True, security_status=0.0,
            )
        ],
        inserted_time=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )

    async def fake_fetch(ids):
        return RawKillDetailResponse(count=1, kills=[kill])

    async def fake_names(*a, **k):
        return ({1: "Vic", 2: "Atk"}, {}, {}, {})

    async def fake_types(ids):
        return {587: "Rifter"}

    monkeypatch.setattr(main.query_cache, "get", fake_get)
    monkeypatch.setattr(main.query_cache, "set", fake_set)
    monkeypatch.setattr(main, "fetch_kills_by_ids", fake_fetch)
    monkeypatch.setattr(main.entities, "fetch_entity_names", fake_names)
    monkeypatch.setattr(main, "get_type_names", fake_types)

    resp = asyncio.run(main.get_kill_details_processed(9, if_none_match=None))
    assert resp.status_code == 200
    parsed = json.loads(captured["value"])
    # exclude_none: no key anywhere should have a null value
    assert _no_nulls(parsed)
    assert resp.headers["Cache-Control"] == f"public, max-age={main.config.cache.kill_detail_processed_ttl}"
    assert resp.headers["ETag"] == '"e"'


def _no_nulls(obj):
    if isinstance(obj, dict):
        return all(v is not None and _no_nulls(v) for v in obj.values())
    if isinstance(obj, list):
        return all(_no_nulls(v) for v in obj)
    return True


def test_processed_404_when_missing(monkeypatch):
    async def fake_get(prefix, params):
        return None

    async def fake_fetch(ids):
        return RawKillDetailResponse(count=0, kills=[])

    monkeypatch.setattr(main.query_cache, "get", fake_get)
    monkeypatch.setattr(main, "fetch_kills_by_ids", fake_fetch)

    with pytest.raises(HTTPException) as exc:
        asyncio.run(main.get_kill_details_processed(123, if_none_match=None))
    assert exc.value.status_code == 404


def test_processed_serves_from_cache_hit(monkeypatch):
    async def fake_get(prefix, params):
        return '"hit"', False, b'{"attackers":1}'

    monkeypatch.setattr(main.query_cache, "get", fake_get)
    resp = asyncio.run(main.get_kill_details_processed(9, if_none_match=None))
    assert resp.body == b'{"attackers":1}'
    assert resp.headers["ETag"] == '"hit"'


def test_raw_details_sets_etag_and_max_age(monkeypatch):
    from app.models import RawKillDetailResponse

    async def fake_cached(ids):
        return RawKillDetailResponse(count=0, kills=[])

    monkeypatch.setattr(main, "get_kill_details_cached", fake_cached)
    resp = asyncio.run(main.get_kill_details("1,2,3", if_none_match=None))
    assert resp.status_code == 200
    assert resp.headers["Cache-Control"] == f"public, max-age={main.config.cache.kill_detail_ttl}"
    assert resp.headers["ETag"].startswith('"')
    assert "Content-Encoding" not in resp.headers  # compression left to middleware


def test_raw_details_304_on_matching_etag(monkeypatch):
    from app.models import RawKillDetailResponse
    from app.http_cache import compute_etag

    payload = RawKillDetailResponse(count=0, kills=[])

    async def fake_cached(ids):
        return payload

    monkeypatch.setattr(main, "get_kill_details_cached", fake_cached)
    etag = compute_etag(payload.model_dump_json().encode("utf-8"))
    resp = asyncio.run(main.get_kill_details("1", if_none_match=etag))
    assert resp.status_code == 304
