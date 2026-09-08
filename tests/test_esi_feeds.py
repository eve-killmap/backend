import asyncio
import json

import pytest

import app.esi as esi_mod
from app.esi import EsiFeed, EsiFeedRefreshError, EsiTransientError, ttl_from_expires


class _FakeRedis:
    def __init__(self):
        self.store: dict[str, str] = {}
        self.expires: dict[str, int] = {}

    async def set(self, key, value, ex=None):
        self.store[key] = value
        self.expires[key] = ex

    async def get(self, key):
        return self.store.get(key)


def _feed(**overrides) -> EsiFeed:
    base = dict(
        name="probe",
        path="/probe/",
        redis_key="esi:probe",
        fallback_ttl=lambda: 300,
        ttl_floor=15,
        transform=lambda data: {"n": data["n"]},
        decode=lambda raw: raw,
        sleep_skew=2,
        sleep_min=15,
        sleep_max=60,
    )
    base.update(overrides)
    return EsiFeed(**base)


def test_ttl_from_expires_honors_per_feed_floor():
    # A 30s ESI cache must not be clamped up to 60 for a short-lived feed.
    assert ttl_from_expires(None, fallback_seconds=30, floor=15) == 30
    assert ttl_from_expires("bogus", fallback_seconds=30, floor=15) == 30


def test_refresh_stores_transformed_value_with_store_ttl(monkeypatch):
    feed = _feed(store_ttl=lambda: 600)
    client = esi_mod.EsiClient()
    fake = _FakeRedis()
    client._redis = fake

    async def fake_fetch(_feed):
        return {"n": 7}, None

    monkeypatch.setattr(client, "_fetch_json", fake_fetch)
    ttl, value = asyncio.run(client.refresh(feed))

    assert value == {"n": 7}
    assert json.loads(fake.store["esi:probe"]) == {"n": 7}
    assert ttl == 300  # ESI ttl drives cadence
    assert fake.expires["esi:probe"] == 600  # store_ttl drives retention


def test_refresh_offline_value_on_transient_error(monkeypatch):
    feed = _feed(offline_value={"online": False}, store_ttl=lambda: 600)
    client = esi_mod.EsiClient()
    fake = _FakeRedis()
    client._redis = fake

    async def boom(_feed):
        raise EsiTransientError("503")

    monkeypatch.setattr(client, "_fetch_json", boom)
    ttl, value = asyncio.run(client.refresh(feed))

    assert value == {"online": False}
    assert json.loads(fake.store["esi:probe"]) == {"online": False}
    assert ttl == 300  # cadence unchanged; not a failure


def test_refresh_optional_feed_degrades(monkeypatch):
    feed = _feed(required=False)
    client = esi_mod.EsiClient()
    client._redis = _FakeRedis()

    async def boom(_feed):
        raise EsiTransientError("503")

    monkeypatch.setattr(client, "_fetch_json", boom)
    with pytest.raises(EsiFeedRefreshError):
        asyncio.run(client.refresh(feed))


def test_refresh_required_feed_raises(monkeypatch):
    feed = _feed()
    client = esi_mod.EsiClient()
    client._redis = _FakeRedis()

    async def boom(_feed):
        raise RuntimeError("schema change")

    monkeypatch.setattr(client, "_fetch_json", boom)
    with pytest.raises(EsiFeedRefreshError):
        asyncio.run(client.refresh(feed))


def test_get_cached_decodes(monkeypatch):
    feed = _feed(decode=lambda raw: {int(k): v for k, v in raw.items()})
    client = esi_mod.EsiClient()
    fake = _FakeRedis()
    fake.store["esi:probe"] = json.dumps({"30000142": 5})
    client._redis = fake

    assert asyncio.run(client.get_cached(feed)) == {30000142: 5}


def test_get_cached_missing_returns_none():
    client = esi_mod.EsiClient()
    client._redis = _FakeRedis()
    assert asyncio.run(client.get_cached(_feed())) is None
