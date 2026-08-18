import asyncio

from app.esi import EsiClient, _reduce_sov_structures, ttl_from_expires


def test_reduce_skips_null_level():
    data = [
        {
            "solar_system_id": 30000142,
            "vulnerability_occupancy_level": None,
            "structure_type_id": 32458,
        },
        {
            "solar_system_id": 30000144,
            "vulnerability_occupancy_level": 3.0,
            "structure_type_id": 32458,
        },
    ]
    assert _reduce_sov_structures(data) == {30000144: 3.0}


def test_reduce_takes_max_regardless_of_order():
    fwd = [
        {
            "solar_system_id": 1,
            "vulnerability_occupancy_level": 2.0,
            "structure_type_id": 32458,
        },
        {
            "solar_system_id": 1,
            "vulnerability_occupancy_level": 5.0,
            "structure_type_id": 32458,
        },
    ]
    assert _reduce_sov_structures(fwd) == {1: 5.0}
    assert _reduce_sov_structures(list(reversed(fwd))) == {1: 5.0}


def test_reduce_does_not_filter_by_structure_type():
    # A non-IHub structure still contributes ADM (Verite filters only for ownership).
    data = [
        {
            "solar_system_id": 7,
            "vulnerability_occupancy_level": 4.0,
            "structure_type_id": 99999,
        }
    ]
    assert _reduce_sov_structures(data) == {7: 4.0}


def test_ttl_from_expires_header_then_fallback():
    assert ttl_from_expires(None, 3600) == 3600
    assert ttl_from_expires("not-a-date", 1234) == 1234
    assert ttl_from_expires("Wed, 21 Oct 2099 07:28:00 GMT", 3600) >= 60


def test_refresh_stores_under_expires_ttl(monkeypatch):
    client = EsiClient()

    async def fake_fetch():
        return {30000142: 6.0}, "Wed, 21 Oct 2099 07:28:00 GMT"

    captured = {}

    class _FakeRedis:
        async def set(self, key, value, ex=None):
            captured["key"] = key
            captured["value"] = value
            captured["ex"] = ex

    monkeypatch.setattr(client, "_fetch_sov_structures", fake_fetch)
    client._redis = _FakeRedis()
    ttl = asyncio.run(client.refresh_sov_structures())
    assert captured["key"] == "esi:sov_structures"
    assert captured["ex"] == ttl and ttl >= 60


def test_refresh_falls_back_when_no_expires(monkeypatch):
    import dataclasses

    from app.config import config as real_config

    client = EsiClient()

    async def fake_fetch():
        return {1: 1.0}, None  # no Expires header

    class _FakeRedis:
        async def set(self, *a, **k):
            pass

    # CacheConfig is a frozen dataclass — build a replacement with a distinctive
    # fallback TTL and point app.esi's config reference at it, proving
    # refresh_sov_structures reads that specific field.
    patched = dataclasses.replace(
        real_config,
        cache=dataclasses.replace(
            real_config.cache, esi_sov_structures_fallback_ttl=4242
        ),
    )
    monkeypatch.setattr("app.esi.config", patched)
    monkeypatch.setattr(client, "_fetch_sov_structures", fake_fetch)
    client._redis = _FakeRedis()
    assert asyncio.run(client.refresh_sov_structures()) == 4242


def test_get_cached_parses_and_misses():
    client = EsiClient()
    client._redis = None
    assert asyncio.run(client.get_sov_structures_cached()) is None

    class _R:
        def __init__(self, v):
            self._v = v

        async def get(self, k):
            return self._v

    client._redis = _R('{"30000142": 6.0}')
    assert asyncio.run(client.get_sov_structures_cached()) == {30000142: 6.0}

    client._redis = _R(None)  # key absent -> feed absent
    assert asyncio.run(client.get_sov_structures_cached()) is None
