import asyncio
import json
import logging
import time

import pytest
from prometheus_client import REGISTRY

import app.esi as esi_mod
from app import redis_client as rc
from app.esi import EsiFeed, EsiFeedRefreshError, EsiTransientError, ttl_from_expires


def _sample(name, labels):
    return REGISTRY.get_sample_value(name, labels) or 0.0


def _refreshes(feed_name, outcome):
    return _sample(
        "eve_killmap_esi_feed_refreshes_total",
        {"feed": feed_name, "outcome": outcome},
    )


def _requests(feed_name, outcome):
    return _sample(
        "eve_killmap_esi_requests_total",
        {"endpoint": feed_name, "outcome": outcome},
    )


class _FakeRedis:
    def __init__(self):
        self.store: dict[str, str] = {}
        self.expires: dict[str, int] = {}
        self.published: list[tuple[str, str]] = []

    async def set(self, key, value, ex=None):
        self.store[key] = value
        self.expires[key] = ex

    async def get(self, key):
        return self.store.get(key)

    async def publish(self, channel, data):
        self.published.append((channel, data))


class _FakeResponse:
    def __init__(self, headers, payload=None):
        self.headers = headers
        self._payload = payload if payload is not None else {"n": 1}

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    def raise_for_status(self):
        pass

    async def json(self):
        return self._payload


class _FakeSession:
    def __init__(self, response):
        self._response = response
        self.requests: list[str] = []

    def get(self, url):
        self.requests.append(url)
        return self._response


def _limit_headers(remain=None, reset=None) -> dict[str, str]:
    headers = {}
    if remain is not None:
        headers["X-ESI-Error-Limit-Remain"] = remain
    if reset is not None:
        headers["X-ESI-Error-Limit-Reset"] = reset
    return headers


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
    feed = _feed(name="probe_ok", store_ttl=lambda: 600)
    client = esi_mod.EsiClient()
    fake = _FakeRedis()
    client._redis = fake

    async def fake_fetch(_feed):
        return {"n": 7}, None

    monkeypatch.setattr(client, "_fetch_json", fake_fetch)
    before = _refreshes("probe_ok", "ok")
    ttl, value = asyncio.run(client.refresh(feed))

    assert value == {"n": 7}
    assert json.loads(fake.store["esi:probe"]) == {"n": 7}
    assert ttl == 300  # ESI ttl drives cadence
    assert fake.expires["esi:probe"] == 600  # store_ttl drives retention
    assert _refreshes("probe_ok", "ok") == before + 1
    assert _refreshes("probe_ok", "error") == 0


def test_refresh_offline_value_on_transient_error(monkeypatch):
    feed = _feed(
        name="probe_offline", offline_value={"online": False}, store_ttl=lambda: 600
    )
    client = esi_mod.EsiClient()
    fake = _FakeRedis()
    client._redis = fake

    async def boom(_feed):
        raise EsiTransientError("503")

    monkeypatch.setattr(client, "_fetch_json", boom)
    before = _refreshes("probe_offline", "offline")
    ttl, value = asyncio.run(client.refresh(feed))

    assert value == {"online": False}
    assert json.loads(fake.store["esi:probe"]) == {"online": False}
    assert ttl == 300  # cadence unchanged; not a failure
    assert _refreshes("probe_offline", "offline") == before + 1
    # an offline cluster is a normal reading, not a refresh failure
    assert _refreshes("probe_offline", "error") == 0
    assert _refreshes("probe_offline", "ok") == 0


def test_refresh_optional_feed_degrades(monkeypatch):
    feed = _feed(name="probe_degraded", required=False)
    client = esi_mod.EsiClient()
    client._redis = _FakeRedis()

    async def boom(_feed):
        raise EsiTransientError("503")

    monkeypatch.setattr(client, "_fetch_json", boom)
    before = _refreshes("probe_degraded", "degraded")
    with pytest.raises(EsiFeedRefreshError):
        asyncio.run(client.refresh(feed))

    assert _refreshes("probe_degraded", "degraded") == before + 1
    # a degraded optional feed must not page anyone as an error
    assert _refreshes("probe_degraded", "error") == 0


def test_refresh_required_feed_raises(monkeypatch):
    feed = _feed(name="probe_error")
    client = esi_mod.EsiClient()
    client._redis = _FakeRedis()

    async def boom(_feed):
        raise RuntimeError("schema change")

    monkeypatch.setattr(client, "_fetch_json", boom)
    before = _refreshes("probe_error", "error")
    with pytest.raises(EsiFeedRefreshError):
        asyncio.run(client.refresh(feed))

    assert _refreshes("probe_error", "error") == before + 1
    assert _refreshes("probe_error", "degraded") == 0


def test_refresh_required_feed_transient_error_is_not_degraded(monkeypatch):
    """`required` -- not the error's transience -- decides degraded vs error."""
    feed = _feed(name="probe_required_transient")
    client = esi_mod.EsiClient()
    client._redis = _FakeRedis()

    async def boom(_feed):
        raise EsiTransientError("503")

    monkeypatch.setattr(client, "_fetch_json", boom)
    before = _refreshes("probe_required_transient", "error")
    with pytest.raises(EsiFeedRefreshError):
        asyncio.run(client.refresh(feed))

    assert _refreshes("probe_required_transient", "error") == before + 1
    assert _refreshes("probe_required_transient", "degraded") == 0


def test_refresh_once_records_last_success_timestamp(monkeypatch):
    """The freshness gauge the alerting relies on is set on the success path."""
    feed = _feed(name="probe_fresh")
    client = esi_mod.EsiClient()
    client._redis = _FakeRedis()

    async def fake_fetch(_feed):
        return {"n": 1}, None

    monkeypatch.setattr(client, "_fetch_json", fake_fetch)
    monkeypatch.setattr(rc, "esi_client", client)
    broadcaster = rc.KillBroadcaster()
    asyncio.run(broadcaster._esi_refresh_once(feed))

    assert _sample(
        "eve_killmap_esi_feed_last_success_timestamp_seconds", {"feed": "probe_fresh"}
    ) == pytest.approx(time.time(), abs=60)


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


def test_broadcast_publishes_the_decoded_value(monkeypatch):
    """The connect snapshot (get_cached) and the broadcast frames must agree; a feed
    with a non-identity decode would otherwise ship two different key types."""
    feed = _feed(
        name="probe_broadcast",
        decode=lambda raw: {**raw, "decoded": True},
        broadcast_channel="probe:broadcast",
    )
    client = esi_mod.EsiClient()
    fake = _FakeRedis()
    client._redis = fake

    async def fake_fetch(_feed):
        return {"n": 3}, None

    monkeypatch.setattr(client, "_fetch_json", fake_fetch)
    monkeypatch.setattr(rc, "esi_client", client)
    broadcaster = rc.KillBroadcaster()
    broadcaster._redis = fake
    asyncio.run(broadcaster._esi_refresh_once(feed))

    channel, data = fake.published[0]
    assert channel == "probe:broadcast"
    assert json.loads(data) == {"n": 3, "decoded": True}
    assert json.loads(data) == asyncio.run(client.get_cached(feed))


def test_request_success_is_recorded_even_when_the_transform_fails(monkeypatch):
    """esi_requests describes the HTTP call, so a transform that raises afterwards
    must not erase the request's success -- the same split corporation/alliance use."""

    def boom(_data):
        raise KeyError("players")

    feed = _feed(name="probe_transform", transform=boom)
    client = esi_mod.EsiClient()
    client._redis = _FakeRedis()
    session = _FakeSession(_FakeResponse({}))

    async def fake_get_session():
        return session

    monkeypatch.setattr(client, "_get_session", fake_get_session)
    before = _requests("probe_transform", "ok")
    with pytest.raises(KeyError):
        asyncio.run(client.refresh(feed))

    assert _requests("probe_transform", "ok") == before + 1
    assert _requests("probe_transform", "error") == 0  # the request itself was fine
    # The refresh never completed, so it must not count as a successful one.
    assert _refreshes("probe_transform", "ok") == 0


def test_error_limit_headers_set_the_remain_gauge():
    client = esi_mod.EsiClient()
    client._record_error_limit(_limit_headers(remain="87"))

    assert REGISTRY.get_sample_value("eve_killmap_esi_error_limit_remain") == 87
    assert client._error_limit_reset_at == 0.0  # headroom left: no cooldown


def test_error_limit_reset_is_clamped_to_the_window(monkeypatch):
    """An absurd Reset would otherwise park every feed on this client for its whole
    span, since the cooldown is client-wide and waited on before every request."""
    monkeypatch.setattr(esi_mod.time, "monotonic", lambda: 1000.0)
    assert esi_mod.ESI_ERROR_LIMIT_MAX_RESET == 60  # ESI's actual window

    client = esi_mod.EsiClient()
    client._record_error_limit(_limit_headers(remain="1", reset="86400"))
    assert client._error_limit_reset_at == 1060.0

    # a reset inside the window is honored verbatim, not flattened to the ceiling
    plausible = esi_mod.EsiClient()
    plausible._record_error_limit(_limit_headers(remain="1", reset="12"))
    assert plausible._error_limit_reset_at == 1012.0


@pytest.mark.parametrize(
    "remain,reset",
    [
        ("2", "soon"),
        ("2", None),
        ("2", ""),
        ("2", "-90"),
        ("nope", "30"),
        (None, "30"),
    ],
)
def test_error_limit_malformed_headers_set_no_deadline(monkeypatch, remain, reset):
    monkeypatch.setattr(esi_mod.time, "monotonic", lambda: 1000.0)
    client = esi_mod.EsiClient()
    client._record_error_limit(_limit_headers(remain=remain, reset=reset))
    assert client._error_limit_reset_at == 0.0


def test_error_limit_warns_once_when_the_cooldown_engages(monkeypatch, caplog):
    monkeypatch.setattr(esi_mod.time, "monotonic", lambda: 1000.0)
    client = esi_mod.EsiClient()
    with caplog.at_level(logging.WARNING, logger="app.esi"):
        client._record_error_limit(_limit_headers(remain="3", reset="86400"))
        client._record_error_limit(_limit_headers(remain="2", reset="86400"))

    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 1  # once per engagement, not once per response
    message = warnings[0].getMessage()
    assert "3" in message and "60" in message  # remaining errors and clamped reset


def test_error_limit_cooldown_delays_the_next_request(monkeypatch):
    slept: list[float] = []

    async def fake_sleep(delay):
        slept.append(delay)

    session = _FakeSession(_FakeResponse(_limit_headers(remain="1", reset="86400")))

    async def fake_get_session():
        return session

    client = esi_mod.EsiClient()
    monkeypatch.setattr(client, "_get_session", fake_get_session)
    monkeypatch.setattr(esi_mod.asyncio, "sleep", fake_sleep)
    feed = _feed(name="probe_cooldown")

    asyncio.run(client._fetch_json(feed))
    assert slept == []  # nothing to wait for until the limit is known

    asyncio.run(client._fetch_json(feed))
    assert len(slept) == 1
    assert slept[0] == pytest.approx(esi_mod.ESI_ERROR_LIMIT_MAX_RESET, abs=1)
