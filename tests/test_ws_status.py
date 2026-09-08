import asyncio
import json

import pytest

import app.routers.ws as ws_router
from app import redis_client as rc
from app.metrics import metrics
from app.redis_client import broadcaster


def test_status_subscribe_receives_broadcast():
    q = broadcaster.subscribe_status()
    try:
        broadcaster._fanout_status({"online": True, "players": 12})
        assert q.get_nowait() == {"online": True, "players": 12}
    finally:
        broadcaster.unsubscribe_status(q)


def test_status_unsubscribe_stops_delivery():
    q = broadcaster.subscribe_status()
    broadcaster.unsubscribe_status(q)
    broadcaster._fanout_status({"online": False})
    assert q.empty()


def test_status_fanout_does_not_touch_kill_subscribers():
    kq = broadcaster.subscribe_global()
    sq = broadcaster.subscribe_status()
    try:
        broadcaster._fanout_status({"online": False})
        assert kq.empty()
        assert sq.get_nowait() == {"online": False}
    finally:
        broadcaster.unsubscribe_global(kq)
        broadcaster.unsubscribe_status(sq)


def _count_fanouts(monkeypatch) -> dict[str, int]:
    seen = {"kills": 0, "status": 0}
    monkeypatch.setattr(
        rc.broadcaster, "_fanout", lambda p: seen.__setitem__("kills", seen["kills"] + 1)
    )
    monkeypatch.setattr(
        rc.broadcaster,
        "_fanout_status",
        lambda p: seen.__setitem__("status", seen["status"] + 1),
    )
    return seen


def test_dispatch_routes_by_channel(monkeypatch):
    seen = _count_fanouts(monkeypatch)
    rc.broadcaster._dispatch(rc.config.streaming.pubsub_channel, {"solar_system_id": 1})
    rc.broadcaster._dispatch(rc.config.streaming.status_channel, {"online": True})
    assert seen == {"kills": 1, "status": 1}


def test_dispatch_drops_unknown_channel(monkeypatch):
    """Routing must fail closed. An unrecognized payload reaching the kill fan-out
    would raise on its missing solar_system_id, and the subscriber loop does not
    restart, so delivery would end for the kill and status sockets alike."""
    seen = _count_fanouts(monkeypatch)
    rc.broadcaster._dispatch(rc.config.streaming.invalidate_channel, {"targets": []})
    assert seen == {"kills": 0, "status": 0}


class _FakePubSub:
    """Replays a fixed list of pubsub messages, then ends the listen() stream."""

    def __init__(self, messages):
        self._messages = messages
        self.subscribed: tuple = ()
        self.unsubscribed: tuple = ()

    async def subscribe(self, *channels):
        self.subscribed = channels

    async def unsubscribe(self, *channels):
        self.unsubscribed = channels

    async def aclose(self):
        pass

    async def listen(self):
        for m in self._messages:
            yield m


class _FakePubSubRedis:
    def __init__(self, pubsub):
        self._pubsub = pubsub

    def pubsub(self):
        return self._pubsub


def test_subscriber_loop_subscribes_both_channels_and_routes_each(monkeypatch):
    """End-to-end: the live loop must subscribe to both channels and route each
    message to the matching fan-out, keeping the killstream isolated from status."""
    kill_channel = rc.config.streaming.pubsub_channel
    status_channel = rc.config.streaming.status_channel
    pubsub = _FakePubSub(
        [
            {"type": "subscribe", "channel": kill_channel, "data": 1},
            {
                "type": "message",
                "channel": kill_channel,
                "data": json.dumps({"solar_system_id": 30000142, "killmail_id": 7}),
            },
            {
                "type": "message",
                "channel": status_channel,
                "data": json.dumps({"online": True, "players": 3}),
            },
        ]
    )

    b = rc.KillBroadcaster()
    b._redis = _FakePubSubRedis(pubsub)
    kq = b.subscribe_global()
    sq = b.subscribe_status()
    try:
        asyncio.run(b._subscriber_loop())

        assert set(pubsub.subscribed) == {kill_channel, status_channel}
        assert set(pubsub.unsubscribed) == {kill_channel, status_channel}
        assert kq.get_nowait()["killmail_id"] == 7
        assert kq.empty()  # the status message never reached a kill subscriber
        assert sq.get_nowait() == {"online": True, "players": 3}
        assert sq.empty()  # the kill message never reached a status subscriber
    finally:
        b.unsubscribe_global(kq)
        b.unsubscribe_status(sq)


class _FakeWS:
    def __init__(self):
        self.headers = {"origin": "http://localhost"}
        self.sent: list[str] = []
        self.accepted = False
        self.closed: tuple | None = None

    async def accept(self):
        self.accepted = True

    async def send_text(self, text):
        self.sent.append(text)

    async def receive(self):
        return {"type": "websocket.disconnect"}

    async def close(self, code=1000, reason=""):
        self.closed = (code, reason)


async def _ok_guard(_ws):
    return True


def _patch_status_cache(monkeypatch, value):
    async def fake_cached():
        return value

    monkeypatch.setattr(ws_router.esi_client, "get_status_cached", fake_cached)


def test_ws_status_sends_cached_snapshot_on_connect(monkeypatch):
    monkeypatch.setattr(ws_router, "_ws_guard", _ok_guard)
    _patch_status_cache(monkeypatch, {"online": True, "players": 77})
    sock = _FakeWS()
    asyncio.run(ws_router.ws_universe_status(sock))
    assert '"players":77' in sock.sent[0] or '"players": 77' in sock.sent[0]


def test_ws_status_silent_when_cache_cold(monkeypatch):
    # A cold cache must NOT be reported as the cluster being offline.
    monkeypatch.setattr(ws_router, "_ws_guard", _ok_guard)
    _patch_status_cache(monkeypatch, None)
    sock = _FakeWS()
    asyncio.run(ws_router.ws_universe_status(sock))
    assert sock.sent == []


def test_ws_status_releases_slot_on_abnormal_disconnect(monkeypatch):
    """A socket that dies mid-handshake must still give its slot back, or the
    counter climbs monotonically and eventually trips the capacity guard."""
    monkeypatch.setattr(ws_router, "_ws_guard", _ok_guard)
    _patch_status_cache(monkeypatch, {"online": True, "players": 3})

    class _BrokenWS(_FakeWS):
        async def accept(self):
            raise RuntimeError("connection reset")

    before = metrics.ws_status_connections
    with pytest.raises(RuntimeError):
        asyncio.run(ws_router.ws_universe_status(_BrokenWS()))
    assert metrics.ws_status_connections == before
    assert broadcaster._status_subs == set()


def test_ws_guard_counts_status_sockets_toward_capacity(monkeypatch):
    """A status socket costs a connection slot like any other."""
    monkeypatch.setattr(ws_router, "origin_allowed", lambda *_: True)
    monkeypatch.setattr(
        ws_router.metrics,
        "ws_status_connections",
        ws_router.config.limits.max_ws_connections,
    )
    sock = _FakeWS()
    assert asyncio.run(ws_router._ws_guard(sock)) is False
    assert sock.closed == (1013, "Server at capacity")
