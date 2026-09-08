import asyncio
import json

import pytest

import app.routers.ws as ws_router
from app import redis_client as rc
from app.esi import STATUS
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
    would raise on its missing solar_system_id, tearing down the subscription and
    stalling the kill and status sockets alike until the reconnect lands."""
    seen = _count_fanouts(monkeypatch)
    rc.broadcaster._dispatch(rc.config.streaming.invalidate_channel, {"targets": []})
    assert seen == {"kills": 0, "status": 0}


class _FakePubSub:
    """Replays a fixed list of pubsub messages, then ends the listen() stream.

    `subscribe_error` fails the subscribe instead. `then`, raised once the messages
    run out, defaults to CancelledError so a test can drive the real subscriber
    loop to a stop; that loop reconnects forever otherwise, exactly as stop() is
    the only thing that ends it in production. Pass then=None to end listen()
    normally and exercise the reconnect.
    """

    def __init__(self, messages=(), then=asyncio.CancelledError, subscribe_error=None):
        self._messages = messages
        self._then = then
        self._subscribe_error = subscribe_error
        self.subscribed: tuple = ()
        self.unsubscribed: tuple = ()

    async def subscribe(self, *channels):
        if self._subscribe_error is not None:
            raise self._subscribe_error
        self.subscribed = channels

    async def unsubscribe(self, *channels):
        self.unsubscribed = channels

    async def aclose(self):
        pass

    async def listen(self):
        for m in self._messages:
            yield m
        if self._then is not None:
            raise self._then


class _FakePubSubRedis:
    """Hands out one pubsub per subscriber attempt, in order; the last repeats."""

    def __init__(self, *pubsubs):
        self._pubsubs = list(pubsubs)
        self.handed_out: list[_FakePubSub] = []

    def pubsub(self):
        ps = self._pubsubs[min(len(self.handed_out), len(self._pubsubs) - 1)]
        self.handed_out.append(ps)
        return ps

    async def aclose(self):
        pass


def _record_sleeps(monkeypatch, stop_after=None):
    """Record backoff delays without ever really sleeping. With `stop_after`, end
    the loop at that many sleeps the way stop()'s cancellation would."""
    delays: list[float] = []

    async def fake_sleep(seconds):
        delays.append(seconds)
        if stop_after is not None and len(delays) >= stop_after:
            raise asyncio.CancelledError

    monkeypatch.setattr(asyncio, "sleep", fake_sleep)
    return delays


def _kill_message(killmail_id):
    return {
        "type": "message",
        "channel": rc.config.streaming.pubsub_channel,
        "data": json.dumps(
            {"solar_system_id": 30000142, "killmail_id": killmail_id}
        ),
    }


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

    async def _no_backoff(_seconds):
        raise AssertionError("cancellation was swallowed into a retry")

    monkeypatch.setattr(rc.asyncio, "sleep", _no_backoff)
    try:
        with pytest.raises(asyncio.CancelledError):
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


def _run_reconnect(monkeypatch, first):
    """Drive the real loop across one reconnect: `first` fails or ends, the second
    attempt delivers a kill. Returns the fake Redis and the kill queue. The sleep
    budget bounds a loop that never reaches the second attempt."""
    _record_sleeps(monkeypatch, stop_after=3)
    second = _FakePubSub([_kill_message(99)])
    b = rc.KillBroadcaster()
    redis = _FakePubSubRedis(first, second)
    b._redis = redis
    kq = b.subscribe_global()
    try:
        with pytest.raises(asyncio.CancelledError):
            asyncio.run(b._subscriber_loop())
    finally:
        b.unsubscribe_global(kq)
    return redis, kq


def _assert_reconnected(redis, kq):
    assert len(redis.handed_out) == 2
    assert redis.handed_out[0] is not redis.handed_out[1]  # a fresh pubsub each time
    assert set(redis.handed_out[1].subscribed) == {
        rc.config.streaming.pubsub_channel,
        rc.config.streaming.status_channel,
    }
    assert kq.get_nowait()["killmail_id"] == 99  # delivery resumed after the gap


def test_subscriber_loop_resubscribes_after_error(monkeypatch):
    """A transient Redis error must not end live delivery for the worker."""
    redis, kq = _run_reconnect(
        monkeypatch, _FakePubSub(then=RuntimeError("connection reset"))
    )
    _assert_reconnected(redis, kq)


def test_subscriber_loop_resubscribes_after_normal_listen_exit(monkeypatch):
    """A cleanly closed pubsub stream is just as fatal to delivery as an error."""
    redis, kq = _run_reconnect(monkeypatch, _FakePubSub(then=None))
    _assert_reconnected(redis, kq)


def test_subscriber_loop_backs_off_instead_of_hot_spinning(monkeypatch):
    """An instantly failing Redis must not spin the worker at 100% CPU."""
    delays = _record_sleeps(monkeypatch, stop_after=10)
    b = rc.KillBroadcaster()
    b._redis = _FakePubSubRedis(_FakePubSub(subscribe_error=RuntimeError("refused")))

    with pytest.raises(asyncio.CancelledError):
        asyncio.run(b._subscriber_loop())

    assert delays[0] == rc._SUBSCRIBER_RETRY_INITIAL
    assert delays[1] == pytest.approx(delays[0] * 2)
    assert delays[2] == pytest.approx(delays[1] * 2)
    assert max(delays) == rc._SUBSCRIBER_RETRY_MAX  # growth is capped
    assert delays[-1] == rc._SUBSCRIBER_RETRY_MAX
    assert b._subscriber_connected is False


def test_subscriber_reconnect_increments_metric(monkeypatch):
    from prometheus_client import REGISTRY

    name = "eve_killmap_broadcaster_subscriber_reconnects_total"
    before = REGISTRY.get_sample_value(name) or 0.0
    _run_reconnect(monkeypatch, _FakePubSub(then=RuntimeError("connection reset")))
    assert (REGISTRY.get_sample_value(name) or 0.0) - before == 1


def test_is_running_false_while_subscriber_is_reconnecting(monkeypatch):
    """The task now outlives an outage, so liveness alone must not report the
    stream as available: a client accepted here would sit in silence."""

    async def scenario():
        backing_off = asyncio.Event()

        async def blocked_sleep(seconds):
            backing_off.set()
            await asyncio.Event().wait()

        monkeypatch.setattr(asyncio, "sleep", blocked_sleep)
        b = rc.KillBroadcaster()
        b._redis = _FakePubSubRedis(_FakePubSub(subscribe_error=RuntimeError("down")))
        b._subscriber_task = asyncio.create_task(b._subscriber_loop())
        await asyncio.wait_for(backing_off.wait(), timeout=1)

        assert b._subscriber_task.done() is False  # the loop is still alive
        assert b.is_running is False  # but not delivering

        b._subscriber_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await b._subscriber_task

    asyncio.run(scenario())


def test_stop_cancels_subscriber_loop_promptly(monkeypatch):
    """The retry loop only ever exits by cancellation; stop() must not hang, and
    must not be delayed by a backoff sleep on the way out."""

    async def scenario():
        listening = asyncio.Event()

        class _BlockingPubSub(_FakePubSub):
            async def listen(self):
                listening.set()
                await asyncio.Event().wait()
                yield {}  # pragma: no cover - makes this an async generator

        b = rc.KillBroadcaster()
        b._redis = _FakePubSubRedis(_BlockingPubSub())
        b._subscriber_task = asyncio.create_task(b._subscriber_loop())
        await asyncio.wait_for(listening.wait(), timeout=1)
        assert b.is_running is True

        async def no_backoff(seconds):
            raise AssertionError("cancellation was swallowed into a retry")

        monkeypatch.setattr(asyncio, "sleep", no_backoff)
        loop = asyncio.get_running_loop()
        started = loop.time()
        await b.stop()

        assert loop.time() - started < 0.5
        assert b._subscriber_task.cancelled()
        assert b.is_running is False

    asyncio.run(scenario())


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


async def _ok_guard(_ws, _transport):
    return True


def _patch_status_cache(monkeypatch, value):
    async def fake_cached(feed):
        assert feed is STATUS
        return value

    monkeypatch.setattr(ws_router.esi_client, "get_cached", fake_cached)


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


def _ws_conn(transport, outcome):
    from prometheus_client import REGISTRY

    return (
        REGISTRY.get_sample_value(
            "eve_killmap_ws_connections_total",
            {"transport": transport, "outcome": outcome},
        )
        or 0.0
    )


def test_status_endpoint_guard_outcomes_are_labeled_ws_status(monkeypatch):
    """live_clients already distinguishes ws_status; the guard counters must too,
    or the status endpoint's accepts and rejections hide inside the kill series."""
    from types import SimpleNamespace

    monkeypatch.setattr(ws_router, "origin_allowed", lambda *_: True)
    monkeypatch.setattr(ws_router, "broadcaster", SimpleNamespace(is_running=True))
    outcomes = ("accepted", "rejected_capacity")
    before = {o: _ws_conn("ws_status", o) for o in outcomes}
    kill_before = {o: _ws_conn("ws", o) for o in outcomes}

    assert asyncio.run(ws_router._ws_guard(_FakeWS(), "ws_status")) is True
    monkeypatch.setattr(
        ws_router.metrics,
        "ws_status_connections",
        ws_router.config.limits.max_ws_connections,
    )
    asyncio.run(ws_router.ws_universe_status(_FakeWS()))  # rejected at capacity

    assert _ws_conn("ws_status", "accepted") - before["accepted"] == 1
    assert _ws_conn("ws_status", "rejected_capacity") - before["rejected_capacity"] == 1
    assert all(_ws_conn("ws", o) - kill_before[o] == 0 for o in outcomes)


def test_kill_endpoint_guard_still_reports_transport_ws(monkeypatch):
    """transport="ws" is the series the dashboards already query: the status socket
    gets a new label rather than renaming or splitting the killstream's."""
    monkeypatch.setattr(ws_router, "origin_allowed", lambda *_: True)
    monkeypatch.setattr(
        ws_router.metrics,
        "ws_global_connections",
        ws_router.config.limits.max_ws_connections,
    )
    before = _ws_conn("ws", "rejected_capacity")
    status_before = _ws_conn("ws_status", "rejected_capacity")

    asyncio.run(ws_router.ws_kills_live(_FakeWS()))

    assert _ws_conn("ws", "rejected_capacity") - before == 1
    assert _ws_conn("ws_status", "rejected_capacity") - status_before == 0


def test_ws_guard_counts_status_sockets_toward_capacity(monkeypatch):
    """A status socket costs a connection slot like any other."""
    monkeypatch.setattr(ws_router, "origin_allowed", lambda *_: True)
    monkeypatch.setattr(
        ws_router.metrics,
        "ws_status_connections",
        ws_router.config.limits.max_ws_connections,
    )
    sock = _FakeWS()
    assert asyncio.run(ws_router._ws_guard(sock, "ws_status")) is False
    assert sock.closed == (1013, "Server at capacity")
