import asyncio
import json

import pytest

import app.routers.ws as ws_router
from app import redis_client as rc


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
        "data": json.dumps({"solar_system_id": 30000142, "killmail_id": killmail_id}),
    }


def _drive_subscriber_loop(monkeypatch, messages):
    """Run the real loop over `messages`, forbidding any backoff sleep so a
    swallowed cancellation or an unexpected reconnect fails loudly."""
    pubsub = _FakePubSub(messages)
    b = rc.KillBroadcaster()
    b._redis = _FakePubSubRedis(pubsub)
    kq = b.subscribe_global()

    async def _no_backoff(_seconds):
        raise AssertionError("cancellation was swallowed into a retry")

    monkeypatch.setattr(rc.asyncio, "sleep", _no_backoff)
    try:
        with pytest.raises(asyncio.CancelledError):
            asyncio.run(b._subscriber_loop())
    finally:
        b.unsubscribe_global(kq)
    return pubsub, kq


def test_subscriber_loop_subscribes_the_kill_channel_and_fans_out(monkeypatch):
    """End-to-end: the live loop must subscribe to the kill channel and deliver
    its messages to the kill fan-out."""
    kill_channel = rc.config.streaming.pubsub_channel
    pubsub, kq = _drive_subscriber_loop(
        monkeypatch,
        [
            {"type": "subscribe", "channel": kill_channel, "data": 1},
            _kill_message(7),
        ],
    )

    assert set(pubsub.subscribed) == {kill_channel}
    assert set(pubsub.unsubscribed) == {kill_channel}
    assert kq.get_nowait()["killmail_id"] == 7
    assert kq.empty()


def test_subscriber_loop_drops_messages_from_other_channels(monkeypatch):
    """Delivery must fail closed. Redis can hand this subscription a message from
    another channel; passed to the kill fan-out it would raise on its missing
    solar_system_id, tearing down the subscription and stalling every kill socket
    on this worker until the reconnect lands."""
    _, kq = _drive_subscriber_loop(
        monkeypatch,
        [
            {
                "type": "message",
                "channel": rc.config.streaming.invalidate_channel,
                "data": json.dumps({"targets": ["sov"]}),
            },
            _kill_message(7),
        ],
    )

    assert kq.get_nowait()["killmail_id"] == 7  # the foreign message did not stop it
    assert kq.empty()


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
    assert set(redis.handed_out[1].subscribed) == {rc.config.streaming.pubsub_channel}
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


def _ws_conn(outcome):
    from prometheus_client import REGISTRY

    return (
        REGISTRY.get_sample_value(
            "eve_killmap_ws_connections_total",
            {"transport": "ws", "outcome": outcome},
        )
        or 0.0
    )


def test_kill_endpoint_guard_labels_its_outcomes_transport_ws(monkeypatch):
    """transport="ws" is the series the dashboards query, and a socket over the
    cap is closed with 1013 rather than accepted into an oversubscribed worker."""
    from types import SimpleNamespace

    monkeypatch.setattr(ws_router, "origin_allowed", lambda *_: True)
    monkeypatch.setattr(ws_router, "broadcaster", SimpleNamespace(is_running=True))
    outcomes = ("accepted", "rejected_capacity")
    before = {o: _ws_conn(o) for o in outcomes}

    assert asyncio.run(ws_router._ws_guard(_FakeWS())) is True
    monkeypatch.setattr(
        ws_router.metrics,
        "ws_global_connections",
        ws_router.config.limits.max_ws_connections,
    )
    sock = _FakeWS()
    asyncio.run(ws_router.ws_kills_live(sock))  # rejected at capacity

    assert _ws_conn("accepted") - before["accepted"] == 1
    assert _ws_conn("rejected_capacity") - before["rejected_capacity"] == 1
    assert sock.closed == (1013, "Server at capacity")
