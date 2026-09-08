import asyncio
import json

from app import redis_client as rc
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


def test_dispatch_routes_by_channel(monkeypatch):
    seen = {"kills": 0, "status": 0}
    monkeypatch.setattr(
        rc.broadcaster, "_fanout", lambda p: seen.__setitem__("kills", seen["kills"] + 1)
    )
    monkeypatch.setattr(
        rc.broadcaster,
        "_fanout_status",
        lambda p: seen.__setitem__("status", seen["status"] + 1),
    )
    rc.broadcaster._dispatch(rc.config.streaming.pubsub_channel, {"solar_system_id": 1})
    rc.broadcaster._dispatch(rc.config.streaming.status_channel, {"online": True})
    assert seen == {"kills": 1, "status": 1}


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
