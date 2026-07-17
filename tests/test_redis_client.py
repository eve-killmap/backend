import asyncio

from app.redis_client import KillBroadcaster, _LOCK_KEY


class _FakeLockRedis:
    """Minimal Redis that models just the leader lock (SET NX / GET / renew EVAL).

    TTL expiry is simulated in the tests by clearing the key.
    """

    def __init__(self):
        self.store: dict[str, str] = {}

    async def set(self, key, value, nx=False, ex=None):
        if nx and key in self.store:
            return None
        self.store[key] = value
        return True

    async def get(self, key):
        return self.store.get(key)

    async def eval(self, script, numkeys, key, ident, ttl):
        # Renew succeeds only while we still own the key.
        return 1 if self.store.get(key) == ident else 0

    async def delete(self, key):
        return 1 if self.store.pop(key, None) is not None else 0


def _stub_promote_demote(b: KillBroadcaster) -> None:
    """Replace promote/demote with plain flag flips so the election decision can be
    tested without starting the real leader/sov background tasks."""

    async def promote():
        b._is_leader = True

    async def demote():
        b._is_leader = False

    b._promote = promote  # type: ignore[method-assign]
    b._demote = demote  # type: ignore[method-assign]


def test_acquires_leadership_when_lock_free():
    b = KillBroadcaster()
    b._redis = _FakeLockRedis()  # type: ignore[assignment]
    _stub_promote_demote(b)

    asyncio.run(b._election_step())

    assert b._is_leader is True
    assert b._redis.store[_LOCK_KEY] == b._instance_id  # type: ignore[attr-defined]


def test_second_instance_stays_follower():
    shared = _FakeLockRedis()
    b1 = KillBroadcaster(); b1._redis = shared  # type: ignore[assignment]
    b2 = KillBroadcaster(); b2._redis = shared  # type: ignore[assignment]
    _stub_promote_demote(b1)
    _stub_promote_demote(b2)

    asyncio.run(b1._election_step())
    asyncio.run(b2._election_step())

    assert b1._is_leader is True
    assert b2._is_leader is False


def test_follower_takes_over_when_leader_lock_expires():
    shared = _FakeLockRedis()
    b1 = KillBroadcaster(); b1._redis = shared  # type: ignore[assignment]
    b2 = KillBroadcaster(); b2._redis = shared  # type: ignore[assignment]
    _stub_promote_demote(b1)
    _stub_promote_demote(b2)

    asyncio.run(b1._election_step())   # b1 leads
    asyncio.run(b2._election_step())   # b2 follows
    assert b2._is_leader is False

    # b1 "dies": its lock expires (simulate by clearing the key).
    shared.store.clear()

    asyncio.run(b2._election_step())   # b2 should now take over
    assert b2._is_leader is True
    assert shared.store[_LOCK_KEY] == b2._instance_id


def test_leader_demotes_when_lock_stolen():
    b = KillBroadcaster()
    shared = _FakeLockRedis()
    b._redis = shared  # type: ignore[assignment]
    _stub_promote_demote(b)

    # b believes it is leader, but another instance now owns the lock.
    b._is_leader = True
    shared.store[_LOCK_KEY] = "someone-else"

    asyncio.run(b._election_step())

    assert b._is_leader is False
    assert shared.store[_LOCK_KEY] == "someone-else"  # not stolen back


class _FakeStreamRedis:
    def __init__(self, last_id=None, exc=None):
        self._last_id = last_id
        self._exc = exc

    async def xinfo_stream(self, name):
        if self._exc is not None:
            raise self._exc
        return {"last-generated-id": self._last_id}


def test_resolve_start_id_uses_last_generated_id():
    b = KillBroadcaster()
    b._redis = _FakeStreamRedis(last_id="1720000000000-0")  # type: ignore[assignment]
    assert asyncio.run(b._resolve_start_id()) == "1720000000000-0"


def test_resolve_start_id_falls_back_when_stream_missing():
    from redis.exceptions import ResponseError

    b = KillBroadcaster()
    b._redis = _FakeStreamRedis(exc=ResponseError("no such key"))  # type: ignore[assignment]
    assert asyncio.run(b._resolve_start_id()) == "$"


import app.prometheus_metrics as pm  # noqa: E402
from prometheus_client import REGISTRY  # noqa: E402


def _sample(name, labels=None):
    return REGISTRY.get_sample_value(name, labels) or 0.0


def test_subscribe_unsubscribe_updates_live_clients_gauge():
    b = KillBroadcaster()
    base = _sample("eve_killmap_live_clients", {"transport": "ws"})
    q = b.subscribe_global()
    assert _sample("eve_killmap_live_clients", {"transport": "ws"}) - base == 1
    b.unsubscribe_global(q)
    assert _sample("eve_killmap_live_clients", {"transport": "ws"}) - base == 0
    b.unsubscribe_global(q)  # double unsubscribe must not drift the gauge
    assert _sample("eve_killmap_live_clients", {"transport": "ws"}) - base == 0


def test_fanout_counts_dropped_messages():
    b = KillBroadcaster()
    full = asyncio.Queue(maxsize=1)
    full.put_nowait({"already": "full"})
    b._global_subs.add(full)  # type: ignore[attr-defined]
    d0 = _sample("eve_killmap_ws_messages_dropped_total")
    b._fanout({"solar_system_id": 30000142})
    assert _sample("eve_killmap_ws_messages_dropped_total") - d0 == 1


import app.entities as entities  # noqa: E402
import app.redis_client as rc  # noqa: E402


def test_enrich_kill_uses_db(monkeypatch):
    async def fake_entities(char_ids, corp_ids, alliance_ids, faction_ids):
        return ({1: "Pilot"}, {10: ("Corp", "TIC")}, {20: ("Alli", "AL1")}, {})

    async def fake_types(ids):
        return {587: "Rifter"}

    monkeypatch.setattr(entities, "fetch_entity_names", fake_entities)
    monkeypatch.setattr(rc, "get_type_names", fake_types)

    kill = {
        "killmail_id": 42, "victim_character_id": 1, "victim_ship_type_id": 587,
        "victim_corporation_id": 10, "victim_alliance_id": 20,
        "attackers": [{"final_blow": True, "character_id": 1, "ship_type_id": 587,
                       "corporation_id": 10, "alliance_id": 20}],
    }
    out = asyncio.run(rc._enrich_kill(kill))
    assert out["v_character_name"] == "Pilot"
    assert out["v_corporation_name"] == "Corp"
    assert out["v_alliance_name"] == "Alli"
    assert out["v_ship_name"] == "Rifter"


def test_enrich_kill_resilient_on_db_error(monkeypatch):
    async def boom(*a, **k):
        raise RuntimeError("db down")

    monkeypatch.setattr(entities, "fetch_entity_names", boom)

    async def fake_types(ids):
        return {}

    monkeypatch.setattr(rc, "get_type_names", fake_types)
    kill = {"killmail_id": 7, "victim_character_id": 1, "victim_ship_type_id": 587,
            "victim_corporation_id": 10, "victim_alliance_id": 20, "attackers": []}
    out = asyncio.run(rc._enrich_kill(kill))   # must not raise
    assert out["v_character_name"] is None
    assert out["v_corporation_name"] is None
