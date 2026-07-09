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
