import asyncio
import asyncio as _asyncio

from app.cache import SingleFlight
from app.esi import EsiClient


def test_single_flight_returns_same_lock_per_key():
    sf = SingleFlight()

    async def go():
        a = sf.lock("k1")
        b = sf.lock("k1")
        c = sf.lock("k2")
        return a is b, a is c

    same, diff = asyncio.run(go())
    assert same is True
    assert diff is False


class _FakeRedisStr:
    def __init__(self, value):
        self._value = value

    async def get(self, key):
        return self._value


def test_get_sov_map_cached_int_keys():
    client = EsiClient()
    client._redis = _FakeRedisStr('{"30000142": {"system_id": 30000142}}')  # type: ignore[attr-defined]
    result = _asyncio.run(client.get_sov_map_cached())
    assert result == {30000142: {"system_id": 30000142}}


def test_get_sov_map_cached_none_without_redis():
    client = EsiClient()
    assert _asyncio.run(client.get_sov_map_cached()) is None


from app.cache import should_short_circuit


def test_should_short_circuit():
    assert should_short_circuit(since=100, latest=90) is True  # client up to date
    assert should_short_circuit(since=100, latest=100) is True
    assert should_short_circuit(since=100, latest=101) is False  # newer kill exists
    assert should_short_circuit(since=None, latest=100) is False  # full fetch
    assert (
        should_short_circuit(since=100, latest=None) is False
    )  # unknown -> must query


from app.models import KillDetail, Victim, RawKillDetailResponse
from app.queries import merge_kill_details
from datetime import datetime, timezone


def _kill(kid):
    return KillDetail(
        killmail_id=kid,
        killmail_time=datetime(2026, 1, 1, tzinfo=timezone.utc),
        position=(0.0, 0.0, 0.0),
        war_id=None,
        victim=Victim(
            character_id=1,
            corporation_id=None,
            alliance_id=None,
            faction_id=None,
            damage_taken=10,
            ship_type_id=587,
        ),
        attackers=[],
        inserted_time=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )


def test_merge_kill_details_counts():
    resp = merge_kill_details([_kill(1), _kill(2)])
    assert isinstance(resp, RawKillDetailResponse)
    assert resp.count == 2
    assert {k.killmail_id for k in resp.kills} == {1, 2}
