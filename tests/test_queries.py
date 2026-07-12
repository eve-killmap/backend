import asyncio

from app.queries import normalize_farthest_kill

import app.queries as queries
import app.prometheus_metrics as pm  # noqa: F401  (ensures registration)
from prometheus_client import REGISTRY


def test_normalize_none_is_minus_one():
    assert normalize_farthest_kill(None) == -1


def test_normalize_rounds_to_int():
    assert normalize_farthest_kill(1234.0) == 1234
    assert normalize_farthest_kill(0.0) == 0


def _sample(name, labels=None):
    return REGISTRY.get_sample_value(name, labels) or 0.0


class _FakeMget:
    def __init__(self, values):
        self._values = values

    async def mget(self, *keys):
        return self._values


def test_get_type_names_hit_metric(monkeypatch):
    # All ids resolved from Redis -> one type_name hit, no DB.
    monkeypatch.setattr(queries, "_redis", _FakeMget(["Rifter"]))
    h0 = _sample("eve_killmap_cache_hits_total", {"cache": "type_name"})
    result = asyncio.run(queries.get_type_names({587}))
    assert result == {587: "Rifter"}
    assert _sample("eve_killmap_cache_hits_total", {"cache": "type_name"}) - h0 == 1


def test_get_kill_details_cached_hit_metric(monkeypatch):
    from datetime import datetime, timezone

    from app.models import KillDetail, Victim

    # killmail_time / inserted_time are required datetimes (see app/models.py).
    ts = datetime(2026, 1, 1, tzinfo=timezone.utc)
    kill = KillDetail(
        killmail_id=42,
        killmail_time=ts,
        position=(0.0, 0.0, 0.0),
        war_id=None,
        victim=Victim(
            character_id=1, corporation_id=None, alliance_id=None,
            faction_id=None, damage_taken=1, ship_type_id=587,
        ),
        attackers=[],
        inserted_time=ts,
    )
    monkeypatch.setattr(queries, "_redis", _FakeMget([kill.model_dump_json()]))
    h0 = _sample("eve_killmap_cache_hits_total", {"cache": "kill_details"})
    resp = asyncio.run(queries.get_kill_details_cached([42]))
    assert resp.count == 1
    assert _sample("eve_killmap_cache_hits_total", {"cache": "kill_details"}) - h0 == 1
