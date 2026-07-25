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


class _FakeDbCapture:
    def __init__(self, value):
        self._value = value
        self.query = None

    async def fetchval(self, query, *args):
        self.query = query
        return self._value


def test_fetch_system_latest_inserted_floors_epoch(monkeypatch):
    # The freshness boundary must never round UP past the true MAX(inserted_time),
    # or delta-polling would skip a same-second kill. Guard against a silent revert
    # to a bare ::BIGINT cast.
    fake = _FakeDbCapture(1700000000)
    monkeypatch.setattr(queries, "db", fake)
    result = asyncio.run(queries.fetch_system_latest_inserted(30000142))
    assert result == 1700000000
    assert "FLOOR(EXTRACT(EPOCH FROM MAX(inserted_time)))" in fake.query


class _FakeDbFetch:
    def __init__(self, rows):
        self._rows = rows
        self.query = None

    async def fetch(self, query, *args):
        self.query = query
        return self._rows


def test_fetch_system_kills_aligns_and_defaults(monkeypatch):
    # Index i in every array must belong to system_ids[i]; a windowed view that
    # lacks a system contributes 0 (COALESCE in SQL), never a shifted/dropped cell.
    rows = [
        {"solar_system_id": 30000142, "all_count": 100, "day_count": 5,
         "week_count": 20, "month_count": 60, "six_months_count": 80, "year_count": 90},
        {"solar_system_id": 30002187, "all_count": 3, "day_count": 0,
         "week_count": 0, "month_count": 1, "six_months_count": 2, "year_count": 3},
    ]
    fake = _FakeDbFetch(rows)
    monkeypatch.setattr(queries, "db", fake)
    result = asyncio.run(queries.fetch_system_kills())

    assert result.system_ids == [30000142, 30002187]
    assert result.all == [100, 3]
    assert result.day == [5, 0]
    assert result.week == [20, 0]
    assert result.month == [60, 1]
    assert result.six_months == [80, 2]
    assert result.year == [90, 3]
    # every column is the same length as system_ids
    assert {len(result.all), len(result.day), len(result.week), len(result.month),
            len(result.six_months), len(result.year)} == {len(result.system_ids)}

    # Guard the SQL shape that makes alignment structural.
    q = fake.query
    assert "FROM mv_kills_per_system" in q
    for view in ("mv_kills_per_system_24h", "mv_kills_per_system_7d",
                 "mv_kills_per_system_30d", "mv_kills_per_system_6m",
                 "mv_kills_per_system_1y"):
        assert view in q, view
    assert "LEFT JOIN" in q.upper()
    assert "COALESCE" in q.upper()
    assert "ORDER BY" in q.upper()
