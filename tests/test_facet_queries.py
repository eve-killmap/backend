import asyncio
import app.facet_queries as fq
from app.filters import parse_filter
from prometheus_client import REGISTRY


class _FakeDbFetch:
    def __init__(self, rows):
        self._rows = rows
        self.query = None
        self.args = None

    async def fetch(self, query, *args):
        self.query = query
        self.args = args
        return self._rows


def test_fetch_filtered_map_builds_columns(monkeypatch):
    rows = [
        {"solar_system_id": 30000142, "kill_count": 10},
        {"solar_system_id": 30002187, "kill_count": 4},
    ]
    fake = _FakeDbFetch(rows)
    monkeypatch.setattr(fq, "db", fake)
    f = parse_filter(["alliance:attacker:99005338"], max_conditions=8, max_ids=50)
    result = asyncio.run(fq.fetch_filtered_map(f))
    assert result.system_ids == [30000142, 30002187]
    assert result.counts == [10, 4]
    assert "kill_facets" in fake.query


def test_fetch_filtered_map_threads_window(monkeypatch):
    from datetime import date, datetime, timezone

    fake = _FakeDbFetch([])
    monkeypatch.setattr(fq, "db", fake)
    f = parse_filter(["alliance:attacker:99005338"], max_conditions=8, max_ids=50)
    asyncio.run(fq.fetch_filtered_map(f, date(2026, 1, 1), date(2026, 3, 1)))
    assert "killmail_time >= $" in fake.query
    assert "killmail_time < $" in fake.query
    assert datetime(2026, 1, 1, tzinfo=timezone.utc) in fake.args
    assert datetime(2026, 3, 1, tzinfo=timezone.utc) in fake.args


def test_fetch_filtered_system_kill_ids(monkeypatch):
    fake = _FakeDbFetch([{"killmail_id": 111}, {"killmail_id": 222}])
    monkeypatch.setattr(fq, "db", fake)
    f = parse_filter(["character:involved:12345"], max_conditions=8, max_ids=50)
    result = asyncio.run(fq.fetch_filtered_system_kill_ids(30000142, f))
    assert result.count == 2
    assert result.killmail_ids == [111, 222]
    assert "DISTINCT" in fake.query
    assert 30000142 in fake.args


def _hist_count(name, labels=None):
    return REGISTRY.get_sample_value(name, labels) or 0.0


def test_fetch_filtered_map_records_metrics(monkeypatch):
    fake = _FakeDbFetch([])
    monkeypatch.setattr(fq, "db", fake)
    q0 = _hist_count("eve_killmap_facet_query_seconds_count", {"query": "map"})
    c0 = _hist_count("eve_killmap_filter_conditions_count")
    f = parse_filter(["alliance:attacker:99005338"], max_conditions=8, max_ids=50)
    asyncio.run(fq.fetch_filtered_map(f))
    assert (
        _hist_count("eve_killmap_facet_query_seconds_count", {"query": "map"}) - q0 == 1
    )
    assert _hist_count("eve_killmap_filter_conditions_count") - c0 == 1


def test_fetch_filtered_system_records_metrics(monkeypatch):
    fake = _FakeDbFetch([])
    monkeypatch.setattr(fq, "db", fake)
    q0 = _hist_count("eve_killmap_facet_query_seconds_count", {"query": "system"})
    f = parse_filter(["character:involved:12345"], max_conditions=8, max_ids=50)
    asyncio.run(fq.fetch_filtered_system_kill_ids(30000142, f))
    assert (
        _hist_count("eve_killmap_facet_query_seconds_count", {"query": "system"}) - q0
        == 1
    )
