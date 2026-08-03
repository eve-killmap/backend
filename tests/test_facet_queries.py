import asyncio
import app.facet_queries as fq
from app.filters import parse_filter


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
        {"solar_system_id": 30000142, "all_count": 10, "day_count": 1,
         "week_count": 2, "month_count": 3, "six_months_count": 5, "year_count": 8},
        {"solar_system_id": 30002187, "all_count": 4, "day_count": 0,
         "week_count": 0, "month_count": 1, "six_months_count": 2, "year_count": 3},
    ]
    fake = _FakeDbFetch(rows)
    monkeypatch.setattr(fq, "db", fake)
    f = parse_filter(["alliance:attacker:99005338"], max_conditions=8, max_ids=50)
    result = asyncio.run(fq.fetch_filtered_map(f))
    assert result.system_ids == [30000142, 30002187]
    assert result.all == [10, 4]
    assert result.day == [1, 0]
    assert result.six_months == [5, 2]
    assert "kill_facets" in fake.query
