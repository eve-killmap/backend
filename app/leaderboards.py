import asyncio
from typing import Literal

from app import entities
from app.database import db
from app.filters import ATTRIBUTE_KINDS, SIDE_ROLES
from app.models import LeaderboardEntry, LeaderboardsResponse
from app.queries import get_type_names
from app.timeparse import datetime_to_epoch

Window = Literal["all", "1d", "7d", "30d", "6m", "1y"]
Role = Literal["victim", "attacker"]

WINDOWS: dict[str, str] = {  # public value -> entity_leaderboard.window_key
    "all": "all",
    "1d": "day",
    "7d": "week",
    "30d": "month",
    "6m": "six_months",
    "1y": "year",
}
ROLES: tuple[str, ...] = ("victim", "attacker")
KINDS: tuple[str, ...] = (
    "character",
    "corporation",
    "alliance",
    "faction",
    "ship",
    "weapon",
)
_KIND_IDS = [ATTRIBUTE_KINDS[k] for k in KINDS]
_KIND_NAMES = {ATTRIBUTE_KINDS[k]: k for k in KINDS}

_BOARD_SQL = (
    "SELECT facet_kind, facet_value, kill_count, computed_at "
    "FROM entity_leaderboard "
    "WHERE facet_kind = ANY($1::smallint[]) AND role = $2 AND window_key = $3 "
    "AND rank <= $4 "
    "ORDER BY facet_kind, rank"
)


async def fetch_leaderboards(window: str, role: str, limit: int) -> LeaderboardsResponse:
    """Top-``limit`` boards for every facet kind in one ``window``/``role``, with
    names from the reference tables. Ranks are contiguous per board, so
    ``rank <= limit`` bounds each board in one query; short or empty boards are
    returned as they are. ``name``/``ticker`` stay unset when unresolved."""
    rows = await db.fetch(
        _BOARD_SQL, _KIND_IDS, SIDE_ROLES[role], WINDOWS[window], limit
    )
    by_kind: dict[str, list] = {k: [] for k in KINDS}
    for r in rows:
        by_kind[_KIND_NAMES[r["facet_kind"]]].append(r)
    ids = {k: {r["facet_value"] for r in by_kind[k]} for k in KINDS}

    (char_names, corp_info, alliance_info, faction_names), type_names = (
        await asyncio.gather(
            entities.fetch_entity_names(
                ids["character"], ids["corporation"], ids["alliance"], ids["faction"]
            ),
            get_type_names(ids["ship"] | ids["weapon"]),
        )
    )
    names = {
        "character": char_names,
        "corporation": {i: n for i, (n, _t) in corp_info.items()},
        "alliance": {i: n for i, (n, _t) in alliance_info.items()},
        "faction": faction_names,
        "ship": type_names,
        "weapon": type_names,
    }
    tickers = {
        "corporation": {i: t for i, (_n, t) in corp_info.items()},
        "alliance": {i: t for i, (_n, t) in alliance_info.items()},
    }

    def board(kind: str) -> list[LeaderboardEntry]:
        return [
            LeaderboardEntry(
                id=r["facet_value"],
                name=names[kind].get(r["facet_value"]),
                ticker=tickers.get(kind, {}).get(r["facet_value"]),
                kills=r["kill_count"],
            )
            for r in by_kind[kind]
        ]

    computed_at = datetime_to_epoch(
        max((r["computed_at"] for r in rows), default=None)
    )
    return LeaderboardsResponse(
        computed_at=computed_at, **{k: board(k) for k in KINDS}
    )
