from __future__ import annotations

from typing import NotRequired, TypedDict


class RawKillsColumns(TypedDict):
    """The columnar dict returned by queries.fetch_raw_kills."""

    count: int
    killmail_ids: list[int]
    x: list[float]
    y: list[float]
    z: list[float]
    killmail_times: list[int]
    ship_types: list[int]


class PgStatDatabaseRow(TypedDict):
    """Subset of pg_stat_database consumed by the health endpoint (Plan 2)."""

    numbackends: int
    xact_commit: int
    xact_rollback: int
    blks_read: int
    blks_hit: int
    tup_returned: int
    tup_fetched: int
    tup_inserted: int
    tup_updated: int
    tup_deleted: int
    deadlocks: int
    temp_files: int
    stats_reset: NotRequired[str]


class EsiWarParticipant(TypedDict):
    ships_killed: int
    corporation_id: NotRequired[int]
    alliance_id: NotRequired[int]


class EsiWar(TypedDict):
    aggressor: EsiWarParticipant
    defender: EsiWarParticipant
    declared: str
    mutual: bool
    finished: NotRequired[str]
    retracted: NotRequired[str]
    started: NotRequired[str]


class EsiSovEntry(TypedDict):
    system_id: int
    alliance_id: NotRequired[int]
    corporation_id: NotRequired[int]
    faction_id: NotRequired[int]
