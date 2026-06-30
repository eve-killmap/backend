from pydantic import BaseModel
from datetime import datetime


class Victim(BaseModel):
    character_id: int | None
    corporation_id: int | None
    alliance_id: int | None
    faction_id: int | None
    damage_taken: int
    ship_type_id: int


class Attacker(BaseModel):
    character_id: int | None
    corporation_id: int | None
    alliance_id: int | None
    faction_id: int | None
    ship_type_id: int | None
    weapon_type_id: int | None
    damage_done: int
    final_blow: bool
    security_status: float


class KillDetail(BaseModel):
    killmail_id: int
    killmail_time: datetime
    position: tuple[float, float, float]
    war_id: int | None
    victim: Victim
    attackers: list[Attacker]
    inserted_time: datetime


class RawKillDetailResponse(BaseModel):
    count: int
    kills: list[KillDetail]


class VictimProcessed(BaseModel):
    character: str
    character_corporation: str | None
    character_corporation_ticker: str | None
    character_alliance: str | None
    character_alliance_ticker: str | None
    character_faction: str | None
    damage_taken: int


class AttackerProcessed(BaseModel):
    character: str
    character_corporation: str | None
    character_corporation_ticker: str | None
    character_alliance: str | None
    character_alliance_ticker: str | None
    character_faction: str | None
    ship: str | None
    weapon: str | None
    damage_done: int
    security_status: float


class WarParticipant(BaseModel):
    alliance: str | None
    alliance_ticker: str | None
    corporation: str | None
    corporation_ticker: str | None
    ships_killed: int


class WarProcessed(BaseModel):
    aggressor: WarParticipant
    defender: WarParticipant
    declared: int
    finished: int | None
    mutual: bool
    retracted: int | None
    started: int | None


class ProcessedKillDetailResponse(BaseModel):
    victim: VictimProcessed
    final_blow: AttackerProcessed
    top_damage: AttackerProcessed
    war_id: int | None = None
    war_info: WarProcessed | None = None
    final_blow_is_top_damage: bool
    attackers: int


class RankSystem(BaseModel):
    solar_system_id: int
    kill_count: int


class TopSystems(BaseModel):
    all: list[RankSystem]
    day: list[RankSystem]
    week: list[RankSystem]
    month: list[RankSystem]
    six_months: list[RankSystem]
    year: list[RankSystem]


class RankSystemsResponse(BaseModel):
    top: TopSystems
    bottom: list[RankSystem]


class FarthestKillResponse(BaseModel):
    farthest_kill: int


class GroupInfo(BaseModel):
    id: int
    name: str
    ticker: str


class SovResponse(BaseModel):
    claimed: bool
    alliance: GroupInfo | None = None
    corporation: GroupInfo | None = None


class WorkersSummary(BaseModel):
    worker_count: int
    degraded: bool
    cache_hit_rate: float | None
    totals: dict
    workers: list[dict]


class HealthDetailResponse(BaseModel):
    status: str
    workers: WorkersSummary
    database: dict
    domain: dict
    redis: dict
