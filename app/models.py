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
    fitted_value: float | None = None
    dropped_value: float | None = None
    destroyed_value: float | None = None
    total_value: float | None = None
    total_droppable_value: float | None = None
    npc: bool | None = None
    solo: bool | None = None
    awox: bool | None = None
    labels: list[str] | None = None


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


class WarSummary(BaseModel):
    war_id: int
    declared: int | None
    started: int | None
    finished: int | None
    retracted: int | None
    mutual: bool
    aggressor_corporation_id: int | None
    aggressor_alliance_id: int | None
    defender_corporation_id: int | None
    defender_alliance_id: int | None
    ally_corporation_ids: list[int]
    ally_alliance_ids: list[int]


class ProcessedKillDetailResponse(BaseModel):
    victim: VictimProcessed
    final_blow: AttackerProcessed
    top_damage: AttackerProcessed
    war_id: int | None = None
    war_info: WarProcessed | None = None
    final_blow_is_top_damage: bool
    attackers: int
    fitted_value: float | None = None
    dropped_value: float | None = None
    destroyed_value: float | None = None
    total_value: float | None = None
    total_droppable_value: float | None = None
    npc: bool | None = None
    solo: bool | None = None
    awox: bool | None = None
    labels: list[str] | None = None


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


class SystemKillsResponse(BaseModel):
    system_ids: list[int]
    all: list[int]
    day: list[int]
    week: list[int]
    month: list[int]
    six_months: list[int]
    year: list[int]


class SystemKillIdsResponse(BaseModel):
    count: int
    killmail_ids: list[int]


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
    adm: float | None = None
    vulnerable_start: int | None = None
    vulnerable_end: int | None = None


class SovereigntyMapResponse(BaseModel):
    updated_at: int
    adm_available: bool
    owner_kinds: list[int]
    owner_ids: list[int]
    owner_names: list[str | None]
    owner_tickers: list[str | None]
    system_ids: list[int]
    owner_idx: list[int]
    adm: list[float]


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


class EntityCandidate(BaseModel):
    id: int
    name: str
    ticker: str | None = None
    image_url: str
    member_count: int | None = None
    date_founded: int | None = None


class TypeCandidate(BaseModel):
    id: int
    name: str
    image_url: str


class NameResolution(BaseModel):
    category: str
    name: str
    ticker: str | None = None
    image_url: str
