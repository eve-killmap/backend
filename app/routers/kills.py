import asyncio
from typing import Annotated

from fastapi import APIRouter, Query, Header, HTTPException

from app.config import config
from app.security import validate_id_list
from app.cache import query_cache, single_flight
from app.http_cache import json_cache_response, compute_etag
from app import entities
from app.models import VictimProcessed, AttackerProcessed, ProcessedKillDetailResponse
from app.queries import (
    fetch_kills_by_ids,
    get_type_names,
    get_kill_details_cached,
)

router = APIRouter()


def _parse_killmail_ids(killmail_ids_str: str) -> list[int]:
    try:
        return [int(x.strip()) for x in killmail_ids_str.split(",")]
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail="Invalid killmail_ids format. Expected comma-separated integers.",
        )


@router.get("/kills/details/raw", response_model=None)
async def get_kill_details(
    killmail_ids: Annotated[
        str,
        Query(description="Comma-separated list of killmail IDs to fetch details for"),
    ],
    if_none_match: Annotated[str | None, Header(alias="If-None-Match")] = None,
):
    """Get raw, unprocessed kill details for a list of killmail IDs."""
    ids_list = _parse_killmail_ids(killmail_ids)
    error = validate_id_list(ids_list, config.limits.max_killmail_ids)
    if error is not None:
        raise HTTPException(status_code=400, detail=error)
    result = await get_kill_details_cached(ids_list)
    json_bytes = result.model_dump_json().encode("utf-8")
    return json_cache_response(
        json_bytes,
        gzipped=False,
        etag=compute_etag(json_bytes),
        max_age=config.cache.kill_detail_ttl,
        if_none_match=if_none_match,
    )


@router.get("/kills/details/processed", response_model=None)
async def get_kill_details_processed(
    killmail_id: Annotated[int, Query(description="Killmail ID to fetch details for")],
    if_none_match: Annotated[str | None, Header(alias="If-None-Match")] = None,
):
    """Get processed kill details for a killmail. Processed data includes character names,
    corporations, alliances, war information, etc."""
    cache_params = {"killmail_id": killmail_id}
    res = await query_cache.get("kill_details_processed", cache_params)
    if res is None:
        async with single_flight.lock(f"kill_details_processed:{killmail_id}"):
            res = await query_cache.get("kill_details_processed", cache_params)
            if res is None:
                raw = await fetch_kills_by_ids([killmail_id])
                if not raw.kills:
                    raise HTTPException(
                        status_code=404, detail=f"Killmail {killmail_id} not found"
                    )

                kill = raw.kills[0]
                victim = kill.victim
                attackers = kill.attackers

                if not attackers:
                    raise HTTPException(
                        status_code=500,
                        detail="Killmail data is malformed: no attackers",
                    )

                final_blow_idx = next(
                    (i for i, a in enumerate(attackers) if a.final_blow), None
                )
                top_damage_idx = max(
                    range(len(attackers)), key=lambda i: attackers[i].damage_done
                )

                if final_blow_idx is None:
                    raise HTTPException(
                        status_code=500,
                        detail="Killmail data is malformed: no final blow attacker",
                    )

                final_blow_attacker = attackers[final_blow_idx]
                top_damage_attacker = attackers[top_damage_idx]
                final_blow_is_top_damage = final_blow_idx == top_damage_idx

                character_ids: set[int] = set()
                faction_ids: set[int] = set()
                type_ids_to_resolve: set[int] = set()

                if victim.character_id is None:
                    type_ids_to_resolve.add(victim.ship_type_id)
                else:
                    character_ids.add(victim.character_id)
                if victim.faction_id is not None:
                    faction_ids.add(victim.faction_id)

                for atk in [final_blow_attacker, top_damage_attacker]:
                    if atk.character_id is None:
                        if atk.ship_type_id is not None:
                            type_ids_to_resolve.add(atk.ship_type_id)
                    else:
                        character_ids.add(atk.character_id)
                        if atk.ship_type_id is not None:
                            type_ids_to_resolve.add(atk.ship_type_id)
                    if atk.faction_id is not None:
                        faction_ids.add(atk.faction_id)
                    if atk.weapon_type_id is not None:
                        type_ids_to_resolve.add(atk.weapon_type_id)

                corp_id_set: set[int] = {
                    id_
                    for id_ in [
                        victim.corporation_id,
                        final_blow_attacker.corporation_id,
                        top_damage_attacker.corporation_id,
                    ]
                    if id_ is not None
                }
                alliance_id_set: set[int] = {
                    id_
                    for id_ in [
                        victim.alliance_id,
                        final_blow_attacker.alliance_id,
                        top_damage_attacker.alliance_id,
                    ]
                    if id_ is not None
                }

                war_row = (
                    await entities.fetch_war(kill.war_id)
                    if kill.war_id is not None
                    else None
                )
                if war_row is not None:
                    for cid in (
                        war_row["aggressor_corporation_id"],
                        war_row["defender_corporation_id"],
                    ):
                        if cid is not None:
                            corp_id_set.add(cid)
                    for aid in (
                        war_row["aggressor_alliance_id"],
                        war_row["defender_alliance_id"],
                    ):
                        if aid is not None:
                            alliance_id_set.add(aid)

                (
                    character_names,
                    corp_info,
                    alliance_info,
                    faction_names,
                ), type_names = await asyncio.gather(
                    entities.fetch_entity_names(
                        character_ids, corp_id_set, alliance_id_set, faction_ids
                    ),
                    get_type_names(type_ids_to_resolve),
                )
                names: dict[int, str] = {
                    **character_names,
                    **faction_names,
                    **type_names,
                }

                def build_attacker(atk) -> AttackerProcessed:
                    if atk.character_id is None:
                        character = (
                            names.get(atk.ship_type_id, "Unknown")
                            if atk.ship_type_id is not None
                            else "Unknown"
                        )
                        ship = None
                    else:
                        character = names.get(atk.character_id, "Unknown")
                        ship = (
                            names.get(atk.ship_type_id)
                            if atk.ship_type_id is not None
                            else None
                        )
                    corp = (
                        corp_info.get(atk.corporation_id)
                        if atk.corporation_id is not None
                        else None
                    )
                    alliance = (
                        alliance_info.get(atk.alliance_id)
                        if atk.alliance_id is not None
                        else None
                    )
                    return AttackerProcessed(
                        character=character,
                        character_corporation=corp[0] if corp else None,
                        character_corporation_ticker=corp[1] if corp else None,
                        character_alliance=alliance[0] if alliance else None,
                        character_alliance_ticker=alliance[1] if alliance else None,
                        character_faction=(
                            names.get(atk.faction_id)
                            if atk.faction_id is not None
                            else None
                        ),
                        ship=ship,
                        weapon=(
                            names.get(atk.weapon_type_id)
                            if atk.weapon_type_id is not None
                            else None
                        ),
                        damage_done=atk.damage_done,
                        security_status=atk.security_status,
                    )

                victim_corp = (
                    corp_info.get(victim.corporation_id)
                    if victim.corporation_id is not None
                    else None
                )
                victim_alliance = (
                    alliance_info.get(victim.alliance_id)
                    if victim.alliance_id is not None
                    else None
                )

                if victim.character_id is None:
                    victim_name = names.get(victim.ship_type_id, "Unknown")
                else:
                    victim_name = names.get(victim.character_id, "Unknown")

                victim_processed = VictimProcessed(
                    character=victim_name,
                    character_corporation=victim_corp[0] if victim_corp else None,
                    character_corporation_ticker=(
                        victim_corp[1] if victim_corp else None
                    ),
                    character_alliance=victim_alliance[0] if victim_alliance else None,
                    character_alliance_ticker=(
                        victim_alliance[1] if victim_alliance else None
                    ),
                    character_faction=(
                        names.get(victim.faction_id)
                        if victim.faction_id is not None
                        else None
                    ),
                    damage_taken=victim.damage_taken,
                )

                war_info = (
                    entities.build_war_processed(war_row, corp_info, alliance_info)
                    if war_row is not None
                    else None
                )

                result = ProcessedKillDetailResponse(
                    victim=victim_processed,
                    final_blow=build_attacker(final_blow_attacker),
                    top_damage=build_attacker(top_damage_attacker),
                    war_id=kill.war_id,
                    war_info=war_info,
                    final_blow_is_top_damage=final_blow_is_top_damage,
                    attackers=len(attackers),
                )

                res = await query_cache.set(
                    "kill_details_processed",
                    cache_params,
                    result.model_dump_json(exclude_none=True),
                    ttl=config.cache.kill_detail_processed_ttl,
                )
    etag, gzipped, body = res
    return json_cache_response(
        body, gzipped, etag, config.cache.kill_detail_processed_ttl, if_none_match
    )
