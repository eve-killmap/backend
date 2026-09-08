import asyncio
import json
import logging
import os
import time
import uuid
from typing import cast

import redis.asyncio as aioredis
from redis.exceptions import (
    ConnectionError as RedisConnectionError,
    ResponseError as RedisResponseError,
    TimeoutError as RedisTimeoutError,
)

from app.config import config
from app.metrics import metrics
from app import prometheus_metrics as pm
from app import entities
from app.esi import (
    SOV_MAP,
    SOV_STRUCTURES,
    STATUS,
    SYSTEM_JUMPS,
    EsiFeed,
    EsiFeedRefreshError,
    esi_client,
)
from app.queries import get_type_names
from app.timeparse import iso_to_epoch
from app.positions import sanitize_position

logger = logging.getLogger(__name__)

_LOCK_KEY = "kills:broadcaster:leader"
_LOCK_TTL = 30
_ELECTION_INTERVAL = 10

LEADER_FEEDS: tuple[EsiFeed, ...] = (SOV_MAP, SOV_STRUCTURES, SYSTEM_JUMPS, STATUS)

_RENEW_SCRIPT = """
if redis.call('get', KEYS[1]) == ARGV[1] then
    return redis.call('expire', KEYS[1], ARGV[2])
else
    return 0
end
"""

_GLOBAL_FIELDS = frozenset(
    {
        "killmail_id",
        "killmail_time",
        "solar_system_id",
        "v_ship_type_id",
        "v_ship_name",
        "v_character_id",
        "v_character_name",
        "v_corporation_id",
        "v_corporation_name",
        "v_alliance_id",
        "v_alliance_name",
        "v_faction_id",
        "v_faction_name",
        "fb_ship_type_id",
        "fb_ship_name",
        "fb_character_id",
        "fb_character_name",
        "fb_corporation_id",
        "fb_corporation_name",
        "fb_alliance_id",
        "fb_alliance_name",
        "fb_faction_id",
        "fb_faction_name",
        "war_id",
        "a_character_ids",
        "a_corporation_ids",
        "a_alliance_ids",
        "a_faction_ids",
        "a_ship_type_ids",
        "a_weapon_type_ids",
        "fitted_value",
        "dropped_value",
        "destroyed_value",
        "total_value",
        "total_droppable_value",
        "npc",
        "solo",
        "awox",
        "labels",
    }
)

_SYSTEM_FIELDS = (_GLOBAL_FIELDS - {"solar_system_id"}) | {"x", "y", "z"}


async def _enrich_kill(kill: dict) -> dict:
    """Resolve victim and final-blow names from the DB reference tables."""
    victim_character_id = kill.get("victim_character_id")
    victim_ship_type_id = kill.get("victim_ship_type_id")
    victim_corporation_id = kill.get("victim_corporation_id")
    victim_alliance_id = kill.get("victim_alliance_id")
    victim_faction_id = kill.get("victim_faction_id")

    final_blow = next(
        (a for a in kill.get("attackers", []) if a.get("final_blow")), None
    )
    fb_character_id = final_blow.get("character_id") if final_blow else None
    fb_ship_type_id = final_blow.get("ship_type_id") if final_blow else None
    fb_corporation_id = final_blow.get("corporation_id") if final_blow else None
    fb_alliance_id = final_blow.get("alliance_id") if final_blow else None
    fb_faction_id = final_blow.get("faction_id") if final_blow else None

    character_ids: set[int] = set()
    type_ids: set[int] = set()
    if victim_character_id is not None:
        character_ids.add(victim_character_id)
    if victim_ship_type_id is not None:
        type_ids.add(victim_ship_type_id)
    if fb_character_id is not None:
        character_ids.add(fb_character_id)
    if fb_ship_type_id is not None:
        type_ids.add(fb_ship_type_id)

    corp_ids = list(
        {c for c in [victim_corporation_id, fb_corporation_id] if c is not None}
    )
    alliance_ids = list(
        {a for a in [victim_alliance_id, fb_alliance_id] if a is not None}
    )
    faction_ids = list({f for f in [victim_faction_id, fb_faction_id] if f is not None})

    kill_id = kill.get("killmail_id")

    try:
        character_names, corp_info, alliance_info, faction_names = (
            await entities.fetch_entity_names(
                character_ids, set(corp_ids), set(alliance_ids), set(faction_ids)
            )
        )
        type_names = await get_type_names(type_ids)
        names: dict[int, str] = {**character_names, **type_names}
    except Exception as exc:
        pm.errors.labels(component="broadcaster").inc()
        logger.warning(
            "Kill enrichment DB lookup failed for kill %s: %s",
            kill_id,
            exc or repr(exc),
        )
        names = {}
        corp_info = {}
        alliance_info = {}
        faction_names = {}

    if victim_character_id is not None:
        victim_name = names.get(victim_character_id)
    else:
        victim_name = names.get(victim_ship_type_id) if victim_ship_type_id else None

    victim_corp = (
        corp_info.get(victim_corporation_id) if victim_corporation_id else None
    )
    victim_alliance = (
        alliance_info.get(victim_alliance_id) if victim_alliance_id else None
    )

    if fb_character_id is not None:
        fb_name = names.get(fb_character_id)
    else:
        fb_name = names.get(fb_ship_type_id) if fb_ship_type_id else None

    fb_corp = corp_info.get(fb_corporation_id) if fb_corporation_id else None
    fb_alliance = alliance_info.get(fb_alliance_id) if fb_alliance_id else None

    v_faction_name = faction_names.get(victim_faction_id) if victim_faction_id else None
    fb_faction_name = faction_names.get(fb_faction_id) if fb_faction_id else None

    return {
        "v_character_id": victim_character_id,
        "v_character_name": victim_name,
        "v_ship_name": names.get(victim_ship_type_id) if victim_ship_type_id else None,
        "v_corporation_id": victim_corporation_id,
        "v_corporation_name": victim_corp[0] if victim_corp else None,
        "v_alliance_id": victim_alliance_id,
        "v_alliance_name": victim_alliance[0] if victim_alliance else None,
        "v_faction_name": v_faction_name,
        "fb_character_id": fb_character_id,
        "fb_character_name": fb_name,
        "fb_ship_type_id": fb_ship_type_id,
        "fb_ship_name": names.get(fb_ship_type_id) if fb_ship_type_id else None,
        "fb_corporation_id": fb_corporation_id,
        "fb_corporation_name": fb_corp[0] if fb_corp else None,
        "fb_alliance_id": fb_alliance_id,
        "fb_alliance_name": fb_alliance[0] if fb_alliance else None,
        "fb_faction_id": fb_faction_id,
        "fb_faction_name": fb_faction_name,
    }


def _facet_ids(kill: dict) -> dict:
    """Raw facet ids for client-side live filtering, straight from the parsed
    killmail (no DB). Attacker ids are deduped per attribute, nulls stripped."""
    attackers = kill.get("attackers") or []

    def _distinct(field: str) -> list[int]:
        seen: set[int] = set()
        out: list[int] = []
        for a in attackers:
            v = a.get(field)
            if v is not None and v not in seen:
                seen.add(v)
                out.append(v)
        return out

    return {
        "v_faction_id": kill.get("victim_faction_id"),
        "war_id": kill.get("war_id"),
        "a_character_ids": _distinct("character_id"),
        "a_corporation_ids": _distinct("corporation_id"),
        "a_alliance_ids": _distinct("alliance_id"),
        "a_faction_ids": _distinct("faction_id"),
        "a_ship_type_ids": _distinct("ship_type_id"),
        "a_weapon_type_ids": _distinct("weapon_type_id"),
    }


async def _parse_kill(kill: dict) -> dict:
    # Out-of-range positions (abyssal deadspace, ~1e36) collapse to the (0,0,0)
    # "no position" sentinel, matching the REST binary path.
    x, y, z = sanitize_position(
        kill["position_x"], kill["position_y"], kill["position_z"]
    )
    payload = {
        "killmail_id": kill["killmail_id"],
        "killmail_time": iso_to_epoch(kill["killmail_time"]),
        "solar_system_id": kill["solar_system_id"],
        "x": x,
        "y": y,
        "z": z,
        "v_ship_type_id": kill["victim_ship_type_id"],
        "fitted_value": kill.get("fitted_value"),
        "dropped_value": kill.get("dropped_value"),
        "destroyed_value": kill.get("destroyed_value"),
        "total_value": kill.get("total_value"),
        "total_droppable_value": kill.get("total_droppable_value"),
        "npc": kill.get("npc"),
        "solo": kill.get("solo"),
        "awox": kill.get("awox"),
        "labels": kill.get("labels"),
    }
    payload.update(await _enrich_kill(kill))
    payload.update(_facet_ids(kill))
    return payload


class KillBroadcaster:

    def __init__(self) -> None:
        self._global_subs: set[asyncio.Queue] = set()
        self._system_subs: dict[int, set[asyncio.Queue]] = {}
        self._status_subs: set[asyncio.Queue] = set()
        self._redis: aioredis.Redis | None = None
        self._subscriber_task: asyncio.Task | None = None
        self._leader_tasks: list[asyncio.Task] = []
        self._election_task: asyncio.Task | None = None
        self._is_leader: bool = False
        self._instance_id: str = f"{os.getpid()}-{uuid.uuid4().hex[:8]}"

    async def start(self) -> None:
        if not config.redis_url:
            logger.warning("REDIS_URL not set; live kill streaming disabled")
            return
        self._redis = aioredis.from_url(
            config.redis_url,
            decode_responses=True,
            socket_keepalive=True,
            health_check_interval=30,
        )
        metrics.broadcaster_role = "follower"
        pm.broadcaster_is_leader.set(0)
        self._election_task = asyncio.create_task(self._election_loop())
        self._subscriber_task = asyncio.create_task(self._subscriber_loop())
        logger.info("Kill broadcaster started (instance=%s)", self._instance_id)

    async def stop(self) -> None:
        if self._election_task:
            self._election_task.cancel()
            try:
                await self._election_task
            except asyncio.CancelledError:
                pass
            self._election_task = None
        await self._cancel_leader_tasks()
        if self._subscriber_task:
            self._subscriber_task.cancel()
            try:
                await self._subscriber_task
            except asyncio.CancelledError:
                pass
        if self._is_leader and self._redis:
            try:
                current = await self._redis.get(_LOCK_KEY)
                if current == self._instance_id:
                    await self._redis.delete(_LOCK_KEY)
            except Exception:
                pass
            self._is_leader = False
        if self._redis:
            await self._redis.aclose()
        logger.info("Kill broadcaster stopped")

    async def _resolve_start_id(self) -> str:
        assert self._redis is not None
        try:
            info = await self._redis.xinfo_stream(config.streaming.stream_name)
            return str(info.get("last-generated-id", "$"))
        except RedisResponseError:
            return "$"  # stream does not exist yet
        except (RedisTimeoutError, RedisConnectionError):
            return "$"

    async def _leader_loop(self) -> None:
        assert self._redis is not None  # set in start() before task is created
        last_id = await self._resolve_start_id()
        while True:
            try:
                _raw = await self._redis.xread(
                    {config.streaming.stream_name: last_id}, block=5000, count=10
                )
                # xread stubs include int in return type for pipeline use; cast to list.
                # decode_responses=True (set in start()) means keys/values are str.
                results = cast(list[tuple[str, list[tuple[str, dict[str, str]]]]], _raw)
                if not results:
                    continue
                for _, entries in results:
                    for entry_id, fields in entries:
                        last_id = entry_id
                        pm.stream_entries_read.inc()
                        try:
                            _ms = int(entry_id.split("-")[0])
                            pm.stream_consumer_lag_seconds.set(
                                max(time.time() - _ms / 1000.0, 0.0)
                            )
                        except (ValueError, IndexError):
                            pass
                        try:
                            payload = await _parse_kill(json.loads(fields["data"]))
                        except Exception as exc:
                            logger.warning(
                                "Skipping malformed stream entry %s: %s", entry_id, exc
                            )
                            continue
                        await self._redis.publish(
                            config.streaming.pubsub_channel, json.dumps(payload)
                        )
                        pm.live_events_pushed.inc()
            except asyncio.CancelledError:
                raise
            except (RedisTimeoutError, RedisConnectionError) as exc:
                pm.stream_read_interruptions.inc()
                logger.debug(
                    "Broadcaster leader read interrupted (%s); continuing", exc
                )
                await asyncio.sleep(0.5)
            except Exception as exc:
                pm.errors.labels(component="broadcaster").inc()
                logger.warning("Broadcaster leader error: %s; retrying in 2s", exc)
                await asyncio.sleep(2)

    async def _election_loop(self) -> None:
        assert self._redis is not None  # set in start() before task is created
        while True:
            try:
                await self._election_step()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                pm.errors.labels(component="broadcaster").inc()
                logger.warning("Broadcaster election error: %s", exc)
            await asyncio.sleep(_ELECTION_INTERVAL)

    async def _election_step(self) -> None:
        assert self._redis is not None
        if not config.leader_election:
            return
        if self._is_leader:
            renewed = await self._redis.eval(
                _RENEW_SCRIPT, 1, _LOCK_KEY, self._instance_id, str(_LOCK_TTL)
            )
            if not renewed:
                await self._demote()
        if not self._is_leader:
            acquired = await self._redis.set(
                _LOCK_KEY, self._instance_id, nx=True, ex=_LOCK_TTL
            )
            if acquired:
                await self._promote()

    async def _promote(self) -> None:
        if self._is_leader:
            return
        self._is_leader = True
        metrics.broadcaster_role = "leader"
        pm.broadcaster_is_leader.set(1)
        pm.leader_promotions.inc()
        logger.info("Broadcaster: promoted to leader (instance=%s)", self._instance_id)
        self._leader_tasks = [asyncio.create_task(self._leader_loop())] + [
            asyncio.create_task(self._esi_refresh_loop(feed)) for feed in LEADER_FEEDS
        ]

    async def _demote(self) -> None:
        if not self._is_leader:
            return
        self._is_leader = False
        metrics.broadcaster_role = "follower"
        pm.broadcaster_is_leader.set(0)
        logger.warning(
            "Broadcaster: lost leader lock; demoted to follower (instance=%s)",
            self._instance_id,
        )
        await self._cancel_leader_tasks()

    async def _cancel_leader_tasks(self) -> None:
        for task in self._leader_tasks:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        self._leader_tasks = []

    async def _esi_refresh_once(self, feed: EsiFeed) -> int:
        """Refresh one feed, publish its side effects, and return the next sleep."""
        ttl, value = await esi_client.refresh(feed)
        pm.esi_feed_last_success_timestamp_seconds.labels(
            feed=feed.name
        ).set_to_current_time()
        if self._redis is not None:
            if feed.invalidate_targets:
                await self._redis.publish(
                    config.streaming.invalidate_channel,
                    json.dumps({"targets": list(feed.invalidate_targets)}),
                )
            if feed.broadcast_channel:
                await self._redis.publish(
                    feed.broadcast_channel, json.dumps(feed.decode(value))
                )
        return min(max(ttl - feed.sleep_skew, feed.sleep_min), feed.sleep_max)

    def _retry_delay(self, failures: int) -> int:
        return min(
            config.cache.esi_retry_initial_seconds * 2 ** (failures - 1),
            config.cache.esi_retry_max_seconds,
        )

    async def _esi_refresh_loop(self, feed: EsiFeed) -> None:
        failures = 0
        while True:
            try:
                delay = await self._esi_refresh_once(feed)
                failures = 0
            except asyncio.CancelledError:
                raise
            except EsiFeedRefreshError:
                failures += 1
                delay = self._retry_delay(failures)
            except Exception as exc:
                failures += 1
                pm.errors.labels(component="broadcaster").inc()
                logger.warning("ESI feed %s loop error: %s", feed.name, exc)
                delay = self._retry_delay(failures)
            await asyncio.sleep(delay)

    async def _subscriber_loop(self) -> None:
        assert self._redis is not None  # set in start() before task is created
        pubsub = self._redis.pubsub()
        channels = (config.streaming.pubsub_channel, config.streaming.status_channel)
        await pubsub.subscribe(*channels)
        try:
            async for message in pubsub.listen():
                if message["type"] != "message":
                    continue
                try:
                    payload = json.loads(message["data"])
                except Exception as exc:
                    logger.warning("Broadcaster: malformed pubsub message: %s", exc)
                    continue
                self._dispatch(message["channel"], payload)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            pm.errors.labels(component="broadcaster").inc()
            logger.warning("Broadcaster: subscriber error: %s; retrying in 2s", exc)
            await asyncio.sleep(2)
        finally:
            try:
                await pubsub.unsubscribe(*channels)
                await pubsub.aclose()
            except Exception:
                pass

    def _dispatch(self, channel: str, payload: dict) -> None:
        if channel == config.streaming.pubsub_channel:
            self._fanout(payload)
        elif channel == config.streaming.status_channel:
            self._fanout_status(payload)
        else:
            logger.warning("Broadcaster: dropping message from channel %s", channel)

    def _push(self, subs: set[asyncio.Queue], payload: dict) -> set[asyncio.Queue]:
        """Deliver to every queue; return those that were full, so the caller can
        drop them from its subscriber set."""
        dead: set[asyncio.Queue] = set()
        for q in subs:
            try:
                q.put_nowait(payload)
            except asyncio.QueueFull:
                dead.add(q)
                pm.ws_messages_dropped.inc()
        return dead

    def _fanout(self, payload: dict) -> None:
        system_id: int = payload["solar_system_id"]
        global_payload = {
            k: v for k, v in payload.items() if k in _GLOBAL_FIELDS and v is not None
        }
        system_payload = {
            k: v for k, v in payload.items() if k in _SYSTEM_FIELDS and v is not None
        }

        self._global_subs -= self._push(self._global_subs, global_payload)

        dead = self._push(self._system_subs.get(system_id, set()), system_payload)
        if system_id in self._system_subs:
            self._system_subs[system_id] -= dead

    def _fanout_status(self, payload: dict) -> None:
        self._status_subs -= self._push(self._status_subs, payload)

    @property
    def is_running(self) -> bool:
        return self._subscriber_task is not None and not self._subscriber_task.done()

    @property
    def is_leader(self) -> bool:
        return self._is_leader

    def subscribe_global(self) -> asyncio.Queue:
        q: asyncio.Queue = asyncio.Queue(maxsize=100)
        self._global_subs.add(q)
        metrics.ws_global_connections += 1
        pm.live_clients.labels(transport="ws").inc()
        return q

    def unsubscribe_global(self, q: asyncio.Queue) -> None:
        removed = q in self._global_subs
        self._global_subs.discard(q)
        if removed:
            metrics.ws_global_connections -= 1
            pm.live_clients.labels(transport="ws").dec()

    def subscribe_status(self) -> asyncio.Queue:
        q: asyncio.Queue = asyncio.Queue(maxsize=100)
        self._status_subs.add(q)
        metrics.ws_status_connections += 1
        pm.live_clients.labels(transport="ws_status").inc()
        return q

    def unsubscribe_status(self, q: asyncio.Queue) -> None:
        removed = q in self._status_subs
        self._status_subs.discard(q)
        if removed:
            metrics.ws_status_connections -= 1
            pm.live_clients.labels(transport="ws_status").dec()

    def subscribe_system(self, system_id: int) -> asyncio.Queue:
        q: asyncio.Queue = asyncio.Queue(maxsize=100)
        self._system_subs.setdefault(system_id, set()).add(q)
        metrics.ws_system_connections += 1
        pm.live_clients.labels(transport="ws").inc()
        return q

    def unsubscribe_system(self, system_id: int, q: asyncio.Queue) -> None:
        subs = self._system_subs.get(system_id)
        if subs and q in subs:
            subs.discard(q)
            metrics.ws_system_connections -= 1
            pm.live_clients.labels(transport="ws").dec()
            if not subs:
                del self._system_subs[system_id]


broadcaster = KillBroadcaster()
