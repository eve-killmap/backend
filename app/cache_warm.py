import logging
import time

from app.config import config
from app.global_kills import MAP_RANGES
from app.leaderboards import ROLES, SCOPES, WINDOWS
from app.routers.stats import (
    build_system_kills,
    build_system_rankings,
    build_global_kills,
    build_leaderboards,
)
from app import prometheus_metrics as pm

logger = logging.getLogger(__name__)


async def warm_all() -> None:
    """Recompute + cache the fixed-parameter rollup-derived warm set.

    Leader-only caller (startup, and the invalidation subscriber loop after a
    warmable target's flush). No-ops when ``config.cache.warm_on_signal`` is
    False. Never raises: a warm failure is logged and counted, not propagated,
    so it can't crash the caller (startup or the subscriber loop).
    """
    if not config.cache.warm_on_signal:
        return
    start = time.monotonic()
    try:
        await build_system_kills(None, None, None)  # system-kills all-time
        await build_system_rankings(config.limits.system_rankings_default_limit)
        bins = config.limits.global_kills_default_bins
        for map_type in MAP_RANGES:
            await build_global_kills(map_type, bins, None)
        limit = config.limits.leaderboards_default_limit
        for window in WINDOWS:
            for role in ROLES:
                for scope in SCOPES:
                    await build_leaderboards(window, role, scope, limit)
        pm.cache_warm_seconds.observe(time.monotonic() - start)
        pm.cache_warm_runs_total.labels(outcome="success").inc()
        pm.cache_warm_last_success_timestamp_seconds.set_to_current_time()
    except Exception as exc:
        pm.cache_warm_runs_total.labels(outcome="error").inc()
        pm.errors.labels(component="cache_warm").inc()
        logger.warning("Cache warm failed: %s", exc)
