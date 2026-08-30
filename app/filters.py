from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone

ATTRIBUTE_KINDS = {
    "character": 1,
    "corporation": 2,
    "alliance": 3,
    "faction": 4,
    "ship": 5,
    "weapon": 6,
    "war": 7,
}
SIDE_ROLES: dict[str, int | None] = {"victim": 0, "attacker": 1, "involved": None}


class FilterError(ValueError):
    """Raised for a malformed filter; surfaced by endpoints as HTTP 400."""


@dataclass(frozen=True)
class Condition:
    facet_kind: int
    role: int | None
    values: tuple[int, ...]
    war_any: bool = False


@dataclass(frozen=True)
class Filter:
    conditions: tuple[Condition, ...]

    @property
    def is_empty(self) -> bool:
        return not self.conditions

    def canonical(self) -> str:
        parts = []
        for c in self.conditions:
            role = "i" if c.role is None else str(c.role)
            vals = "any" if c.war_any else ",".join(str(v) for v in c.values)
            parts.append(f"{c.facet_kind}:{role}:{vals}")
        return "|".join(parts)


def _parse_ids(raw: str, max_ids: int) -> tuple[int, ...]:
    if raw == "":
        raise FilterError("condition has no ids")
    out: list[int] = []
    for token in raw.split(","):
        token = token.strip()
        try:
            v = int(token)
        except ValueError:
            raise FilterError(f"non-integer id: {token!r}")
        if v <= 0:
            raise FilterError(f"non-positive id: {v}")
        out.append(v)
    if len(out) > max_ids:
        raise FilterError(f"too many ids in one condition (>{max_ids})")
    return tuple(sorted(set(out)))


def _parse_one(raw: str, max_ids: int) -> Condition:
    bits = raw.split(":")
    attr = bits[0]
    kind = ATTRIBUTE_KINDS.get(attr)
    if kind is None:
        raise FilterError(f"unknown attribute: {attr!r}")

    if attr == "war":
        if len(bits) != 2:
            raise FilterError("war condition must be 'war:<ids>' or 'war:any'")
        if bits[1] == "any":
            return Condition(facet_kind=7, role=None, values=(), war_any=True)
        return Condition(facet_kind=7, role=None, values=_parse_ids(bits[1], max_ids))

    if attr == "weapon":
        if len(bits) != 2:
            raise FilterError("weapon condition must be 'weapon:<ids>' (no side)")
        return Condition(facet_kind=6, role=1, values=_parse_ids(bits[1], max_ids))

    # entity / ship: attr:side:ids
    if len(bits) != 3:
        raise FilterError(f"condition must be '{attr}:<side>:<ids>'")
    side = bits[1]
    if side not in SIDE_ROLES:
        raise FilterError(f"unknown side: {side!r}")
    return Condition(
        facet_kind=kind, role=SIDE_ROLES[side], values=_parse_ids(bits[2], max_ids)
    )


def _sort_key(c: Condition) -> tuple:
    role_sort = -1 if c.role is None else c.role
    return (c.facet_kind, role_sort, 0 if c.war_any else 1, c.values)


def parse_filter(raw: list[str], *, max_conditions: int, max_ids: int) -> Filter:
    parsed = [_parse_one(r, max_ids) for r in raw if r != ""]
    # dedup exact conditions, then canonical sort
    unique = tuple(sorted(set(parsed), key=_sort_key))
    if len(unique) > max_conditions:
        raise FilterError(f"too many conditions (>{max_conditions})")
    return Filter(conditions=unique)


# lower rank = more selective -> better driver. Tunable.
DRIVER_RANK = {1: 0, 2: 1, 3: 2, 6: 3, 5: 4, 4: 5, 7: 6}


def _bucket_sql(time_col: str) -> str:
    # single all-time count; the window predicate (Task 3) narrows the WHERE, not this.
    return "COUNT(DISTINCT killmail_id) AS kill_count"


def _bin_expr(day_col: str, bins_ph: str, earliest_ph: str) -> str:
    """SQL expression bucketing ``day_col`` (a date) into one of ``bins`` equal-width
    bins over the fixed axis [earliest, CURRENT_DATE]. ``bins_ph``/``earliest_ph`` are
    ``$N`` placeholder strings supplied by the caller. Shared by the unfiltered
    (rollup ``day``) and filtered (``killmail_time::date``) global-kills paths so
    their bin boundaries are identical."""
    return (
        f"LEAST({bins_ph} - 1, "
        f"GREATEST(0, floor(({day_col} - {earliest_ph}::date) * {bins_ph} "
        f"/ GREATEST(1, (CURRENT_DATE - {earliest_ph}::date)))::int))"
    )


def _pred(cond: Condition, params: list, alias: str = "") -> str:
    """Append this condition's params and return its SQL predicate."""
    p = f"{alias}." if alias else ""
    parts: list[str] = []
    params.append(cond.facet_kind)
    parts.append(f"{p}facet_kind = ${len(params)}")
    if not cond.war_any:
        params.append(list(cond.values))
        parts.append(f"{p}facet_value = ANY(${len(params)}::bigint[])")
    if cond.role is not None:
        params.append(cond.role)
        parts.append(f"{p}role = ${len(params)}")
    return " AND ".join(parts)


def _exists(cond: Condition, params: list) -> str:
    inner = _pred(cond, params, alias="g")
    return (
        "EXISTS (SELECT 1 FROM kill_facets g "
        f"WHERE g.killmail_id = f.killmail_id AND {inner})"
    )


def _midnight(d: date) -> datetime:
    return datetime(d.year, d.month, d.day, tzinfo=timezone.utc)


def _window_preds(
    start: date | None, end: date | None, params: list, alias: str = ""
) -> list[str]:
    """Append optional window bounds to params; return their SQL predicates
    (referencing killmail_time at UTC midnight for the given dates)."""
    p = f"{alias}." if alias else ""
    preds: list[str] = []
    if start is not None:
        params.append(_midnight(start))
        preds.append(f"{p}killmail_time >= ${len(params)}")
    if end is not None:
        params.append(_midnight(end))
        preds.append(f"{p}killmail_time < ${len(params)}")
    return preds


def _split_driver(f: Filter) -> tuple[Condition, list[Condition]]:
    if not f.conditions:
        raise FilterError("cannot build SQL for an empty filter")
    driver_i = min(
        range(len(f.conditions)),
        key=lambda i: DRIVER_RANK[f.conditions[i].facet_kind],
    )
    driver = f.conditions[driver_i]
    others = [c for j, c in enumerate(f.conditions) if j != driver_i]
    return driver, others


def build_map_sql(
    f: Filter, start: date | None = None, end: date | None = None
) -> tuple[str, list]:
    params: list = []
    if len(f.conditions) == 1:
        where = _pred(f.conditions[0], params)
        for pred in _window_preds(start, end, params):
            where += f" AND {pred}"
        sql = f"""
        SELECT solar_system_id,
          {_bucket_sql("killmail_time")}
        FROM kill_facets
        WHERE {where}
        GROUP BY solar_system_id
        """
        return sql, params

    driver, others = _split_driver(f)
    where = _pred(driver, params, alias="f")
    for pred in _window_preds(start, end, params, alias="f"):
        where += f" AND {pred}"
    exists_sql = "\n            AND ".join(_exists(c, params) for c in others)
    sql = f"""
        SELECT solar_system_id,
          {_bucket_sql("m.killmail_time")}
        FROM (
          SELECT f.killmail_id, f.solar_system_id, f.killmail_time
          FROM kill_facets f
          WHERE {where}
            AND {exists_sql}
        ) m
        GROUP BY solar_system_id
        """
    return sql, params


def build_system_sql(f: Filter, solar_system_id: int) -> tuple[str, list]:
    params: list = []
    if len(f.conditions) == 1:
        where = _pred(f.conditions[0], params)
        params.append(solar_system_id)
        sql = f"""
        SELECT DISTINCT killmail_id
        FROM kill_facets
        WHERE {where} AND solar_system_id = ${len(params)}
        """
        return sql, params

    driver, others = _split_driver(f)
    where = _pred(driver, params, alias="f")
    params.append(solar_system_id)
    system_clause = f"f.solar_system_id = ${len(params)}"
    exists_sql = "\n            AND ".join(_exists(c, params) for c in others)
    sql = f"""
        SELECT DISTINCT f.killmail_id
        FROM kill_facets f
        WHERE {where} AND {system_clause}
            AND {exists_sql}
        """
    return sql, params


def build_global_kills_sql(
    f: Filter, lo: int, hi: int, bins: int, earliest: date
) -> tuple[str, list]:
    params: list = []
    if len(f.conditions) == 1:
        where = _pred(f.conditions[0], params)
        params.append(lo)
        lo_ph = f"${len(params)}"
        params.append(hi)
        hi_ph = f"${len(params)}"
        params.append(bins)
        bins_ph = f"${len(params)}"
        params.append(earliest)
        earliest_ph = f"${len(params)}"
        sql = f"""
        SELECT {_bin_expr("killmail_time::date", bins_ph, earliest_ph)} AS bin,
               COUNT(DISTINCT killmail_id) AS kill_count
        FROM kill_facets
        WHERE {where}
          AND solar_system_id >= {lo_ph} AND solar_system_id < {hi_ph}
        GROUP BY bin
        """
        return sql, params

    driver, others = _split_driver(f)
    where = _pred(driver, params, alias="f")
    params.append(lo)
    lo_ph = f"${len(params)}"
    params.append(hi)
    hi_ph = f"${len(params)}"
    exists_sql = "\n            AND ".join(_exists(c, params) for c in others)
    params.append(bins)
    bins_ph = f"${len(params)}"
    params.append(earliest)
    earliest_ph = f"${len(params)}"
    sql = f"""
        SELECT {_bin_expr("killmail_time::date", bins_ph, earliest_ph)} AS bin,
               COUNT(DISTINCT killmail_id) AS kill_count
        FROM (
          SELECT f.killmail_id, f.killmail_time
          FROM kill_facets f
          WHERE {where}
            AND f.solar_system_id >= {lo_ph} AND f.solar_system_id < {hi_ph}
            AND {exists_sql}
        ) m
        GROUP BY bin
        """
    return sql, params
