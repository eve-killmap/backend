from __future__ import annotations

from dataclasses import dataclass, field

ATTRIBUTE_KINDS = {
    "character": 1, "corporation": 2, "alliance": 3,
    "faction": 4, "ship": 5, "weapon": 6, "war": 7,
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
    return Condition(facet_kind=kind, role=SIDE_ROLES[side], values=_parse_ids(bits[2], max_ids))


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
