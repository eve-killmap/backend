from __future__ import annotations


def validate_id_list(ids: list[int], maximum: int) -> str | None:
    """Return an error message if the id list is unusable, else None."""
    if not ids:
        return "At least one ID is required"
    if len(ids) > maximum:
        return f"Maximum {maximum} IDs allowed per request"
    if any(i <= 0 for i in ids):
        return "IDs must be positive integers"
    return None


def origin_allowed(origin: str | None, allowed: list[str]) -> bool:
    if "*" in allowed:
        return True
    if origin is None:
        return False
    return origin in allowed


def at_capacity(current: int, maximum: int) -> bool:
    return current >= maximum


def security_headers() -> dict[str, str]:
    return {
        "X-Content-Type-Options": "nosniff",
        "Referrer-Policy": "no-referrer",
        "X-Frame-Options": "DENY",
    }
