IMAGE_BASE = "https://images.evetech.net"
IMAGE_SIZE = 32


def image_url(category: str, entity_id: int) -> str:
    """Build an evetech image-server URL (32px) for an entity/type id.

    Factions have no dedicated endpoint; per CCP they use the corporations logo.
    """
    if category == "character":
        return f"{IMAGE_BASE}/characters/{entity_id}/portrait?size={IMAGE_SIZE}"
    if category in ("corporation", "faction"):
        return f"{IMAGE_BASE}/corporations/{entity_id}/logo?size={IMAGE_SIZE}"
    if category == "alliance":
        return f"{IMAGE_BASE}/alliances/{entity_id}/logo?size={IMAGE_SIZE}"
    if category == "type":
        return f"{IMAGE_BASE}/types/{entity_id}/icon?size={IMAGE_SIZE}"
    raise ValueError(f"unknown image category: {category!r}")
