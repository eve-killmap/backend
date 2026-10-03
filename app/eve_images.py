IMAGE_BASE = "https://images.evetech.net"
IMAGE_SIZE = 32


def image_url(category: str, entity_id: int) -> str:
    if category == "character":
        return f"{IMAGE_BASE}/characters/{entity_id}/portrait?size={IMAGE_SIZE}"
    if category in ("corporation", "faction"):
        return f"{IMAGE_BASE}/corporations/{entity_id}/logo?size={IMAGE_SIZE}"
    if category == "alliance":
        return f"{IMAGE_BASE}/alliances/{entity_id}/logo?size={IMAGE_SIZE}"
    if category == "type":
        return f"{IMAGE_BASE}/types/{entity_id}/icon?size={IMAGE_SIZE}"
    raise ValueError(f"unknown image category: {category!r}")
