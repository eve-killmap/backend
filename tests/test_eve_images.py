import pytest
from app.eve_images import image_url


def test_character_portrait():
    assert (
        image_url("character", 91072482)
        == "https://images.evetech.net/characters/91072482/portrait?size=32"
    )


def test_corporation_and_faction_use_corporations_logo():
    assert (
        image_url("corporation", 98000001)
        == "https://images.evetech.net/corporations/98000001/logo?size=32"
    )
    assert (
        image_url("faction", 500003)
        == "https://images.evetech.net/corporations/500003/logo?size=32"
    )


def test_alliance_logo_and_type_icon():
    assert (
        image_url("alliance", 99005338)
        == "https://images.evetech.net/alliances/99005338/logo?size=32"
    )
    assert (
        image_url("type", 2929) == "https://images.evetech.net/types/2929/icon?size=32"
    )


def test_unknown_category_raises():
    with pytest.raises(ValueError):
        image_url("wormhole", 1)
