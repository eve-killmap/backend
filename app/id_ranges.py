CATEGORY_TYPE = "type"
CATEGORY_FACTION = "faction"
CATEGORY_CHARACTER = "character"
CATEGORY_CORPORATION = "corporation"
CATEGORY_ALLIANCE = "alliance"
CATEGORY_AMBIGUOUS = "ambiguous"


def classify_id(entity_id: int) -> str:
    """Map an EVE id to a reference-table category by CCP's published ranges.

    The legacy range (100M-2.111B) is shared by characters, corporations and
    alliances and cannot be told apart by number; callers resolve those by
    probing all three entity tables. Anything outside the known ranges also
    falls back to AMBIGUOUS (probe, then omit if unresolved).
    """
    if 0 <= entity_id <= 499_999:
        return CATEGORY_TYPE
    if 500_000 <= entity_id <= 599_999:
        return CATEGORY_FACTION
    if 1_000_000 <= entity_id <= 1_999_999:
        return CATEGORY_CORPORATION
    if 3_000_000 <= entity_id <= 3_999_999:
        return CATEGORY_CHARACTER
    if 90_000_000 <= entity_id <= 97_999_999:
        return CATEGORY_CHARACTER
    if 98_000_000 <= entity_id <= 98_999_999:
        return CATEGORY_CORPORATION
    if 99_000_000 <= entity_id <= 99_999_999:
        return CATEGORY_ALLIANCE
    if 2_112_000_000 <= entity_id <= 2_129_999_999:
        return CATEGORY_CHARACTER
    return CATEGORY_AMBIGUOUS
