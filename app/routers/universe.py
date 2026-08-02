from typing import Annotated

from fastapi import APIRouter, Body, HTTPException

from app.config import config
from app.security import validate_id_list
from app.queries import get_type_names

router = APIRouter()


@router.post("/universe/names")
async def resolve_universe_names(
    ids: Annotated[list[int], Body()],
) -> dict[int, str]:
    """Resolve ship type names for a list of type IDs (from the `types` table)."""
    error = validate_id_list(ids, config.limits.max_name_ids)
    if error is not None:
        raise HTTPException(status_code=400, detail=error)
    return await get_type_names(set(ids))
