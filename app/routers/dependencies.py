from fastapi import Query, HTTPException

from app.config import config
from app.filters import Filter, parse_filter, FilterError


def get_filter(f: list[str] = Query(default=[])) -> Filter:
    try:
        return parse_filter(
            f,
            max_conditions=config.limits.max_filter_conditions,
            max_ids=config.limits.max_filter_ids_per_condition,
        )
    except FilterError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
