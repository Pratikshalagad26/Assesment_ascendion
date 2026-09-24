from typing import Annotated, Optional

from fastapi import Depends, Header, HTTPException, Query, status
from motor.motor_asyncio import AsyncIOMotorDatabase
from redis.asyncio import Redis

from app.config import Settings, get_settings
from app.database import get_mongo_client, get_redis_client


def get_db(settings: Annotated[Settings, Depends(get_settings)]) -> AsyncIOMotorDatabase:
    client = get_mongo_client()
    return client[settings.mongodb_database]


def get_redis() -> Redis:
    return get_redis_client()


def require_user_id(
    user_id: Annotated[
        Optional[str],
        Query(description="Owning user id (required for ownership checks)"),
    ] = None,
    x_user_id: Annotated[
        Optional[str],
        Header(alias="X-User-Id", description="Alternative ownership header"),
    ] = None,
) -> str:
    """Resolve the caller identity from query param or X-User-Id header."""
    resolved = (user_id or x_user_id or "").strip()
    if not resolved:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="user_id query parameter or X-User-Id header is required",
        )
    return resolved
