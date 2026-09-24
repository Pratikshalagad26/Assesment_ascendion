from typing import Annotated

from fastapi import APIRouter, Depends, status
from fastapi.responses import JSONResponse
from motor.motor_asyncio import AsyncIOMotorDatabase
from redis.asyncio import Redis
from redis.exceptions import RedisError

from app.dependencies import get_db, get_redis
from app.logging_config import get_logger
from app.models.schemas import HealthResponse

router = APIRouter(tags=["health"])
logger = get_logger(__name__)


@router.get("/health", response_model=HealthResponse)
async def health(
    db: Annotated[AsyncIOMotorDatabase, Depends(get_db)],
    redis: Annotated[Redis, Depends(get_redis)],
) -> JSONResponse:
    mongo_status = "ok"
    redis_status = "ok"

    try:
        await db.command("ping")
    except Exception as exc:
        logger.error("health_mongo_failed", error=str(exc))
        mongo_status = "unavailable"

    try:
        await redis.ping()
    except RedisError as exc:
        logger.error("health_redis_failed", error=str(exc))
        redis_status = "unavailable"

    overall = "ok" if mongo_status == "ok" and redis_status == "ok" else "degraded"
    body = HealthResponse(status=overall, mongodb=mongo_status, redis=redis_status)
    code = (
        status.HTTP_200_OK
        if overall == "ok"
        else status.HTTP_503_SERVICE_UNAVAILABLE
    )
    return JSONResponse(status_code=code, content=body.model_dump())
