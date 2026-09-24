from typing import Optional

from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase
from redis.asyncio import Redis

from app.config import Settings, get_settings
from app.logging_config import get_logger

logger = get_logger(__name__)

_mongo_client: Optional[AsyncIOMotorClient] = None
_redis_client: Optional[Redis] = None


async def connect_mongo(settings: Optional[Settings] = None) -> AsyncIOMotorDatabase:
    global _mongo_client
    settings = settings or get_settings()
    _mongo_client = AsyncIOMotorClient(
        settings.mongodb_url,
        serverSelectionTimeoutMS=5000,
    )
    # Fail fast if Mongo is unreachable
    await _mongo_client.admin.command("ping")
    db = _mongo_client[settings.mongodb_database]
    logger.info("mongodb_connected", database=settings.mongodb_database)
    return db


async def connect_redis(settings: Optional[Settings] = None) -> Redis:
    global _redis_client
    settings = settings or get_settings()
    _redis_client = Redis.from_url(
        settings.redis_url,
        decode_responses=True,
        socket_connect_timeout=5,
    )
    await _redis_client.ping()
    logger.info("redis_connected")
    return _redis_client


async def close_mongo() -> None:
    global _mongo_client
    if _mongo_client is not None:
        _mongo_client.close()
        _mongo_client = None
        logger.info("mongodb_disconnected")


async def close_redis() -> None:
    global _redis_client
    if _redis_client is not None:
        await _redis_client.aclose()
        _redis_client = None
        logger.info("redis_disconnected")


def get_mongo_client() -> AsyncIOMotorClient:
    if _mongo_client is None:
        raise RuntimeError("MongoDB client is not initialized")
    return _mongo_client


def get_redis_client() -> Redis:
    if _redis_client is None:
        raise RuntimeError("Redis client is not initialized")
    return _redis_client
