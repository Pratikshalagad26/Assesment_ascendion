import json
from typing import Optional

from redis.asyncio import Redis
from redis.exceptions import RedisError

from app.config import Settings
from app.logging_config import get_logger

logger = get_logger(__name__)


class CacheService:
    """Content-hash keyed cache. Independent of document_id."""

    def __init__(self, redis: Redis, settings: Settings):
        self._redis = redis
        self._ttl = settings.cache_ttl_seconds

    @staticmethod
    def _key(content_hash: str) -> str:
        return f"content:{content_hash}"

    async def get(self, content_hash: str) -> Optional[dict]:
        try:
            raw = await self._redis.get(self._key(content_hash))
        except RedisError as exc:
            logger.warning("cache_get_failed", content_hash=content_hash, error=str(exc))
            return None

        if raw is None:
            return None
        try:
            data = json.loads(raw)
        except json.JSONDecodeError as exc:
            logger.warning("cache_corrupt", content_hash=content_hash, error=str(exc))
            return None

        if not isinstance(data, dict) or "summary" not in data or "tags" not in data:
            logger.warning("cache_invalid_shape", content_hash=content_hash)
            return None
        return data

    async def set(self, content_hash: str, *, summary: str, tags: list[str]) -> None:
        payload = json.dumps({"summary": summary, "tags": tags})
        try:
            await self._redis.set(self._key(content_hash), payload, ex=self._ttl)
        except RedisError as exc:
            logger.warning("cache_set_failed", content_hash=content_hash, error=str(exc))
