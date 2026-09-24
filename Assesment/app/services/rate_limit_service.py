from redis.asyncio import Redis
from redis.exceptions import RedisError

from app.config import Settings
from app.logging_config import get_logger

logger = get_logger(__name__)


class RateLimitExceeded(Exception):
    """Raised when the user already has max active pipelines."""


class RateLimitUnavailable(Exception):
    """Raised when Redis is down and fail-closed mode is configured."""


class RateLimitService:
    """
    Tracks per-user active pipeline counts in Redis.

    Uses INCR + conditional DECR. Concurrent submits that both cross the limit
    each roll back their own increment, so the counter does not drift upward.
    """

    def __init__(self, redis: Redis, settings: Settings):
        self._redis = redis
        self._limit = settings.max_active_pipelines
        self._on_failure = settings.rate_limit_on_redis_failure.lower()

    @staticmethod
    def _key(user_id: str) -> str:
        return f"active_pipelines:{user_id}"

    async def try_acquire(self, user_id: str) -> None:
        """Increment active count if under limit; otherwise raise RateLimitExceeded."""
        key = self._key(user_id)
        try:
            current = await self._redis.incr(key)
            if current > self._limit:
                await self._redis.decr(key)
                raise RateLimitExceeded(
                    f"User {user_id} already has {self._limit} active pipelines"
                )
        except RateLimitExceeded:
            raise
        except RedisError as exc:
            logger.error("rate_limit_redis_error", user_id=user_id, error=str(exc))
            if self._on_failure == "closed":
                raise RateLimitUnavailable(
                    "Rate limiting unavailable; refusing submission"
                ) from exc
            logger.warning("rate_limit_fail_open", user_id=user_id)

    async def release(self, user_id: str) -> None:
        """Decrement active count, never going below zero."""
        key = self._key(user_id)
        try:
            value = await self._redis.decr(key)
            if value < 0:
                await self._redis.set(key, 0)
        except RedisError as exc:
            logger.error("rate_limit_release_failed", user_id=user_id, error=str(exc))

    async def get_count(self, user_id: str) -> int:
        try:
            raw = await self._redis.get(self._key(user_id))
        except RedisError as exc:
            logger.warning("rate_limit_get_failed", user_id=user_id, error=str(exc))
            return 0
        return int(raw) if raw is not None else 0
