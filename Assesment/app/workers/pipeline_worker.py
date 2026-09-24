"""Background worker that drives the two-stage processing pipeline."""

from __future__ import annotations

import asyncio
import signal
from typing import Optional

from motor.motor_asyncio import AsyncIOMotorDatabase
from redis.asyncio import Redis

from app.config import Settings, get_settings
from app.database import close_mongo, close_redis, connect_mongo, connect_redis
from app.logging_config import configure_logging, get_logger
from app.models.document import DocumentStatus
from app.repositories.document_repository import DocumentRepository
from app.services.cache_service import CacheService
from app.services.pipeline_service import PipelineService, StageFailed
from app.services.rate_limit_service import RateLimitService

logger = get_logger(__name__)


class PipelineWorker:
    def __init__(
        self,
        db: AsyncIOMotorDatabase,
        redis: Redis,
        settings: Settings,
    ):
        self._repo = DocumentRepository(db)
        self._cache = CacheService(redis, settings)
        self._rate_limit = RateLimitService(redis, settings)
        self._pipeline = PipelineService(settings)
        self._settings = settings
        self._running = True

    def stop(self) -> None:
        self._running = False

    async def run_forever(self) -> None:
        logger.info("worker_started")
        while self._running:
            did_work = await self.poll_once()
            if not did_work:
                await asyncio.sleep(self._settings.worker_poll_interval_seconds)
        logger.info("worker_stopped")

    async def poll_once(self) -> bool:
        """Claim and process at most one job. Returns True if work was done."""
        claimed = await self._repo.claim_for_processing()
        if claimed is not None:
            await self._run_processing(claimed)
            return True

        claimed = await self._repo.claim_for_enriching()
        if claimed is not None:
            await self._run_enriching(claimed)
            return True

        return False

    async def _run_processing(self, doc: dict) -> None:
        document_id = str(doc["_id"])
        version = int(doc["content_version"])
        stage_lock = doc["stage_lock"]
        user_id = doc["user_id"]
        content_hash = doc["content_hash"]

        logger.info(
            "processing_claim",
            document_id=document_id,
            content_version=version,
        )

        cached = await self._cache.get(content_hash)
        if cached is not None:
            result = await self._repo.apply_cached_result(
                document_id,
                content_version=version,
                summary=cached["summary"],
                tags=cached["tags"],
                from_status=DocumentStatus.PROCESSING,
                stage_lock=stage_lock,
            )
            if result is not None:
                await self._rate_limit.release(user_id)
                logger.info("processing_cache_completed", document_id=document_id)
            else:
                logger.info(
                    "processing_stale_after_cache",
                    document_id=document_id,
                    content_version=version,
                )
            return

        try:
            summary = await self._pipeline.run_processing(doc["content"])
        except StageFailed as exc:
            result = await self._repo.fail_processing(
                document_id,
                content_version=version,
                stage_lock=stage_lock,
                error_message=str(exc),
            )
            if result is not None:
                await self._rate_limit.release(user_id)
                logger.warning(
                    "processing_failed",
                    document_id=document_id,
                    content_version=version,
                )
            else:
                logger.info(
                    "processing_fail_stale",
                    document_id=document_id,
                    content_version=version,
                )
            return

        result = await self._repo.complete_processing(
            document_id,
            content_version=version,
            stage_lock=stage_lock,
            summary=summary,
        )
        if result is None:
            logger.info(
                "processing_complete_stale",
                document_id=document_id,
                content_version=version,
            )
            return

        logger.info(
            "processing_complete",
            document_id=document_id,
            content_version=version,
        )

    async def _run_enriching(self, doc: dict) -> None:
        document_id = str(doc["_id"])
        version = int(doc["content_version"])
        stage_lock = doc["stage_lock"]
        user_id = doc["user_id"]
        content_hash = doc["content_hash"]
        summary = doc.get("summary")

        if summary is None or doc.get("summary_for_version") != version:
            logger.warning(
                "enriching_missing_summary",
                document_id=document_id,
                content_version=version,
            )
            await self._repo.release_stage_lock(document_id, stage_lock=stage_lock)
            return

        logger.info(
            "enriching_claim",
            document_id=document_id,
            content_version=version,
        )

        try:
            tags = await self._pipeline.run_enriching(summary)
        except StageFailed as exc:
            result = await self._repo.fail_enriching(
                document_id,
                content_version=version,
                stage_lock=stage_lock,
                error_message=str(exc),
            )
            if result is not None:
                await self._rate_limit.release(user_id)
                logger.warning(
                    "enriching_failed",
                    document_id=document_id,
                    content_version=version,
                )
            else:
                logger.info(
                    "enriching_fail_stale",
                    document_id=document_id,
                    content_version=version,
                )
            return

        result = await self._repo.complete_enriching(
            document_id,
            content_version=version,
            stage_lock=stage_lock,
            tags=tags,
        )
        if result is None:
            logger.info(
                "enriching_complete_stale",
                document_id=document_id,
                content_version=version,
            )
            return

        await self._cache.set(content_hash, summary=summary, tags=tags)
        await self._rate_limit.release(user_id)
        logger.info(
            "enriching_complete",
            document_id=document_id,
            content_version=version,
        )


async def main() -> None:
    configure_logging()
    settings = get_settings()

    db: Optional[AsyncIOMotorDatabase] = None
    redis: Optional[Redis] = None

    for attempt in range(1, 31):
        try:
            db = await connect_mongo(settings)
            redis = await connect_redis(settings)
            break
        except Exception as exc:
            logger.warning(
                "worker_dependency_wait",
                attempt=attempt,
                error=str(exc),
            )
            await asyncio.sleep(2)
    else:
        raise RuntimeError("Could not connect to MongoDB/Redis after retries")

    assert db is not None and redis is not None
    repo = DocumentRepository(db)
    await repo.ensure_indexes()

    worker = PipelineWorker(db, redis, settings)
    loop = asyncio.get_running_loop()

    def _handle_signal() -> None:
        logger.info("worker_signal_received")
        worker.stop()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _handle_signal)
        except NotImplementedError:
            signal.signal(sig, lambda *_: _handle_signal())

    try:
        await worker.run_forever()
    finally:
        await close_redis()
        await close_mongo()


if __name__ == "__main__":
    asyncio.run(main())
