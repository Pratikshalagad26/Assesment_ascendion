"""
Shared fixtures.

Uses mongomock-motor and fakeredis so tests run without Docker.
Delays and failure rates are zeroed by default; individual tests override them.
"""

from __future__ import annotations

import os
from collections.abc import AsyncIterator
from typing import Any
from unittest.mock import patch

import pytest
import pytest_asyncio
from fakeredis import aioredis as fake_aioredis
from httpx import ASGITransport, AsyncClient
from mongomock_motor import AsyncMongoMockClient

# Force test-friendly defaults before app imports settings
os.environ.setdefault("MONGODB_URL", "mongodb://localhost:27017")
os.environ.setdefault("MONGODB_DATABASE", "cohort_insights_test")
os.environ.setdefault("REDIS_URL", "redis://localhost:6379/15")
os.environ.setdefault("PROCESSING_DELAY_MIN", "0")
os.environ.setdefault("PROCESSING_DELAY_MAX", "0")
os.environ.setdefault("ENRICHING_DELAY_MIN", "0")
os.environ.setdefault("ENRICHING_DELAY_MAX", "0")
os.environ.setdefault("STAGE_FAILURE_RATE", "0")
os.environ.setdefault("CACHE_TTL_SECONDS", "3600")
os.environ.setdefault("MAX_ACTIVE_PIPELINES", "3")
os.environ.setdefault("RATE_LIMIT_ON_REDIS_FAILURE", "open")
os.environ.setdefault("WORKER_POLL_INTERVAL_SECONDS", "0.01")

from app.config import Settings, get_settings
from app.database import close_mongo, close_redis
from app import database as database_module
from app.main import app
from app.repositories.document_repository import DocumentRepository
from app.services.document_service import DocumentService
from app.workers.pipeline_worker import PipelineWorker


@pytest.fixture
def settings() -> Settings:
    get_settings.cache_clear()
    s = Settings(
        mongodb_url="mongodb://localhost:27017",
        mongodb_database="cohort_insights_test",
        redis_url="redis://localhost:6379/15",
        processing_delay_min=0,
        processing_delay_max=0,
        enriching_delay_min=0,
        enriching_delay_max=0,
        stage_failure_rate=0.0,
        cache_ttl_seconds=3600,
        max_active_pipelines=3,
        rate_limit_on_redis_failure="open",
        worker_poll_interval_seconds=0.01,
    )
    get_settings.cache_clear()
    with patch("app.config.get_settings", return_value=s):
        yield s
    get_settings.cache_clear()


@pytest_asyncio.fixture
async def mongo_db(settings: Settings):
    client = AsyncMongoMockClient()
    db = client[settings.mongodb_database]
    database_module._mongo_client = client  # type: ignore[assignment]
    repo = DocumentRepository(db)
    await repo.ensure_indexes()
    yield db
    database_module._mongo_client = None
    client.close()


@pytest_asyncio.fixture
async def redis_client(settings: Settings):
    redis = fake_aioredis.FakeRedis(decode_responses=True)
    database_module._redis_client = redis
    yield redis
    await redis.flushall()
    await redis.aclose()
    database_module._redis_client = None


@pytest_asyncio.fixture
async def service(mongo_db, redis_client, settings: Settings) -> DocumentService:
    return DocumentService(mongo_db, redis_client, settings)


@pytest_asyncio.fixture
async def worker(mongo_db, redis_client, settings: Settings) -> PipelineWorker:
    return PipelineWorker(mongo_db, redis_client, settings)


async def drain_pipeline(worker: PipelineWorker, max_rounds: int = 20) -> None:
    """Run the worker until the queue is idle."""
    for _ in range(max_rounds):
        if not await worker.poll_once():
            # One empty poll is enough when delays are zero
            if not await worker.poll_once():
                return
    raise AssertionError("Pipeline did not drain within max_rounds")


@pytest_asyncio.fixture
async def client(mongo_db, redis_client, settings: Settings) -> AsyncIterator[AsyncClient]:
    """HTTP client bound to the FastAPI app with mocked dependencies already wired."""

    async def _noop_lifespan(app_instance):
        yield

    # Bypass real lifespan (which waits on real Mongo/Redis)
    app.router.lifespan_context = _noop_lifespan

    get_settings.cache_clear()
    with patch("app.config.get_settings", return_value=settings):
        with patch("app.dependencies.get_settings", return_value=settings):
            transport = ASGITransport(app=app)
            async with AsyncClient(transport=transport, base_url="http://test") as ac:
                yield ac

    get_settings.cache_clear()


@pytest.fixture
def sample_content() -> dict[str, Any]:
    return {
        "user_id": "user-1",
        "title": "Quarterly Notes",
        "content": (
            "Cohort insights help product teams understand retention patterns "
            "across weekly active users and engagement segments."
        ),
    }
