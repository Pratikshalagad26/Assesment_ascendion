import asyncio
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.config import get_settings
from app.database import close_mongo, close_redis, connect_mongo, connect_redis
from app.logging_config import configure_logging, get_logger
from app.repositories.document_repository import DocumentRepository
from app.routers import documents, health, users

logger = get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    configure_logging()
    settings = get_settings()

    db = None
    redis = None
    for attempt in range(1, 31):
        try:
            db = await connect_mongo(settings)
            redis = await connect_redis(settings)
            break
        except Exception as exc:
            logger.warning(
                "api_dependency_wait",
                attempt=attempt,
                error=str(exc),
            )
            await asyncio.sleep(2)
    else:
        raise RuntimeError("Could not connect to MongoDB/Redis after retries")

    repo = DocumentRepository(db)
    await repo.ensure_indexes()
    logger.info("api_started")

    yield

    await close_redis()
    await close_mongo()
    logger.info("api_stopped")


app = FastAPI(
    title="Cohort Insights API",
    description="Ingest, process, and serve documents through a two-stage pipeline.",
    version="1.0.0",
    lifespan=lifespan,
)

app.include_router(health.router)
app.include_router(documents.router)
app.include_router(users.router)
