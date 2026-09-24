from typing import Annotated, Optional

from fastapi import APIRouter, Depends, Query
from motor.motor_asyncio import AsyncIOMotorDatabase
from redis.asyncio import Redis

from app.config import Settings, get_settings
from app.dependencies import get_db, get_redis
from app.models.document import DocumentStatus
from app.models.schemas import DocumentListResponse
from app.services.document_service import DocumentService

router = APIRouter(prefix="/users", tags=["users"])


def get_document_service(
    db: Annotated[AsyncIOMotorDatabase, Depends(get_db)],
    redis: Annotated[Redis, Depends(get_redis)],
    settings: Annotated[Settings, Depends(get_settings)],
) -> DocumentService:
    return DocumentService(db, redis, settings)


@router.get("/{user_id}/documents", response_model=DocumentListResponse)
async def list_user_documents(
    user_id: str,
    service: Annotated[DocumentService, Depends(get_document_service)],
    page: Annotated[int, Query(ge=1)] = 1,
    page_size: Annotated[int, Query(ge=1, le=100)] = 20,
    status: Annotated[Optional[DocumentStatus], Query()] = None,
) -> DocumentListResponse:
    return await service.list_for_user(
        user_id,
        page=page,
        page_size=page_size,
        status_filter=status,
    )
