from typing import Annotated, Optional

from fastapi import APIRouter, Depends, Query, status
from motor.motor_asyncio import AsyncIOMotorDatabase
from redis.asyncio import Redis

from app.config import Settings, get_settings
from app.dependencies import get_db, get_redis, require_user_id
from app.models.schemas import (
    DocumentCreate,
    DocumentCreateResponse,
    DocumentResponse,
    DocumentUpdate,
)
from app.services.document_service import DocumentService

router = APIRouter(prefix="/documents", tags=["documents"])


def get_document_service(
    db: Annotated[AsyncIOMotorDatabase, Depends(get_db)],
    redis: Annotated[Redis, Depends(get_redis)],
    settings: Annotated[Settings, Depends(get_settings)],
) -> DocumentService:
    return DocumentService(db, redis, settings)


@router.post(
    "",
    response_model=DocumentCreateResponse,
    status_code=status.HTTP_201_CREATED,
)
async def create_document(
    payload: DocumentCreate,
    service: Annotated[DocumentService, Depends(get_document_service)],
) -> DocumentCreateResponse:
    return await service.create(payload)


@router.get("/by-ref/{client_doc_ref}", response_model=DocumentResponse)
async def get_document_by_ref(
    client_doc_ref: str,
    user_id: Annotated[str, Depends(require_user_id)],
    service: Annotated[DocumentService, Depends(get_document_service)],
) -> DocumentResponse:
    return await service.get_by_ref(client_doc_ref, user_id)


@router.get("/{document_id}", response_model=DocumentResponse)
async def get_document(
    document_id: str,
    user_id: Annotated[str, Depends(require_user_id)],
    service: Annotated[DocumentService, Depends(get_document_service)],
) -> DocumentResponse:
    return await service.get(document_id, user_id)


@router.patch("/{document_id}", response_model=DocumentResponse)
async def patch_document(
    document_id: str,
    payload: DocumentUpdate,
    user_id: Annotated[str, Depends(require_user_id)],
    service: Annotated[DocumentService, Depends(get_document_service)],
    expected_version: Annotated[
        Optional[int],
        Query(
            description=(
                "Optional optimistic concurrency check: reject with 409 if the "
                "document's content_version does not match."
            )
        ),
    ] = None,
) -> DocumentResponse:
    return await service.patch(
        document_id,
        user_id,
        payload,
        expected_version=expected_version,
    )
