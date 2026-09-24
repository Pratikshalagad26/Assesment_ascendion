from datetime import datetime, timezone
from typing import Any, Optional

from fastapi import HTTPException, status
from motor.motor_asyncio import AsyncIOMotorDatabase
from pymongo.errors import DuplicateKeyError
from redis.asyncio import Redis

from app.config import Settings
from app.logging_config import get_logger
from app.models.document import ACTIVE_STATUSES, DocumentStatus, FailedStage
from app.models.schemas import (
    DocumentCreate,
    DocumentCreateResponse,
    DocumentListResponse,
    DocumentResponse,
    DocumentUpdate,
)
from app.repositories.document_repository import DocumentRepository
from app.services.cache_service import CacheService
from app.services.rate_limit_service import (
    RateLimitExceeded,
    RateLimitService,
    RateLimitUnavailable,
)
from app.utils.hashing import content_hash as hash_content

logger = get_logger(__name__)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def to_response(doc: dict[str, Any]) -> DocumentResponse:
    """
    Build an API response that never mixes versions.

    summary/tags are only exposed when their generated_for_version equals
    the document's current content_version.
    """
    content_version = int(doc["content_version"])
    summary_for = doc.get("summary_for_version")
    tags_for = doc.get("tags_for_version")

    summary = doc.get("summary")
    tags = doc.get("tags")

    summary_ok = summary is not None and summary_for == content_version
    tags_ok = tags is not None and tags_for == content_version
    # Only expose derived fields as a pair when BOTH match the current version.
    # This makes mixed-version combinations unobservable over the API.
    result_matches = summary_ok and tags_ok

    failed_stage = doc.get("failed_stage")

    # Enriching-failed documents still expose a matching stage-1 summary so the
    # caller can see partial progress without any tags from another version.
    expose_summary_only = (
        summary_ok
        and not tags_ok
        and DocumentStatus(doc["status"]) == DocumentStatus.FAILED
        and doc.get("failed_stage") == FailedStage.ENRICHING.value
    )

    return DocumentResponse(
        document_id=str(doc["_id"]),
        user_id=doc["user_id"],
        title=doc["title"],
        content=doc["content"],
        content_hash=doc["content_hash"],
        content_version=content_version,
        client_doc_ref=doc.get("client_doc_ref"),
        status=DocumentStatus(doc["status"]),
        failed_stage=FailedStage(failed_stage) if failed_stage else None,
        error_message=doc.get("error_message"),
        summary=summary if (result_matches or expose_summary_only) else None,
        summary_for_version=(
            summary_for if (result_matches or expose_summary_only) else None
        ),
        tags=tags if result_matches else None,
        tags_for_version=tags_for if result_matches else None,
        result_matches_content=result_matches,
        created_at=doc["created_at"],
        updated_at=doc["updated_at"],
    )


class DocumentService:
    def __init__(
        self,
        db: AsyncIOMotorDatabase,
        redis: Redis,
        settings: Settings,
    ):
        self._repo = DocumentRepository(db)
        self._cache = CacheService(redis, settings)
        self._rate_limit = RateLimitService(redis, settings)
        self._settings = settings

    async def create(self, payload: DocumentCreate) -> DocumentCreateResponse:
        digest = hash_content(payload.content)

        # Rate limit before any Mongo write
        try:
            await self._rate_limit.try_acquire(payload.user_id)
        except RateLimitExceeded as exc:
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail=str(exc),
            ) from exc
        except RateLimitUnavailable as exc:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail=str(exc),
            ) from exc

        now = _utcnow()
        doc = {
            "user_id": payload.user_id,
            "title": payload.title,
            "content": payload.content,
            "content_hash": digest,
            "content_version": 1,
            "client_doc_ref": payload.client_doc_ref,
            "status": DocumentStatus.QUEUED.value,
            "failed_stage": None,
            "error_message": None,
            "summary": None,
            "summary_for_version": None,
            "tags": None,
            "tags_for_version": None,
            "stage_lock": None,
            "created_at": now,
            "updated_at": now,
        }

        try:
            document_id = await self._repo.insert(doc)
        except DuplicateKeyError as exc:
            # Unique client_doc_ref collision — release the slot we just acquired
            await self._rate_limit.release(payload.user_id)
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=(
                    "client_doc_ref already exists. "
                    "Use GET /documents/by-ref/{client_doc_ref} to fetch the existing "
                    "document, or PATCH that document_id to change content."
                ),
            ) from exc
        except Exception:
            await self._rate_limit.release(payload.user_id)
            raise

        logger.info(
            "document_created",
            document_id=document_id,
            user_id=payload.user_id,
            content_hash=digest,
        )

        # Fast-path: identical content already processed successfully
        cached = await self._cache.get(digest)
        if cached is not None:
            applied = await self._repo.apply_cached_result(
                document_id,
                content_version=1,
                summary=cached["summary"],
                tags=cached["tags"],
            )
            if applied is not None:
                await self._rate_limit.release(payload.user_id)
                logger.info("cache_hit_applied", document_id=document_id, content_hash=digest)
                return DocumentCreateResponse(
                    document_id=document_id,
                    status=DocumentStatus.COMPLETED,
                )

        return DocumentCreateResponse(
            document_id=document_id,
            status=DocumentStatus.QUEUED,
        )

    async def get(self, document_id: str, user_id: str) -> DocumentResponse:
        try:
            doc = await self._repo.find_by_id(document_id)
        except ValueError as exc:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Document not found",
            ) from exc

        # Ownership: non-owners get the same 404 as missing docs
        if doc is None or doc["user_id"] != user_id:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Document not found",
            )
        return to_response(doc)

    async def get_by_ref(self, client_doc_ref: str, user_id: str) -> DocumentResponse:
        doc = await self._repo.find_by_client_doc_ref(client_doc_ref)
        if doc is None or doc["user_id"] != user_id:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Document not found",
            )
        return to_response(doc)

    async def list_for_user(
        self,
        user_id: str,
        *,
        page: int,
        page_size: int,
        status_filter: Optional[DocumentStatus] = None,
    ) -> DocumentListResponse:
        items, total = await self._repo.list_for_user(
            user_id,
            page=page,
            page_size=page_size,
            status=status_filter,
        )
        return DocumentListResponse(
            items=[to_response(doc) for doc in items],
            page=page,
            page_size=page_size,
            total=total,
        )

    async def patch(
        self,
        document_id: str,
        user_id: str,
        payload: DocumentUpdate,
        *,
        expected_version: Optional[int] = None,
    ) -> DocumentResponse:
        try:
            existing = await self._repo.find_by_id(document_id)
        except ValueError as exc:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Document not found",
            ) from exc

        if existing is None or existing["user_id"] != user_id:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Document not found",
            )

        digest = hash_content(payload.content)
        was_active = DocumentStatus(existing["status"]) in ACTIVE_STATUSES

        # Acquire a pipeline slot only if the doc was not already active
        acquired = False
        if not was_active:
            try:
                await self._rate_limit.try_acquire(user_id)
                acquired = True
            except RateLimitExceeded as exc:
                raise HTTPException(
                    status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                    detail=str(exc),
                ) from exc
            except RateLimitUnavailable as exc:
                raise HTTPException(
                    status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                    detail=str(exc),
                ) from exc

        try:
            updated = await self._repo.patch_content(
                document_id,
                user_id,
                content=payload.content,
                content_hash=digest,
                expected_version=expected_version,
            )
        except Exception:
            if acquired:
                await self._rate_limit.release(user_id)
            raise

        if updated is None:
            if acquired:
                await self._rate_limit.release(user_id)
            # Either wrong owner (already checked) or version conflict
            if expected_version is not None:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail=(
                        f"Document version conflict: expected content_version="
                        f"{expected_version}"
                    ),
                )
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Document not found",
            )

        logger.info(
            "document_patched",
            document_id=document_id,
            content_version=updated["content_version"],
            content_hash=digest,
        )

        # Cache hit for the new content
        cached = await self._cache.get(digest)
        if cached is not None:
            applied = await self._repo.apply_cached_result(
                document_id,
                content_version=updated["content_version"],
                summary=cached["summary"],
                tags=cached["tags"],
            )
            if applied is not None:
                # Leaving active state → release slot (whether newly acquired or reused)
                await self._rate_limit.release(user_id)
                return to_response(applied)

        return to_response(updated)
