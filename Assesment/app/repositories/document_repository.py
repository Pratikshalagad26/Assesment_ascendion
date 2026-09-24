from datetime import datetime, timezone
from typing import Any, Optional
from uuid import uuid4

from bson import ObjectId
from bson.errors import InvalidId
from motor.motor_asyncio import AsyncIOMotorCollection, AsyncIOMotorDatabase
from pymongo import ASCENDING, DESCENDING, IndexModel, ReturnDocument
from pymongo.errors import DuplicateKeyError

from app.models.document import DocumentStatus


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def parse_object_id(document_id: str) -> ObjectId:
    try:
        return ObjectId(document_id)
    except (InvalidId, TypeError) as exc:
        raise ValueError(f"Invalid document_id: {document_id}") from exc


class DocumentRepository:
    def __init__(self, db: AsyncIOMotorDatabase):
        self._collection: AsyncIOMotorCollection = db["documents"]

    async def ensure_indexes(self) -> None:
        await self._collection.create_indexes(
            [
                IndexModel(
                    [("user_id", ASCENDING), ("created_at", DESCENDING)],
                    name="user_created_at",
                ),
                IndexModel(
                    [("user_id", ASCENDING), ("status", ASCENDING)],
                    name="user_status",
                ),
                IndexModel([("content_hash", ASCENDING)], name="content_hash"),
                IndexModel(
                    [("client_doc_ref", ASCENDING)],
                    name="client_doc_ref_unique",
                    unique=True,
                    sparse=True,
                ),
                IndexModel(
                    [("status", ASCENDING), ("updated_at", ASCENDING)],
                    name="status_updated_at",
                ),
            ]
        )

    async def insert(self, doc: dict[str, Any]) -> str:
        result = await self._collection.insert_one(doc)
        return str(result.inserted_id)

    async def find_by_id(self, document_id: str) -> Optional[dict[str, Any]]:
        oid = parse_object_id(document_id)
        return await self._collection.find_one({"_id": oid})

    async def find_by_client_doc_ref(self, client_doc_ref: str) -> Optional[dict[str, Any]]:
        return await self._collection.find_one({"client_doc_ref": client_doc_ref})

    async def list_for_user(
        self,
        user_id: str,
        *,
        page: int,
        page_size: int,
        status: Optional[DocumentStatus] = None,
    ) -> tuple[list[dict[str, Any]], int]:
        query: dict[str, Any] = {"user_id": user_id}
        if status is not None:
            query["status"] = status.value

        total = await self._collection.count_documents(query)
        cursor = (
            self._collection.find(query)
            .sort("created_at", DESCENDING)
            .skip((page - 1) * page_size)
            .limit(page_size)
        )
        items = await cursor.to_list(length=page_size)
        return items, total

    async def patch_content(
        self,
        document_id: str,
        user_id: str,
        *,
        content: str,
        content_hash: str,
        expected_version: Optional[int] = None,
    ) -> Optional[dict[str, Any]]:
        """
        Atomically bump content_version, replace content, clear derived fields,
        release any stage lock, and re-queue.
        """
        oid = parse_object_id(document_id)
        filter_query: dict[str, Any] = {"_id": oid, "user_id": user_id}
        if expected_version is not None:
            filter_query["content_version"] = expected_version

        update = {
            "$set": {
                "content": content,
                "content_hash": content_hash,
                "status": DocumentStatus.QUEUED.value,
                "failed_stage": None,
                "error_message": None,
                "summary": None,
                "summary_for_version": None,
                "tags": None,
                "tags_for_version": None,
                "stage_lock": None,
                "updated_at": _utcnow(),
            },
            "$inc": {"content_version": 1},
        }
        return await self._collection.find_one_and_update(
            filter_query,
            update,
            return_document=ReturnDocument.AFTER,
        )

    async def claim_for_processing(self) -> Optional[dict[str, Any]]:
        """Atomically claim one queued document (queued → processing + stage_lock)."""
        lock_id = str(uuid4())
        return await self._collection.find_one_and_update(
            {
                "status": DocumentStatus.QUEUED.value,
                "stage_lock": None,
            },
            {
                "$set": {
                    "status": DocumentStatus.PROCESSING.value,
                    "stage_lock": lock_id,
                    "updated_at": _utcnow(),
                }
            },
            sort=[("updated_at", ASCENDING)],
            return_document=ReturnDocument.AFTER,
        )

    async def claim_for_enriching(self) -> Optional[dict[str, Any]]:
        """Atomically claim one enriching document via stage_lock."""
        lock_id = str(uuid4())
        return await self._collection.find_one_and_update(
            {
                "status": DocumentStatus.ENRICHING.value,
                "stage_lock": None,
                "summary_for_version": {"$ne": None},
            },
            {
                "$set": {
                    "stage_lock": lock_id,
                    "updated_at": _utcnow(),
                }
            },
            sort=[("updated_at", ASCENDING)],
            return_document=ReturnDocument.AFTER,
        )

    async def complete_processing(
        self,
        document_id: str,
        *,
        content_version: int,
        stage_lock: str,
        summary: str,
    ) -> Optional[dict[str, Any]]:
        """Write summary only if still on the claimed version/lock and in processing."""
        oid = parse_object_id(document_id)
        return await self._collection.find_one_and_update(
            {
                "_id": oid,
                "content_version": content_version,
                "status": DocumentStatus.PROCESSING.value,
                "stage_lock": stage_lock,
            },
            {
                "$set": {
                    "summary": summary,
                    "summary_for_version": content_version,
                    "status": DocumentStatus.ENRICHING.value,
                    "stage_lock": None,
                    "failed_stage": None,
                    "error_message": None,
                    "updated_at": _utcnow(),
                }
            },
            return_document=ReturnDocument.AFTER,
        )

    async def fail_processing(
        self,
        document_id: str,
        *,
        content_version: int,
        stage_lock: str,
        error_message: str,
    ) -> Optional[dict[str, Any]]:
        oid = parse_object_id(document_id)
        return await self._collection.find_one_and_update(
            {
                "_id": oid,
                "content_version": content_version,
                "status": DocumentStatus.PROCESSING.value,
                "stage_lock": stage_lock,
            },
            {
                "$set": {
                    "status": DocumentStatus.FAILED.value,
                    "failed_stage": "processing",
                    "error_message": error_message,
                    "stage_lock": None,
                    "updated_at": _utcnow(),
                }
            },
            return_document=ReturnDocument.AFTER,
        )

    async def complete_enriching(
        self,
        document_id: str,
        *,
        content_version: int,
        stage_lock: str,
        tags: list[str],
    ) -> Optional[dict[str, Any]]:
        oid = parse_object_id(document_id)
        return await self._collection.find_one_and_update(
            {
                "_id": oid,
                "content_version": content_version,
                "status": DocumentStatus.ENRICHING.value,
                "stage_lock": stage_lock,
                "summary_for_version": content_version,
            },
            {
                "$set": {
                    "tags": tags,
                    "tags_for_version": content_version,
                    "status": DocumentStatus.COMPLETED.value,
                    "stage_lock": None,
                    "failed_stage": None,
                    "error_message": None,
                    "updated_at": _utcnow(),
                }
            },
            return_document=ReturnDocument.AFTER,
        )

    async def fail_enriching(
        self,
        document_id: str,
        *,
        content_version: int,
        stage_lock: str,
        error_message: str,
    ) -> Optional[dict[str, Any]]:
        """Fail enriching while preserving the stage-1 summary."""
        oid = parse_object_id(document_id)
        return await self._collection.find_one_and_update(
            {
                "_id": oid,
                "content_version": content_version,
                "status": DocumentStatus.ENRICHING.value,
                "stage_lock": stage_lock,
            },
            {
                "$set": {
                    "status": DocumentStatus.FAILED.value,
                    "failed_stage": "enriching",
                    "error_message": error_message,
                    "stage_lock": None,
                    "updated_at": _utcnow(),
                }
            },
            return_document=ReturnDocument.AFTER,
        )

    async def apply_cached_result(
        self,
        document_id: str,
        *,
        content_version: int,
        summary: str,
        tags: list[str],
        from_status: DocumentStatus = DocumentStatus.QUEUED,
        stage_lock: Optional[str] = None,
    ) -> Optional[dict[str, Any]]:
        """Skip remaining pipeline work when a content-hash cache hit is available."""
        oid = parse_object_id(document_id)
        filter_query: dict[str, Any] = {
            "_id": oid,
            "content_version": content_version,
            "status": from_status.value,
        }
        if stage_lock is not None:
            filter_query["stage_lock"] = stage_lock

        return await self._collection.find_one_and_update(
            filter_query,
            {
                "$set": {
                    "summary": summary,
                    "summary_for_version": content_version,
                    "tags": tags,
                    "tags_for_version": content_version,
                    "status": DocumentStatus.COMPLETED.value,
                    "stage_lock": None,
                    "failed_stage": None,
                    "error_message": None,
                    "updated_at": _utcnow(),
                }
            },
            return_document=ReturnDocument.AFTER,
        )

    async def release_stage_lock(
        self,
        document_id: str,
        *,
        stage_lock: str,
    ) -> None:
        oid = parse_object_id(document_id)
        await self._collection.update_one(
            {"_id": oid, "stage_lock": stage_lock},
            {"$set": {"stage_lock": None, "updated_at": _utcnow()}},
        )


__all__ = ["DocumentRepository", "DuplicateKeyError", "parse_object_id"]
