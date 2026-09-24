from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field, field_validator

from app.models.document import DocumentStatus, FailedStage


class DocumentCreate(BaseModel):
    user_id: str = Field(..., min_length=1, max_length=128)
    title: str = Field(..., min_length=1, max_length=512)
    content: str = Field(..., min_length=1, max_length=500_000)
    client_doc_ref: Optional[str] = Field(default=None, min_length=1, max_length=256)

    @field_validator("user_id", "title", "content", "client_doc_ref", mode="before")
    @classmethod
    def strip_strings(cls, value: object) -> object:
        if isinstance(value, str):
            return value.strip()
        return value

    @field_validator("client_doc_ref")
    @classmethod
    def empty_ref_becomes_none(cls, value: Optional[str]) -> Optional[str]:
        if value is not None and value == "":
            return None
        return value


class DocumentUpdate(BaseModel):
    content: str = Field(..., min_length=1, max_length=500_000)

    @field_validator("content", mode="before")
    @classmethod
    def strip_content(cls, value: object) -> object:
        if isinstance(value, str):
            return value.strip()
        return value


class DocumentCreateResponse(BaseModel):
    document_id: str
    status: DocumentStatus


class DocumentResponse(BaseModel):
    document_id: str
    user_id: str
    title: str
    content: str
    content_hash: str
    content_version: int
    client_doc_ref: Optional[str] = None
    status: DocumentStatus
    failed_stage: Optional[FailedStage] = None
    error_message: Optional[str] = None
    # Derived fields are only populated when they match content_version
    summary: Optional[str] = None
    summary_for_version: Optional[int] = None
    tags: Optional[list[str]] = None
    tags_for_version: Optional[int] = None
    result_matches_content: bool
    created_at: datetime
    updated_at: datetime


class DocumentListResponse(BaseModel):
    items: list[DocumentResponse]
    page: int
    page_size: int
    total: int


class HealthResponse(BaseModel):
    status: str
    mongodb: str
    redis: str
