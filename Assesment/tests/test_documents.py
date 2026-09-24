import pytest
from httpx import AsyncClient

from app.models.document import DocumentStatus
from app.utils.hashing import content_hash
from tests.conftest import drain_pipeline


@pytest.mark.asyncio
async def test_create_document_success(client: AsyncClient, sample_content):
    resp = await client.post("/documents", json=sample_content)
    assert resp.status_code == 201
    body = resp.json()
    assert "document_id" in body
    assert body["status"] == DocumentStatus.QUEUED.value


@pytest.mark.asyncio
async def test_create_validation_failure(client: AsyncClient):
    resp = await client.post(
        "/documents",
        json={"user_id": "", "title": "t", "content": "c"},
    )
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_create_missing_content(client: AsyncClient):
    resp = await client.post(
        "/documents",
        json={"user_id": "u1", "title": "t"},
    )
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_get_document_and_ownership(client: AsyncClient, sample_content, worker):
    created = (await client.post("/documents", json=sample_content)).json()
    doc_id = created["document_id"]

    ok = await client.get(f"/documents/{doc_id}", params={"user_id": "user-1"})
    assert ok.status_code == 200
    data = ok.json()
    assert data["title"] == sample_content["title"]
    assert data["content_version"] == 1
    assert data["content_hash"] == content_hash(sample_content["content"])

    other = await client.get(f"/documents/{doc_id}", params={"user_id": "user-2"})
    assert other.status_code == 404

    missing = await client.get(
        "/documents/000000000000000000000000",
        params={"user_id": "user-1"},
    )
    assert missing.status_code == 404


@pytest.mark.asyncio
async def test_get_requires_user_id(client: AsyncClient, sample_content):
    created = (await client.post("/documents", json=sample_content)).json()
    resp = await client.get(f"/documents/{created['document_id']}")
    assert resp.status_code == 400


@pytest.mark.asyncio
async def test_list_documents_newest_first_and_status_filter(
    client: AsyncClient, worker, mongo_db
):
    import asyncio
    from datetime import datetime, timedelta, timezone

    ids = []
    for i in range(3):
        resp = await client.post(
            "/documents",
            json={
                "user_id": "lister",
                "title": f"Doc {i}",
                "content": f"Unique content number {i} for listing tests.",
            },
        )
        ids.append(resp.json()["document_id"])
        await asyncio.sleep(0)  # yield; timestamps forced below

    # Force strictly increasing created_at so newest-first is deterministic under mongomock
    from bson import ObjectId

    base = datetime.now(timezone.utc)
    for i, doc_id in enumerate(ids):
        await mongo_db["documents"].update_one(
            {"_id": ObjectId(doc_id)},
            {"$set": {"created_at": base + timedelta(seconds=i)}},
        )

    await drain_pipeline(worker)

    listing = await client.get(
        "/users/lister/documents",
        params={"page": 1, "page_size": 2},
    )
    assert listing.status_code == 200
    body = listing.json()
    assert body["total"] == 3
    assert len(body["items"]) == 2
    # Newest first: last created title should lead
    assert body["items"][0]["title"] == "Doc 2"

    filtered = await client.get(
        "/users/lister/documents",
        params={"status": "completed"},
    )
    assert filtered.status_code == 200
    assert filtered.json()["total"] == 3
    assert all(i["status"] == "completed" for i in filtered.json()["items"])

    # Other user sees nothing
    other = await client.get("/users/other/documents")
    assert other.json()["total"] == 0


@pytest.mark.asyncio
async def test_content_hashing_deterministic():
    assert content_hash("hello") == content_hash("hello")
    assert content_hash("hello") != content_hash("Hello")
