import pytest
from httpx import AsyncClient

from app.models.document import DocumentStatus
from app.repositories.document_repository import DocumentRepository
from tests.conftest import drain_pipeline


@pytest.mark.asyncio
async def test_patch_requeues_and_clears_derived_fields(
    client: AsyncClient, worker, sample_content
):
    created = (await client.post("/documents", json=sample_content)).json()
    doc_id = created["document_id"]
    await drain_pipeline(worker)

    before = (
        await client.get(f"/documents/{doc_id}", params={"user_id": "user-1"})
    ).json()
    assert before["status"] == DocumentStatus.COMPLETED.value
    assert before["content_version"] == 1
    old_summary = before["summary"]

    patched = await client.patch(
        f"/documents/{doc_id}",
        params={"user_id": "user-1"},
        json={"content": "Brand new content about onboarding funnels and churn."},
    )
    assert patched.status_code == 200
    mid = patched.json()
    assert mid["content_version"] == 2
    assert mid["status"] == DocumentStatus.QUEUED.value
    assert mid["summary"] is None
    assert mid["tags"] is None
    assert mid["result_matches_content"] is False

    await drain_pipeline(worker)
    after = (
        await client.get(f"/documents/{doc_id}", params={"user_id": "user-1"})
    ).json()
    assert after["status"] == DocumentStatus.COMPLETED.value
    assert after["content_version"] == 2
    assert after["summary_for_version"] == 2
    assert after["tags_for_version"] == 2
    assert after["summary"] != old_summary
    assert after["result_matches_content"] is True


@pytest.mark.asyncio
async def test_get_never_returns_mixed_versions(
    client: AsyncClient, worker, mongo_db, sample_content
):
    """Force a mismatched DB state and confirm the API hides derived fields."""
    created = (await client.post("/documents", json=sample_content)).json()
    doc_id = created["document_id"]
    await drain_pipeline(worker)

    # Corrupt: bump content_version without clearing tags, leave summary on v1
    repo = DocumentRepository(mongo_db)
    from bson import ObjectId

    await mongo_db["documents"].update_one(
        {"_id": ObjectId(doc_id)},
        {
            "$set": {
                "content_version": 2,
                "content": "replaced",
                "summary_for_version": 1,
                "tags_for_version": 2,
                "tags": ["new-tag"],
            }
        },
    )

    resp = (
        await client.get(f"/documents/{doc_id}", params={"user_id": "user-1"})
    ).json()
    # summary is for v1 → hidden; tags claim v2 but we only expose when BOTH match
    assert resp["content_version"] == 2
    assert resp["summary"] is None
    assert resp["tags"] is None
    assert resp["result_matches_content"] is False


@pytest.mark.asyncio
async def test_stale_worker_cannot_overwrite_after_patch(
    client: AsyncClient, worker, mongo_db, sample_content
):
    created = (await client.post("/documents", json=sample_content)).json()
    doc_id = created["document_id"]

    # Claim for processing (simulates in-flight worker)
    repo = DocumentRepository(mongo_db)
    claimed = await repo.claim_for_processing()
    assert claimed is not None
    assert str(claimed["_id"]) == doc_id
    old_version = claimed["content_version"]
    old_lock = claimed["stage_lock"]

    # PATCH races the in-flight job
    patched = await client.patch(
        f"/documents/{doc_id}",
        params={"user_id": "user-1"},
        json={"content": "Patched while worker was running stage 1."},
    )
    assert patched.status_code == 200
    assert patched.json()["content_version"] == old_version + 1

    # Stale worker tries to write its result
    result = await repo.complete_processing(
        doc_id,
        content_version=old_version,
        stage_lock=old_lock,
        summary="STALE SUMMARY THAT MUST NOT APPEAR",
    )
    assert result is None

    doc = (
        await client.get(f"/documents/{doc_id}", params={"user_id": "user-1"})
    ).json()
    assert doc["content_version"] == 2
    assert doc["summary"] is None
    assert "STALE" not in (doc.get("summary") or "")

    # Fresh pipeline for the new version still works
    await drain_pipeline(worker)
    final = (
        await client.get(f"/documents/{doc_id}", params={"user_id": "user-1"})
    ).json()
    assert final["status"] == DocumentStatus.COMPLETED.value
    assert final["summary_for_version"] == 2
    assert final["tags_for_version"] == 2


@pytest.mark.asyncio
async def test_optimistic_concurrency_on_patch(
    client: AsyncClient, worker, sample_content
):
    created = (await client.post("/documents", json=sample_content)).json()
    doc_id = created["document_id"]
    await drain_pipeline(worker)

    first = await client.patch(
        f"/documents/{doc_id}",
        params={"user_id": "user-1", "expected_version": 1},
        json={"content": "First concurrent patch body."},
    )
    assert first.status_code == 200
    assert first.json()["content_version"] == 2

    second = await client.patch(
        f"/documents/{doc_id}",
        params={"user_id": "user-1", "expected_version": 1},
        json={"content": "Second concurrent patch body."},
    )
    assert second.status_code == 409
