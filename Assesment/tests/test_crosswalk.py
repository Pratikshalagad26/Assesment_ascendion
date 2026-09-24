import pytest
from httpx import AsyncClient

from tests.conftest import drain_pipeline


@pytest.mark.asyncio
async def test_lookup_by_client_doc_ref(client: AsyncClient, sample_content):
    payload = {**sample_content, "client_doc_ref": "partner-doc-42"}
    created = (await client.post("/documents", json=payload)).json()

    found = await client.get(
        "/documents/by-ref/partner-doc-42",
        params={"user_id": "user-1"},
    )
    assert found.status_code == 200
    assert found.json()["document_id"] == created["document_id"]
    assert found.json()["client_doc_ref"] == "partner-doc-42"

    # Ownership still applies
    denied = await client.get(
        "/documents/by-ref/partner-doc-42",
        params={"user_id": "intruder"},
    )
    assert denied.status_code == 404

    missing = await client.get(
        "/documents/by-ref/does-not-exist",
        params={"user_id": "user-1"},
    )
    assert missing.status_code == 404


@pytest.mark.asyncio
async def test_duplicate_client_doc_ref_rejected(client: AsyncClient, sample_content):
    payload = {**sample_content, "client_doc_ref": "same-ref"}
    first = await client.post("/documents", json=payload)
    assert first.status_code == 201

    # Same ref, same content
    second = await client.post("/documents", json=payload)
    assert second.status_code == 409

    # Same ref, different content — still 409 (reject strategy)
    third = await client.post(
        "/documents",
        json={
            **sample_content,
            "content": "Entirely different body for the same external ref.",
            "client_doc_ref": "same-ref",
        },
    )
    assert third.status_code == 409
    assert "client_doc_ref already exists" in third.json()["detail"]


@pytest.mark.asyncio
async def test_patch_existing_ref_document_is_the_update_path(
    client: AsyncClient, worker, sample_content
):
    """Documented workflow: change content via PATCH, not re-POST of the ref."""
    payload = {**sample_content, "client_doc_ref": "update-via-patch"}
    created = (await client.post("/documents", json=payload)).json()
    doc_id = created["document_id"]
    await drain_pipeline(worker)

    patched = await client.patch(
        f"/documents/{doc_id}",
        params={"user_id": "user-1"},
        json={"content": "Updated partner content after CMS edit."},
    )
    assert patched.status_code == 200

    by_ref = await client.get(
        "/documents/by-ref/update-via-patch",
        params={"user_id": "user-1"},
    )
    assert by_ref.json()["document_id"] == doc_id
    assert by_ref.json()["content_version"] == 2
