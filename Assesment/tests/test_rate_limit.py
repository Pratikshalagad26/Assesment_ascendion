import pytest
from httpx import AsyncClient

from app.services.cache_service import CacheService
from app.utils.hashing import content_hash
from tests.conftest import drain_pipeline


@pytest.mark.asyncio
async def test_rate_limit_at_three_active_documents(client: AsyncClient, sample_content):
    # Keep docs queued (do not drain worker) so they stay active
    for i in range(3):
        resp = await client.post(
            "/documents",
            json={
                "user_id": "rate-user",
                "title": f"Active {i}",
                "content": f"Active pipeline document number {i} with unique body.",
            },
        )
        assert resp.status_code == 201

    blocked = await client.post(
        "/documents",
        json={
            "user_id": "rate-user",
            "title": "One too many",
            "content": "This should be rejected by the active pipeline limiter.",
        },
    )
    assert blocked.status_code == 429

    # Different user is unaffected
    other = await client.post(
        "/documents",
        json={
            "user_id": "other-user",
            "title": "Other",
            "content": "Other user content for rate limit isolation.",
        },
    )
    assert other.status_code == 201


@pytest.mark.asyncio
async def test_rate_limit_releases_after_completion(
    client: AsyncClient, worker, sample_content
):
    for i in range(3):
        await client.post(
            "/documents",
            json={
                "user_id": "release-user",
                "title": f"Doc {i}",
                "content": f"Release test content body number {i}.",
            },
        )

    blocked = await client.post(
        "/documents",
        json={
            "user_id": "release-user",
            "title": "Blocked",
            "content": "Should be blocked until slots free.",
        },
    )
    assert blocked.status_code == 429

    await drain_pipeline(worker)

    allowed = await client.post(
        "/documents",
        json={
            "user_id": "release-user",
            "title": "After release",
            "content": "Should succeed after pipelines completed.",
        },
    )
    assert allowed.status_code == 201


@pytest.mark.asyncio
async def test_cache_hit_for_identical_content(
    client: AsyncClient, worker, redis_client, settings, sample_content
):
    first = await client.post("/documents", json=sample_content)
    assert first.status_code == 201
    await drain_pipeline(worker)

    first_doc = (
        await client.get(
            f"/documents/{first.json()['document_id']}",
            params={"user_id": "user-1"},
        )
    ).json()
    assert first_doc["status"] == "completed"

    # Second user, identical content → cache hit → completed immediately
    second_payload = {
        "user_id": "user-2",
        "title": "Copy",
        "content": sample_content["content"],
    }
    second = await client.post("/documents", json=second_payload)
    assert second.status_code == 201
    assert second.json()["status"] == "completed"

    second_doc = (
        await client.get(
            f"/documents/{second.json()['document_id']}",
            params={"user_id": "user-2"},
        )
    ).json()
    assert second_doc["summary"] == first_doc["summary"]
    assert second_doc["tags"] == first_doc["tags"]
    assert second_doc["content_hash"] == content_hash(sample_content["content"])


@pytest.mark.asyncio
async def test_cache_keyed_by_hash_not_document_id(
    client: AsyncClient, worker, redis_client, settings, sample_content
):
    created = (await client.post("/documents", json=sample_content)).json()
    await drain_pipeline(worker)

    digest = content_hash(sample_content["content"])
    cache = CacheService(redis_client, settings)
    cached = await cache.get(digest)
    assert cached is not None
    assert "summary" in cached

    # After PATCH to new content, old cache entry must not leak into the response
    await client.patch(
        f"/documents/{created['document_id']}",
        params={"user_id": "user-1"},
        json={"content": "Totally different patched content for cache isolation."},
    )
    mid = (
        await client.get(
            f"/documents/{created['document_id']}",
            params={"user_id": "user-1"},
        )
    ).json()
    assert mid["summary"] is None
    assert mid["content_hash"] != digest
    # Old hash entry still exists in Redis (content-based), but is not used for this doc
    assert await cache.get(digest) is not None
