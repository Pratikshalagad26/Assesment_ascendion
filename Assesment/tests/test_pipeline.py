from unittest.mock import AsyncMock, patch

import pytest
from httpx import AsyncClient

from app.models.document import DocumentStatus, FailedStage
from app.services.pipeline_service import PipelineService, StageFailed
from tests.conftest import drain_pipeline


@pytest.mark.asyncio
async def test_successful_two_stage_processing(client: AsyncClient, worker, sample_content):
    created = (await client.post("/documents", json=sample_content)).json()
    doc_id = created["document_id"]

    await drain_pipeline(worker)

    resp = await client.get(f"/documents/{doc_id}", params={"user_id": "user-1"})
    body = resp.json()
    assert body["status"] == DocumentStatus.COMPLETED.value
    assert body["summary"] is not None
    assert body["tags"] is not None
    assert body["summary_for_version"] == 1
    assert body["tags_for_version"] == 1
    assert body["result_matches_content"] is True
    assert body["failed_stage"] is None


@pytest.mark.asyncio
async def test_processing_failure(client: AsyncClient, worker, sample_content):
    created = (await client.post("/documents", json=sample_content)).json()
    doc_id = created["document_id"]

    with patch.object(
        PipelineService,
        "run_processing",
        new=AsyncMock(side_effect=StageFailed("Simulated processing failure")),
    ):
        await drain_pipeline(worker)

    resp = await client.get(f"/documents/{doc_id}", params={"user_id": "user-1"})
    body = resp.json()
    assert body["status"] == DocumentStatus.FAILED.value
    assert body["failed_stage"] == FailedStage.PROCESSING.value
    assert body["summary"] is None
    assert body["tags"] is None


@pytest.mark.asyncio
async def test_enriching_failure_preserves_processing(
    client: AsyncClient, worker, sample_content
):
    created = (await client.post("/documents", json=sample_content)).json()
    doc_id = created["document_id"]

    with patch.object(
        PipelineService,
        "run_enriching",
        new=AsyncMock(side_effect=StageFailed("Simulated enriching failure")),
    ):
        await drain_pipeline(worker)

    resp = await client.get(f"/documents/{doc_id}", params={"user_id": "user-1"})
    body = resp.json()
    assert body["status"] == DocumentStatus.FAILED.value
    assert body["failed_stage"] == FailedStage.ENRICHING.value
    # Stage-1 progress preserved and visible (matches content_version)
    assert body["summary"] is not None
    assert body["summary_for_version"] == 1
    assert body["tags"] is None
    assert body["result_matches_content"] is False


@pytest.mark.asyncio
async def test_deterministic_summary_and_tags():
    content = "Alpha beta gamma alpha beta delta"
    summary = PipelineService.generate_summary(content)
    assert "Summary" in summary
    assert "Alpha" in summary or "alpha" in summary.lower()

    tags = PipelineService.generate_tags(summary)
    assert isinstance(tags, list)
    assert len(tags) >= 1
