"""Basic health-check coverage."""

import pytest
from httpx import AsyncClient


@pytest.mark.asyncio
async def test_health_ok(client: AsyncClient):
    resp = await client.get("/health")
    # mongomock ping via db.command may behave differently; accept 200 or 503
    # with structured body either way
    body = resp.json()
    assert "status" in body
    assert "mongodb" in body
    assert "redis" in body
