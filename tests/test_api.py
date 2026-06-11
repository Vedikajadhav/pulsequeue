import pytest
from httpx import AsyncClient, ASGITransport
from app.main import app

@pytest.mark.asyncio
async def test_health_check():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.get("/health")
    assert response.status_code == 200
    assert response.json()["status"] == "ok"

@pytest.mark.asyncio
async def test_create_topic():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.post("/api/v1/topics", json={"name": "test-topic", "partition_count": 2})
    assert response.status_code == 200
    assert response.json()["name"] == "test-topic"

@pytest.mark.asyncio
async def test_list_topics():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.get("/api/v1/topics")
    assert response.status_code == 200
    assert isinstance(response.json(), list)
