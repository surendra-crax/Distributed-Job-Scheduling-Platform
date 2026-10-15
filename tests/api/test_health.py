import pytest
from httpx import AsyncClient


@pytest.mark.asyncio
async def test_health_live(client: AsyncClient) -> None:
    """Verify liveness probe returns 200 and alive status."""
    response = await client.get("/health/live")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "alive"
    assert "app_name" in data
    assert "timestamp" in data
    assert data["environment"] == "testing"


@pytest.mark.asyncio
async def test_health_ready_healthy(client: AsyncClient) -> None:
    """Verify readiness probe checks dependencies and returns ready status."""
    response = await client.get("/health/ready")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "ready"
    assert "database" in data["checks"]
    assert data["checks"]["database"]["status"] == "healthy"
    assert "redis" in data["checks"]
    assert data["checks"]["redis"]["status"] == "healthy"


@pytest.mark.asyncio
async def test_health_ready_degraded_when_database_fails(
    client: AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify readiness returns 503 when a dependency check fails."""

    async def mock_failing_db():
        return False, "Connection refused"

    monkeypatch.setattr("app.api.routes.health.check_database_connection", mock_failing_db)

    response = await client.get("/health/ready")
    assert response.status_code == 503
    data = response.json()
    assert data["status"] == "unhealthy"
    assert data["checks"]["database"]["status"] == "unhealthy"
    assert data["checks"]["database"]["error"] == "Connection refused"
