import pytest
from httpx import AsyncClient


@pytest.mark.asyncio
async def test_get_metrics_summary(client: AsyncClient):
    """Verify metrics summary returns aggregate stats on jobs, Redis queue, and attempts."""
    # Create a job first
    await client.post(
        "/api/v1/jobs",
        json={"job_type": "math.calculate", "payload": {"operation": "sum", "operands": [10, 20]}},
    )

    response = await client.get("/api/v1/metrics/summary")
    assert response.status_code == 200
    data = response.json()

    assert "jobs_by_status" in data
    assert data["jobs_by_status"]["QUEUED"] >= 1
    assert "redis_queue_depth" in data
    assert "total_attempts" in data
    assert "avg_execution_duration_seconds" in data
    assert "timestamp" in data
