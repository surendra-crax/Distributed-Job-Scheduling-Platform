import uuid
from datetime import UTC, datetime, timedelta

import pytest
from httpx import AsyncClient


@pytest.mark.asyncio
async def test_submit_immediate_job_success(client: AsyncClient):
    payload = {
        "job_type": "math.calculate",
        "payload": {"operation": "sum", "operands": [10, 20, 30]},
        "priority": 75,
        "max_attempts": 3,
        "timeout_seconds": 60,
    }
    response = await client.post("/api/v1/jobs", json=payload)
    assert response.status_code == 201
    data = response.json()
    assert data["job_type"] == "math.calculate"
    assert data["priority"] == 75
    assert data["status"] == "QUEUED"  # Immediate submission starts as QUEUED
    assert data["attempt_count"] == 0
    assert data["version"] == 1
    assert "id" in data


@pytest.mark.asyncio
async def test_submit_scheduled_job_success(client: AsyncClient):
    future_time = datetime.now(UTC) + timedelta(minutes=15)
    payload = {
        "job_type": "math.calculate",
        "payload": {"operation": "multiply", "operands": [3, 7]},
        "priority": 50,
        "scheduled_at": future_time.isoformat(),
    }
    response = await client.post("/api/v1/jobs", json=payload)
    assert response.status_code == 201
    data = response.json()
    assert data["status"] == "SCHEDULED"


@pytest.mark.asyncio
async def test_submit_job_unknown_type_error(client: AsyncClient):
    payload = {
        "job_type": "nonexistent.type",
        "payload": {"data": "test"},
    }
    response = await client.post("/api/v1/jobs", json=payload)
    assert response.status_code == 400
    assert "UnknownJobType" in response.text


@pytest.mark.asyncio
async def test_submit_job_invalid_payload_schema_error(client: AsyncClient):
    # Missing required operands for calculation
    payload = {
        "job_type": "math.calculate",
        "payload": {"invalid_key": "bad_data"},
    }
    response = await client.post("/api/v1/jobs", json=payload)
    assert response.status_code == 422


@pytest.mark.asyncio
async def test_get_job_by_id(client: AsyncClient):
    # First create a job
    submit_res = await client.post(
        "/api/v1/jobs",
        json={"job_type": "report.generate", "payload": {"report_type": "sales_summary"}},
    )
    job_id = submit_res.json()["id"]

    # Retrieve it
    get_res = await client.get(f"/api/v1/jobs/{job_id}")
    assert get_res.status_code == 200
    assert get_res.json()["id"] == job_id


@pytest.mark.asyncio
async def test_get_job_not_found(client: AsyncClient):
    random_id = uuid.uuid4()
    response = await client.get(f"/api/v1/jobs/{random_id}")
    assert response.status_code == 404
    assert "JobNotFound" in response.text


@pytest.mark.asyncio
async def test_list_jobs_and_filter(client: AsyncClient):
    # Submit a job
    await client.post(
        "/api/v1/jobs",
        json={"job_type": "math.calculate", "payload": {"operation": "sum", "operands": [1, 2]}},
    )

    response = await client.get("/api/v1/jobs?limit=5&offset=0")
    assert response.status_code == 200
    data = response.json()
    assert "items" in data
    assert "total" in data
    assert data["limit"] == 5
    assert len(data["items"]) >= 1


@pytest.mark.asyncio
async def test_cancel_and_retry_job_flow(client: AsyncClient):
    # 1. Create a queued job
    submit_res = await client.post(
        "/api/v1/jobs",
        json={
            "job_type": "notification.simulate",
            "payload": {"recipient": "user@test.com", "channel": "sms", "template": "OTP"},
        },
    )
    job_id = submit_res.json()["id"]
    assert submit_res.json()["status"] == "QUEUED"

    # 2. Cancel the queued job
    cancel_res = await client.post(f"/api/v1/jobs/{job_id}/cancel")
    assert cancel_res.status_code == 200
    cancel_data = cancel_res.json()
    assert cancel_data["previous_status"] == "QUEUED"
    assert cancel_data["new_status"] == "CANCELLED"

    # Verify state via GET
    get_res = await client.get(f"/api/v1/jobs/{job_id}")
    assert get_res.json()["status"] == "CANCELLED"

    # 3. Retry the cancelled job
    retry_res = await client.post(f"/api/v1/jobs/{job_id}/retry")
    assert retry_res.status_code == 200
    retry_data = retry_res.json()
    assert retry_data["new_status"] == "QUEUED"

    # Verify state returned to QUEUED
    get_res2 = await client.get(f"/api/v1/jobs/{job_id}")
    assert get_res2.json()["status"] == "QUEUED"


@pytest.mark.asyncio
async def test_get_job_attempts_empty_initially(client: AsyncClient):
    submit_res = await client.post(
        "/api/v1/jobs",
        json={"job_type": "math.calculate", "payload": {"operation": "sum", "operands": [10]}},
    )
    job_id = submit_res.json()["id"]

    attempts_res = await client.get(f"/api/v1/jobs/{job_id}/attempts")
    assert attempts_res.status_code == 200
    assert attempts_res.json() == []
