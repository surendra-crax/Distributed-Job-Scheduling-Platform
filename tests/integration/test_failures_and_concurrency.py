import asyncio
from datetime import UTC, datetime
from unittest.mock import AsyncMock

import pytest

from app.db.session import get_session_factory
from app.models.job import JobStatus
from app.queue.redis_queue import get_redis_client
from app.repositories.job_repository import JobRepository
from app.schemas.job import JobCreateRequest
from app.services.job_service import JobService
from app.services.recovery_service import RecoveryService
from app.workers.worker import WorkerProcess


@pytest.mark.asyncio
async def test_redis_failure_during_submission_retains_durable_job():
    """If Redis is down or raises during submission, PostgreSQL still commits QUEUED job,

    and Recovery reconciles it when Redis is restored.
    """
    session_factory = get_session_factory()
    redis = get_redis_client()

    # Mock Redis to simulate failure during job submission
    failing_redis = AsyncMock()
    failing_redis.zadd.side_effect = ConnectionError("Redis connection refused")

    async with session_factory() as session:
        service = JobService(session, failing_redis)
        # Submission must NOT crash; it should succeed and commit to PostgreSQL
        job = await service.submit_job(
            JobCreateRequest(
                job_type="math.calculate",
                payload={"operation": "sum", "operands": [10, 5]},
                priority=70,
            )
        )
        job_id = job.id

    # Verify durable record exists in PostgreSQL with status QUEUED
    async with session_factory() as session:
        repo = JobRepository(session)
        persisted_job = await repo.get_by_id(job_id)
        assert persisted_job is not None
        assert persisted_job.status == JobStatus.QUEUED.value

    # Now run Recovery reconciler with healthy real Redis
    recovery = RecoveryService(session_factory=session_factory, redis_client=redis)
    # Move updated_at slightly back so reconciler recognizes it as stuck
    async with session_factory() as session:
        repo = JobRepository(session)
        pjob = await repo.get_by_id(job_id)
        assert pjob is not None
        pjob.updated_at = datetime.fromtimestamp(0, tz=UTC)
        await session.commit()

    reconciled = await recovery.reconcile_lost_queue_jobs()
    assert reconciled >= 1

    # Verify job is now present in Redis queue
    score = await redis.zscore(recovery.settings.REDIS_JOB_QUEUE_KEY, str(job_id))
    assert score is not None


@pytest.mark.asyncio
async def test_duplicate_queue_delivery_processed_once():
    """If Redis delivers the same job ID to multiple workers (at-least-once delivery),

    PostgreSQL row-level claim allows only one execution to succeed.
    """
    session_factory = get_session_factory()
    redis = get_redis_client()

    async with session_factory() as session:
        service = JobService(session, redis)
        job = await service.submit_job(
            JobCreateRequest(
                job_type="math.calculate",
                payload={"operation": "sum", "operands": [100, 200]},
                priority=50,
            )
        )
        job_id = job.id

    worker_1 = WorkerProcess(worker_id="w-1", session_factory=session_factory, redis_client=redis)
    worker_2 = WorkerProcess(worker_id="w-2", session_factory=session_factory, redis_client=redis)

    # Both workers process the same job_id (simulating duplicate delivery)
    results = await asyncio.gather(
        worker_1.process_job(job_id),
        worker_2.process_job(job_id),
    )

    # Exactly one worker succeeded; other dropped cleanly
    assert sum([1 for r in results if r is True]) == 1

    async with session_factory() as session:
        repo = JobRepository(session)
        finished = await repo.get_by_id(job_id)
        assert finished.status == JobStatus.SUCCEEDED.value
        assert finished.attempt_count == 1  # Only 1 attempt recorded!


@pytest.mark.asyncio
async def test_cancellation_during_long_running_job():
    """Client cancels a running job; worker cooperatively acknowledges cancellation."""
    session_factory = get_session_factory()
    redis = get_redis_client()

    # Submit report job with 1.5s simulated duration
    async with session_factory() as session:
        service = JobService(session, redis)
        job = await service.submit_job(
            JobCreateRequest(
                job_type="report.generate",
                payload={"report_type": "sales_summary", "simulated_duration_ms": 1500},
                priority=50,
            )
        )
        job_id = job.id

    worker = WorkerProcess(
        worker_id="w-cancel", session_factory=session_factory, redis_client=redis
    )

    async def run_worker():
        await worker.process_job(job_id)

    async def trigger_cancel():
        # Wait until worker has claimed the job
        await asyncio.sleep(0.2)
        async with session_factory() as session:
            service = JobService(session, redis)
            await service.cancel_job(job_id)

    await asyncio.gather(run_worker(), trigger_cancel())

    # Verify final state is CANCELLED
    async with session_factory() as session:
        repo = JobRepository(session)
        final_job = await repo.get_by_id(job_id)
        assert final_job.status == JobStatus.CANCELLED.value
